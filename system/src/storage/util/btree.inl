#include <iostream>

namespace storage {

/*
 * BTree::Node, public
 */
template <class K, class V>
BTree<K,V>::Node::Node(bool is_leaf, Node* parent, int32_t idx)
        : parent_(parent), prev_(nullptr), next_(nullptr),
        ts_(0), idx_(idx), num_key_(0), is_leaf_(is_leaf) {
    if (is_leaf)
        fanout_ = NODE_SIZE / (sizeof(K) + sizeof(V));
    else
        fanout_ = NODE_SIZE / (sizeof(K) + sizeof(Node*));
}

template <class K, class V>
bool BTree<K,V>::Node::find(K key, V& val, Flag& flag, ReadLock* my_lock) {
    int idx = searchInNode(key);
    bool success = false;

    if (is_leaf_) {
        if (idx > 0 && key == getKey(idx-1)) {
            // seaching key exists
            val = getValue(idx-1);
            success = true;
        } else {
            // failed
            success = false;
        }
    } else {
        // find child and release my_lock
        Node* child = getChild(idx);
        ReadLock child_lock(child->mx_node_);

        int64_t previous_ts = ts_.load();
        my_lock->unlock();

        success = child->find(key, val, flag, &child_lock);
        if (flag == RETRY) return false;

        if (previous_ts != ts_.load()) {
            flag = RETRY;
            return false;
        }
    }
    return success;
}

template <class K, class V>
bool BTree<K,V>::Node::multifind(K key,
        std::vector<V>& vals, Flag& flag, ReadLock* my_lock) {
    int idx = searchInNode(key);
    bool success = false;

    if (is_leaf_) {
        if (idx > 0 && key == getKey(idx-1)) {
            // seaching key exists
            vals.push_back(getValue(idx-1));
            
            // find left values
            this->find_left(key, vals, idx-1);

            // find right values
            this->find_right(key, vals, idx+1);
            
            success = true;
        } else {
            // failed
            success = false;
        }
    } else {
        // find child and release my_lock
        Node* child = getChild(idx);
        ReadLock child_lock(child->mx_node_);

        int64_t previous_ts = ts_.load();
        my_lock->unlock();

        success = child->multifind(key, vals, flag, &child_lock);
        if (flag == RETRY) return false;

        if (previous_ts != ts_.load()) {
            flag = RETRY;
            return false;
        }
    }

    return success;
}

template <class K, class V>
typename BTree<K,V>::iterator BTree<K,V>::Node::begin(K key) {
    int idx = searchInNode(key);
    if (is_leaf_) {
        return iterator(this, idx - 1);
    } else {
        return getChild(idx)->begin(key);
    }
}

template <class K, class V>
bool BTree<K,V>::Node::insert(K key, V val,
        Flag& flag, WLockVec& parent_locks, WriteLock* my_lock, bool unique) {
    int idx = searchInNode(key);

    if (is_leaf_)
        return insertLeaf(idx, key, val,
                flag, parent_locks, my_lock, unique);
    else
        return insertBranch(idx, key, val,
                flag, parent_locks, my_lock, unique);
}

template <class K, class V>
bool BTree<K,V>::Node::remove(K key, Flag& flag, Node*& node) {
    int idx = searchInNode(key);
    if (is_leaf_) return removeLeaf(idx, key, flag, node);
    else return removeBranch(idx, key, flag, node);
}

template <class K, class V>
bool BTree<K,V>::Node::update(K key, V val, Flag& flag, ReadLock* my_lock) {
    int idx = searchInNode(key);
    bool success = false;

    if (is_leaf_) {
        if (key == getKey(idx-1)) {
            // seaching key exists
            getValue(idx-1) = val;
            success = true;
        } else {
            // failed
            success = false;
        }
    } else {
        // find child and release my_lock
        Node* child = getChild(idx);
        ReadLock child_lock(child->mx_node_);
        my_lock->unlock();

        success = child->update(key, val, flag, &child_lock);
    }
    return success;
}

template <class K, class V>
void BTree<K,V>::Node::collectNode(std::set<Node*>& visited) {
    if (!is_leaf_) {
        for (int i = 0; i <= num_key_; i++) {
            Node* child = getChild(i);
            if (child->parent_ != this || child->idx_ != i) {
                assert(false && "link error");
            }
            visited.insert(child);
            getChild(i)->collectNode(visited);
        }
    }
}

template <class K, class V>
uint64_t BTree<K,V>::Node::countNodesDFS() {
    uint64_t count = 1;
    if (!is_leaf_) {
        for (int i = 0; i <= num_key_; i++) {
            count += getChild(i)->countNodesDFS();
        }
    }
    return count;
}

template <class K, class V>
void BTree<K,V>::Node::traverseDFS(bool dbg) {
    for (int i = 0; i < num_key_; i++) {
        if (dbg) std::cout << getKey(i) << " ";
    }
    if (dbg) {
        if (!is_leaf_) std::cout << "high key: " << getKey(fanout_ - 1);
        std::cout << std::endl;
    }

    if (!is_leaf_) {
        for (int i = 0; i <= num_key_; i++) {
            Node* child = getChild(i);
            if (child->parent_ != this || child->idx_ != i) {
                assert(false && "link error");
            }
            getChild(i)->traverseDFS(dbg);
        }
    }
}

template <class K, class V>
void BTree<K,V>::Node::traverseBFS(bool dbg) {
    Node* node = this;
    while (node) {
        if (dbg) {
            node->printKey();
            std::cout << " ";
        }
        if (node->next_) {
            if (node != node->next_->prev_) {
                assert(false && "link error");
            }
        }
        node = node->next_;
    }
    if (dbg) std::cout << std::endl;

    if (is_leaf_) return;

    getChild(0)->traverseBFS(dbg);
}

template <class K, class V>
bool BTree<K,V>::Node::isSorted() {
    Node* node = this;
    while (node) {
        if (!node->ordered()) {
            return false;
        }
        node = node->next_;
    }

    if (is_leaf_) return true;

    return getChild(0)->isSorted();
}

template <class K, class V>
std::size_t BTree<K,V>::Node::size() {
    std::size_t ret = 0;
    Node* node = this->getLeftMostLeaf();

    while (node) {
        ret += node->num_key_;
        node = node->next_;
    }

    return ret;
}

/*
 * BTree::Node, protected
 */
template <class K, class V>
int BTree<K,V>::Node::searchInNode(K key) {
    if (num_key_ == 0 || key < getKey(0)) return 0;
    if (!is_leaf_ && key >= getHighKey()) return num_key_;

    return lower_bound(key);
}

template <class K, class V>
int BTree<K,V>::Node::lower_bound(K key) {
    int low = 0;
    int high = num_key_;

    while (low < high) {
        int mid = low + (high - low) / 2;
        if (getKey(mid) <= key)
            low = mid + 1;
        else high = mid;
    }

    return low;
}

template <class K, class V>
void BTree<K,V>::Node::find_left(K key, std::vector<V>& vals, int idx) {
    // find left values in the node
    while (true) {
        if (idx <= 0 || key != getKey(idx-1)) break;

        vals.push_back(getValue(idx-1));
        idx--;
    }

    // move to left node if necessary
    if (idx <= 0 && prev_) {
        ReadLock left_lock(prev_->mx_node_);
        prev_->find_left(key, vals, prev_->num_key_);
    }
}

template <class K, class V>
void BTree<K,V>::Node::find_right(K key, std::vector<V>& vals, int idx) {
    // find right values in the node
    while (true) {
        if (idx > num_key_ || key != getKey(idx-1)) break;

        vals.push_back(getValue(idx-1));
        idx++;
    }

    // move to right node if necessary
    if (idx > num_key_ && next_) {
        ReadLock right_lock(next_->mx_node_);
        next_->find_right(key, vals, 1);
    }
}

template <class K, class V>
bool BTree<K,V>::Node::insertLeaf(int idx, K key, V val,
        Flag& flag, WLockVec& parent_locks, WriteLock* my_lock, bool unique) {
    // uniqueness check
    if(unique){
        if (idx > 0 && getKey(idx-1) == key) {
            // failed, release all locks
            parent_locks.push_back(my_lock);
            for (auto lock : parent_locks) {
                if (lock) lock->unlock();
            }

            return false;
        }
    }
    // node is full
    if (num_key_ == fanout_) {
        flag = SPLIT;
        //int split_idx = fanout_ - 1;
        int split_idx = fanout_/2;

        // create right sibling and copy items
        Node* right = new Node(true, parent_, idx_ + 1);
        right->prev_ = this;
        right->next_ = next_;
        if (next_ != nullptr) next_->prev_ = right;
        this->next_ = right;

        Flag dummy_flag(NONE);
        WLockVec dummy_parents;
        for (int i = split_idx; i < num_key_; i++) {
            right->insert(getKey(i), getValue(i),
                    dummy_flag, dummy_parents, nullptr, unique);
        }
        num_key_ = split_idx;

        // insert item
        if (idx < split_idx) {
            this->insert(key, val,
                    dummy_flag, dummy_parents, nullptr, unique);
        } else {
            right->insert(key, val,
                    dummy_flag, dummy_parents, nullptr, unique);
        }

        // if it was the root, create new one
        if (parent_ == nullptr) createRoot(this, right);
        ts_++;
    } else {
        // insert into this node
        flag = NONE;

        for (int i = num_key_; i > idx; i--) {
            this->setKey(i, this->getKey(i-1));
            this->setValue(i, this->getValue(i-1));
        }

        this->setKey(idx, key);
        this->setValue(idx, val);
        num_key_++;
    }

    return true;
}

template <class K, class V>
bool BTree<K,V>::Node::insertBranch(int idx, K key, V val, Flag& flag,
        WLockVec& parent_locks, WriteLock* my_lock, bool unique) {
    Node* child = getChild(idx);
    {
        // get child lock
        WriteLock child_lock(child->mx_node_);
        if (child->insertSafe()) {
            // child is insert-safe.
            // release all locks except for this node and the child
            for (auto lock : parent_locks) {
                lock->unlock();
            }
            parent_locks.clear();
        }
        parent_locks.push_back(my_lock);

        if (!child->insert(key, val, flag, parent_locks, &child_lock, unique))
            return false;
    }

    if (flag & SPLIT) {
        Node* new_child = child->next_;

        // node is full
        if (num_key_ == fanout_ - 1) {
            // split
            flag = SPLIT;
            int split_idx = fanout_/2;
            //int split_idx = fanout_ - 2;
            Node* right = new Node(false, parent_, idx_+1);
            right->prev_ = this;
            right->next_ = next_;

            if (next_ != nullptr) next_->prev_ = right;
            this->next_ = right;

            int right_idx = 0;
            int end = num_key_;
            Node* child = nullptr;
            for (int i = split_idx; i < end; i++, right_idx++) {
                child = this->getChild(i);
                right->setChild(right_idx, child);

                right->setKey(right_idx, this->getKey(i));
                right->num_key_++;
                num_key_--;
            }
            right->setChild(right_idx, getChild(end));
            num_key_--;

            right->setHighKey(this->getHighKey());
            this->setHighKey(this->getKey(num_key_-1));

            if (idx < split_idx) this->insertChild(idx, new_child);
            else right->insertChild(idx - split_idx, new_child);

            // if it was the root, create new one
            if (parent_ == nullptr) {
                createRoot(this, right);
            }
        } else {
            flag = NONE;
            insertChild(idx, new_child);
        }
        ts_++;
    } else {
        // nothing was changed in this branch node
        my_lock->unlock();
    }

    return true;
}

template <class K, class V>
void BTree<K,V>::Node::insertChild(int idx, Node* child) {
    for (int i = num_key_; i > idx; i--) {
        setChild(i+1, getChild(i));
        this->setKey(i, this->getKey(i-1));
    }
    setChild(idx + 1, child);

    this->setKey(idx, child->getLeftMostKey());
    this->setHighKey(this->getKey(num_key_));
    num_key_++;
}

template <class K, class V>
bool BTree<K,V>::Node::removeLeaf(int idx, K key, Flag& flag, Node*& node) {
    if (idx == 0 || getKey(idx - 1) != key) return false;

    idx--;
    for (int i = idx; i < num_key_ - 1; i++) {
        this->setKey(i, this->getKey(i+1));
        this->setValue(i, this->getValue(i+1));
    }
    num_key_--;

    if (num_key_ == 0) {
        if (prev_ != nullptr) prev_->next_ = next_;
        if (next_ != nullptr) next_->prev_ = prev_;
        flag = REMOVED;
        node = this;
    } else {
        flag = NONE;
    }

    return true;
}

template <class K, class V>
bool BTree<K,V>::Node::removeBranch(int idx, K key, Flag& flag, Node*& node) {
    Node* child = getChild(idx);
    bool success = child->remove(key, flag, node);
    if (!success) return false;

    if (flag & REMOVED) {
        // get rid of child
        for (int i = idx; i < num_key_ - 1; i++) {
            this->setChild(i, getChild(i+1));
            this->setKey(i, getKey(i+1));
        }

        if (idx != num_key_) {
            setChild(num_key_-1, getChild(num_key_));
        }
        num_key_--;

        if (num_key_ == 0) {
            // in case that one child remains,
            // remove this node also
            if (next_) {
                if (next_->isFull()) {
                    next_->redistribute(this, false);
                    flag = (Flag) (flag - REMOVED);
                } else {
                    next_->merge(getChild(0), false);

                    if (prev_ != nullptr) prev_->next_ = next_;
                    next_->prev_ = prev_;
                    flag = (Flag) (flag | REMOVED);
                }
                flag = (Flag) (flag | RIGHT_UPDATE);
            } else if (prev_) {
                if (prev_->isFull()) {
                    prev_->redistribute(this, true);
                    flag = (Flag) (flag - REMOVED);
                } else {
                    prev_->merge(getChild(0), true);

                    if (next_ != nullptr) next_->prev_ = prev_;
                    prev_->next_ = next_;
                    flag = (Flag) (flag | REMOVED);
                }
                flag = (Flag) (flag | LEFT_UPDATE);
            } else {
                // this is a root node
                flag = REMOVED;
            }
            node = this;
        } else {
            this->setHighKey(this->getKey(num_key_-1));
            flag  = (Flag) (flag - REMOVED);
        }
    }

    if (flag & LEFT_UPDATE) {
        if (idx > 0) {
            this->setKey(idx-1, this->getChild(idx)->getLeftMostKey());
        }
        this->setHighKey(this->getKey(num_key_-1));
    }

    if (flag & RIGHT_UPDATE) {
        if (idx < num_key_) {
            this->setKey(idx, this->getChild(idx+1)->getLeftMostKey());
        }
        this->setHighKey(this->getKey(num_key_-1));
    }

    //if (parent_ == nullptr && num_key_ == 0) is_leaf_ = true;
    return true;
}

template <class K, class V>
void BTree<K,V>::Node::merge(Node* node, bool behind) {
    if (behind) {
        K key = node->getLeftMostKey();
        this->setKey(num_key_, key);
        this->setHighKey(key);
        this->setChild(num_key_+1, node);
        num_key_++;
    } else {
        for (int i = num_key_; i > 0; i--) {
            this->setChild(i+1, this->getChild(i));
            this->setKey(i, this->getKey(i-1));
        }
        this->setChild(1, this->getChild(0));
        this->setChild(0, node);
        this->setKey(0, this->getChild(1)->getLeftMostKey());
        num_key_++;
    }
    node->parent_ = this;
}

template <class K, class V>
void BTree<K,V>::Node::redistribute(Node* node, bool behind) {
    int split_idx = num_key_ / 2;
    if (behind) {
        Node* new_child = node->getChild(0);

        int i = 0;
        for (int idx = split_idx + 1; idx < num_key_; idx++, i++) {
            node->setChild(i, this->getChild(idx));

            node->setKey(i, this->getKey(idx));
            node->num_key_++;
        }
        node->setChild(i, this->getChild(num_key_));

        K key = new_child->getLeftMostKey();
        node->setKey(i++, key);
        node->setHighKey(key);
        node->num_key_++;

        node->setChild(i, new_child);

        this->setHighKey(this->getKey(split_idx-1));
        this->num_key_ = split_idx;
    } else {
        node->setKey(0, this->getLeftMostKey());
        node->num_key_++;

        for (int i = 0; i < split_idx; i++) {
            node->setChild(i+1, this->getChild(i));

            node->setKey(i+1, this->getKey(i));
            node->num_key_++;
        }
        node->setChild(split_idx+1, this->getChild(split_idx));

        node->setHighKey(this->getKey(split_idx-1));

        int i = 0;
        for (int idx = split_idx + 1; idx < num_key_; idx++, i++) {
            this->setKey(i, this->getKey(idx));
            setChild(i, getChild(idx));
        }
        setChild(i, getChild(num_key_));

        num_key_ -= (split_idx + 1);
    }
}

template <class K, class V>
K& BTree<K,V>::Node::getKey(int idx) {
    if (is_leaf_) {
        int pair_size = sizeof(K) + sizeof(V);
        return *reinterpret_cast<K*>(
                nodes_ + idx * pair_size + sizeof(V));
    }
    else {
        int pair_size = sizeof(K) + sizeof(Node*);
        return *reinterpret_cast<K*>(
                nodes_ + idx * pair_size + sizeof(Node*));
    }
}

template <class K, class V>
K& BTree<K,V>::Node::getHighKey() {
    return getKey(fanout_ - 1);
}

template <class K, class V>
K BTree<K,V>::Node::getLeftMostKey() {
    if (is_leaf_) return getKey(0);
    else return getChild(0)->getLeftMostKey();
}

template <class K, class V>
typename BTree<K,V>::Node* BTree<K,V>::Node::getLeftMostLeaf() {
    if (is_leaf_) return this;
    else return getChild(0)->getLeftMostLeaf();
}

template <class K, class V>
void BTree<K,V>::Node::setKey(int idx, K key) {
    K& pos = getKey(idx);
    new (&pos) K(key);
}

template <class K, class V>
void BTree<K,V>::Node::setHighKey(K key) {
    K& pos = getHighKey();
    new (&pos) K(key);
}

template <class K, class V>
V& BTree<K,V>::Node::getValue(int idx) {
    int pair_size = sizeof(K) + sizeof(V);
    return *reinterpret_cast<V*>(nodes_ + idx * pair_size);
}

template <class K, class V>
typename BTree<K,V>::Node*& BTree<K,V>::Node::getChild(int idx) {
    int pair_size = sizeof(K) + sizeof(Node*);
    return *reinterpret_cast<Node**>(nodes_ + idx * pair_size);
}

template <class K, class V>
void BTree<K,V>::Node::setChild(int idx, Node* node) {
    getChild(idx) = node;
    node->idx_ = idx;
    node->parent_ = this;
}

template <class K, class V>
bool BTree<K,V>::Node::isFull() {
    if (is_leaf_) return num_key_ == fanout_;
    else return num_key_ == (fanout_ - 1);
}

template <class K, class V>
bool BTree<K,V>::Node::insertSafe() {
    if (is_leaf_) return num_key_ < fanout_;
    else return num_key_ < (fanout_ - 1);
}

template <class K, class V>
bool BTree<K,V>::Node::ordered() {
    K befKey = getKey(0);
    if (!is_leaf_) {
        if (!(getChild(0)->getMaxKey() < befKey)) return false;
        if (!(befKey <= getChild(1)->getMinKey())) return false;
    }
    for (unsigned i = 1; i < num_key_; i++) {
        K key = getKey(i);
        if (!is_leaf_) {
            if (!(getChild(i)->getMaxKey() < key)) return false;
            if (!(key <= getChild(i+1)->getMinKey())) return false;
        }
        if (key <= befKey) {
            return false;
        }
        befKey = key;
    }
    return true;
}

template <class K, class V>
void BTree<K,V>::Node::createRoot(Node* left, Node* right) {
    Node* root = new Node(false, nullptr, -1);

    K key = right->getLeftMostKey();
    root->setKey(0, key);
    root->setHighKey(key);

    root->setChild(0, left);
    root->setChild(1, right);
    root->num_key_ = 1;
}

template <class K, class V>
void BTree<K,V>::Node::printKey() {
    std::cout << "[";
    for (unsigned i = 0; i < num_key_; i++) {
        std::cout << getKey(i) << ",";
    }
    std::cout << "]";
}

/*
 * BTree::iterator
 */
template <class K, class V>
typename BTree<K,V>::iterator& BTree<K,V>::iterator::operator++() {
    next();
    return *this;
}

template <class K, class V>
void BTree<K,V>::iterator::next() {
    idx_++;
    if (idx_ >= node_->num_key_) {
        node_ = node_->next_;
        if (node_ == nullptr) {
            is_ended_ = true;
            return;
        }

        idx_ = 0;
    }
}

/*
 * BTree, public
 */
template <class K, class V>
void BTree<K,V>::release() {
    // collect Node's address before remove them
    std::set<Node*> visited;

    if (root_ != nullptr) {
        root_->collectNode(visited);

        for (auto it = visited.begin(); it != visited.end(); ++it) {
            delete *it;
        }
        delete root_;
        root_ = nullptr;
    }
}

template <class K, class V>
bool BTree<K,V>::find(K key, V& val) {
    while (true) {
        ReadLock read_lock(mx_root_);
        if (root_ == nullptr) return false;

        Flag flag(NONE);
        bool success = root_->find(key, val, flag, &read_lock);

        if (flag == NONE) return success;
    }

    return false;
}

template <class K, class V>
bool BTree<K,V>::multifind(K key, std::vector<V>& vals) {
    while (true) {
        ReadLock read_lock(mx_root_);
        if (root_ == nullptr) return false;

        Flag flag(NONE);
        bool success = root_->multifind(key, vals, flag, &read_lock);

        if (flag == NONE) return success;
    }

    return false;
}

template <class K, class V>
bool BTree<K,V>::exists(K key) {
    V val;
    while (true) {
        ReadLock read_lock(mx_root_);
        if (root_ == nullptr) return false;

        Flag flag(NONE);
        bool success = root_->find(key, val, flag, &read_lock);

        if (flag == NONE) return success;
    }

    return false;
}

template <class K, class V>
typename BTree<K,V>::iterator BTree<K,V>::begin() {
    Node* node = (root_ == nullptr)? nullptr : root_->getLeftMostLeaf();
    return iterator(node);
}

template <class K, class V>
typename BTree<K,V>::iterator BTree<K,V>::begin(K key) {
    return (root_ == nullptr)? iterator() : root_->begin(key);
}

template <class K, class V>
bool BTree<K,V>::insert(K key, V val) {
    WriteLock write_lock(mx_root_);
    if (root_ == nullptr) {
        root_ = new Node(true, nullptr, -1);
    }

    Flag flag(NONE);
    WLockVec parent_locks;
    bool success = root_->insert(key, val, flag,
                        parent_locks, &write_lock, unique_);

    if (flag & SPLIT) root_ = root_->parent_;

    return success;
}

template <class K, class V>
bool BTree<K,V>::remove(K key) {
    // if tree is empty
    if (root_ == nullptr) return false;

    Node* node = nullptr;

    Flag flag(NONE);
    bool success = root_->remove(key, flag, node);

    if (flag & REMOVED) {
        if (root_->num_key_ == 0 && root_->is_leaf_) {
            root_ = nullptr;
        }
        else {
            Node* newroot = root_->getChild(0);
            newroot->setParent(nullptr);
            //delete root_;
            root_ = newroot;
        }
    }

    if (node != nullptr && flag == REMOVED) {
        delete node;
    }
    return success;
}

template <class K, class V>
bool BTree<K,V>::update(K key, V val) {
    while (true) {
        ReadLock read_lock(mx_root_);
        if (root_ == nullptr) return false;

        Flag flag(NONE);
        bool success = root_->update(key, val, flag, &read_lock);

        if (flag == NONE) return success;
    }

    return false;
}

template <class K, class V>
uint64_t BTree<K,V>::countNodesDFS(){
    uint64_t count = 0;
    if (root_ != nullptr)
        count += root_->countNodesDFS();
    return count;
}

template <class K, class V>
void BTree<K,V>::traverseDFS(bool dbg) {
    if (root_ != nullptr) root_->traverseDFS(dbg);
}

template <class K, class V>
void BTree<K,V>::traverseBFS(bool dbg) {
    if (root_ != nullptr) root_->traverseBFS(dbg);
}

template <class K, class V>
bool BTree<K,V>::isSorted() {
    if(root_ != nullptr) {
        assert (root_->parent_ == nullptr);
        return root_->isSorted();
    }
    return true;
}

template <class K, class V>
std::size_t BTree<K,V>::size() {
    if (root_ != nullptr) return root_->size();
    return 0;
}

template <class K, class V>
uint64_t BTree<K,V>::getMemoryUsage() {
    return countNodesDFS() * BTREE_NODE_SIZE + sizeof(this);
}

}  // namespace storage
