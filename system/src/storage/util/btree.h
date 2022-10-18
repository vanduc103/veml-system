// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef STORAGE_UTIL_BTREE_H_
#define STORAGE_UTIL_BTREE_H_

// Project include
#include "storage/def_const.h"
#include "concurrency/mutex_lock.h"

// C & C++ include
#include <set>

class VersionMap;

namespace storage {

/**
 * @brief B+tree
 * @details handle concurrent read/write of key-value pair
 * @author Ilju Lee, ijlee@kdb.snu.ac.kr
 */
template <class K, class V>
class BTree {
  public:
    /**
     * @brief an enum type of btree operation flag
     * @li NONE: normal
     * @li SPLIT: a branch node was splited while insert
     * @li REMOVE: a node was removed while 
     * @li LEFT_UPDATE: left side of removed node was updated
     * @li RIGHT_UPDATE: right side of removed node was updated
     * @li RETRY: retry search due to timestamp ordering
     */
    enum Flag {
        NONE = 0x00, SPLIT = 0x01, REMOVED = 0x02,
        LEFT_UPDATE = 0x04, RIGHT_UPDATE = 0x08, RETRY = 0x0f
    };
    typedef std::atomic<int64_t> TS;
    typedef std::vector<WriteLock*> WLockVec;

    class iterator;

    /**
     * @brief node in btree
     */
    class Node {
      public:
        /**
         * @brief node constructor
         * @param bool is_leaf: true if the node leaf
         * @param Node* parent: parent node
         * @param int32_t idx: index in parent node
         */
        Node(bool is_leaf, Node* parent, int32_t idx);

        /**
         * @brief find a value for key
         * @param K key: finding key
         * @param V val: return value
         * @param Flag flag: operation flag
         * @param ReadLock my_lock: acquire lock of child to proceed
         * @return true if success
         */
        bool find(K key, V& val, Flag& flag, ReadLock* my_lock);

        /**
         * @brief find multiple values for key
         * @param K key: finding key
         * @param std::vector<V> vals: return values
         * @param Flag flag: operation flag
         * @param ReadLock my_lock: acquire lock of child to proceed
         * @return true if success
         */
        bool multifind(K key, std::vector<V>& vals, Flag& flag, ReadLock* my_lock);

        /**
         * @brief create an iterator that start from key
         * @param K key: starting key
         */
        iterator begin(K key);

        /**
         * @brief insert a key-value pair to node
         * @param K key: key to insert
         * @param V Val: value to insert
         * @param Flag flag: operation flag
         * @param WLockVec parent_locks: lock chain for nodes can be updated
         * @param WriteLock my_lock: acquire lock of child to proceed
         * @param bool unique: true if the tree has unique constraint
         * @return true if success
         */
        bool insert(K key, V val, Flag& flag,
                WLockVec& parent_locks, WriteLock* my_lock, bool unique);

        /**
         * @brief remove a key-value pair
         * @param K key: key to remove
         * @param Flag flag: operation flag
         * @param Node* node: removed node
         */
        bool remove(K key, Flag& flag, Node*& node);

        /**
         * @brief update value for a key
         * @param K key: key to update
         * @param V Val: new value
         * @param Flag flag: operation flag
         * @param ReadLock my_lock: acquire lock of child to proceed
         */
        bool update(K key, V val, Flag& flag, ReadLock* my_lock);

        /**
         * @brief collect all nodes in tree
         * @param std::set<Node*> collected nodes
         */
        void collectNode(std::set<Node*>& visited);

        /**
         * @brief count the number of nodes
         * @return the number of nodes in tree
         */
        uint64_t countNodesDFS();

        /**
         * @brief traverse tree by depth first search
         * @details check link between nodes
         * @param bool dbg: if true cout key info
         */
        void traverseDFS(bool dbg);

        /**
         * @brief traverse tree by breadth first search
         * @details check link between nodes
         * @param bool dbg: if true cout key info
         */
        void traverseBFS(bool dbg);

        /**
         * @brief check nodes in a height are sorted
         * @return true it has no conflict
         */
        bool isSorted();

        /**
         * @brief get the number of keys
         * @return the number of keys in tree
         */
        std::size_t size();

        static const std::size_t NODE_SIZE
            = BTREE_NODE_SIZE
                - sizeof(Node*) - sizeof(Node*) - sizeof(Node*)
                - sizeof(Mutex) - sizeof(TS)
                - sizeof(int32_t) - sizeof(int32_t)
                - sizeof(int32_t) - sizeof(bool);

        friend class BTree;
        friend class BTree::iterator;
        friend VersionMap;

      protected:
        /**
         * @brief search index for key in a node
         * @param K key: search key
         * @return index for key
         */
        int searchInNode(K key);

        /**
         * @brief binary search index for key in a node
         * @param K key: search key
         * @return index for key
         */
        int lower_bound(K key);

        /**
         * @brief find left-side values that have equal key (form multifind)
         * @param K key: search key
         * @param std::vector<V>& vals: collected values
         * @param int idx: current index in node
         */
        void find_left(K key, std::vector<V>& vals, int idx);

        /**
         * @brief find right-side values that have equal key (form multifind)
         * @param K key: search key
         * @param std::vector<V>& vals: collected values
         * @param int idx: current index in node
         */
        void find_right(K key, std::vector<V>& vals, int idx);

        /**
         * @brief insert a key-value pair to leaf node
         * @param int idx: index in node
         * @param K key: key to insert
         * @param V Val: value to insert
         * @param Flag flag: operation flag
         * @param WLockVec parent_locks: lock chain for nodes can be updated
         * @param WriteLock my_lock: acquire lock of child to proceed
         * @param bool unique: true if the tree has unique constraint
         * @return true if success
         */
        bool insertLeaf(int idx, K key, V val, Flag& flag,
                WLockVec& parent_locks, WriteLock* my_lock, bool unique);

        /**
         * @brief insert a key-value pair to branch node
         * @param int idx: index in node
         * @param K key: key to insert
         * @param V Val: value to insert
         * @param Flag flag: operation flag
         * @param WLockVec parent_locks: lock chain for nodes can be updated
         * @param WriteLock my_lock: acquire lock of child to proceed
         * @param bool unique: true if the tree has unique constraint
         * @return true if success
         */
        bool insertBranch(int idx, K key, V val, Flag& flag,
                WLockVec& parent_locks, WriteLock* my_lock, bool unique);

        /**
         * @brief insert a new chile node to branch node
         * @param int idx: index in node
         * @param Node node: node to insert
         */
        void insertChild(int idx, Node* child);

        /**
         * @brief remove a key-value in leaf node
         * @param int idx: index in node
         * @param K key: key to insert
         * @param Flag flag: operation flag
         * @param Node* node: removed node
         */
        bool removeLeaf(int idx, K key, Flag& flag, Node*& node);

        /**
         * @brief find a key-value from to remove
         * @param int idx: index in node
         * @param K key: key to insert
         * @param Flag flag: operation flag
         * @param Node* node: removed node
         */
        bool removeBranch(int idx, K key, Flag& flag, Node*& node);

        /**
         * @brief merge two nodes
         * @param Node* node: node to merge
         * @param bool behind: if true merge with next node
         */
        void merge(Node* node, bool behind);

        /**
         * @brief redistribute two nodes
         * @param Node* node: node to redistribute
         * @param bool behind: if true redistribute with next node
         */
        void redistribute(Node* node, bool behind);

        /**
         * @brief get key in node
         * @param int idx: index in node
         * @return key for index
         */
        K& getKey(int idx);

        /**
         * @brief get highest key in node
         * @return highest key
         */
        K& getHighKey();

        /**
         * @brief get max key in node
         * @return max key
         */
        K& getMaxKey() {return getKey(num_key_-1);}

        /**
         * @brief get min key in node
         * @return min key
         */
        K& getMinKey() {return getKey(0);}

        /**
         * @brief get left most key in node
         * @return left most key
         */
        K getLeftMostKey();

        /**
         * @brief get left most node
         * @return left most node
         */
        Node* getLeftMostLeaf();

        /**
         * @brief set key in node
         * @param int idx: index in node
         * @param K key: key to set
         */
        void setKey(int idx, K key);

        /**
         * @brief set high key in node
         * @param int idx: index in node
         * @param K key: key to set
         */
        void setHighKey(K key);

        /**
         * @brief get value in node
         * @param int idx: index in node
         * @return value for index
         */
        V& getValue(int idx);

        /**
         * @brief set value in node
         * @param int idx: index in node
         * @param V val: value to set
         */
        void setValue(int idx, V val) { getValue(idx) = val; }

        /**
         * @brief get child node
         * @param int idx: index of child
         * @return child node
         */
        Node*& getChild(int idx);

        /**
         * @brief get child node
         * @param int idx: index of child
         * @param Node node: child node
         */
        void setChild(int idx, Node* node);

        /**
         * @brief set parent node
         * @param Node* node parent node
         */
        void setParent(Node *node) { parent_ = node; }

        /**
         * @brief check the node is full
         * @return true if the node is full
         */
        bool isFull();

        /**
         * @brief check the node is safe to insert new key
         * @return true if safe
         */
        bool insertSafe();

        /**
         * @brief check the keys in a node are ordered
         * @return true if they are ordered
         */
        bool ordered();

        /**
         * @brief create a new root
         * @param Node* left: left child
         * @param Node* righ: right child
         */
        void createRoot(Node* left, Node* right);

        /**
         * @brief cout keys in node
         */
        void printKey();

        // link variable
        Node* parent_;
        Node* prev_;
        Node* next_;

        // concurrency control
        Mutex   mx_node_;
        TS      ts_;

        // node variable
        int32_t idx_;
        int32_t num_key_;
        int32_t fanout_;
        bool    is_leaf_;
        char    nodes_[NODE_SIZE];
    };

    /**
     * @brief btree iterator
     */
    class iterator {
      public:
        /**
         * @brief create a null iterator
         */
        iterator() : is_ended_(false) {}

        /**
         * @brief BTree::iterator constructor
         * @param Node* node: starting node
         * @param int32_t idx: starting index
         */
        iterator(Node* node, int32_t idx = 0) : idx_(idx), is_ended_(false) {
            if (node == nullptr || node->num_key_ == 0) is_ended_ = true;
            else {
                node_ = node;
            }
        }

        /**
         * @brief BTree::iterator copy constructor
         * @param iterator other: iterator to copy
         */
        iterator(const iterator& other) {
            node_ = other.node_;
            idx_ = other.idx_;
            is_ended_ = other.is_ended_;
        }

        /**
         * @brief check whether iteration is end
         * @return true if not finished
         */
        operator bool() { return !is_ended_; }

        /**
         * @brief check whether iteration is end
         * @return true if finished
         */
        bool isEnd() { return is_ended_;}

        /**
         * @brief get value of current iterator
         * @return current value
         */
        V operator*() { return node_->getValue(idx_); }

        /**
         * @brief get key of current iterator
         * @return current key
         */
        K& first() { return node_->getKey(idx_); }

        /**
         * @brief get value of current iterator
         * @return current value
         */
        V& second() { return node_->getValue(idx_); }

        /**
         * @brief move to nex
         */
        iterator&   operator++();

        /**
         * @brief move to nex
         */
        void        next();

        friend class BTree;
        friend VersionMap;

      protected:
        Node*       node_;
        int32_t     idx_;
        bool        is_ended_;
    };

    /**
     * @brief BTree constructor
     * @param bool unique: true if the tree has unique constraint
     */
    BTree(bool unique = false) : root_(nullptr), unique_(unique) {}

    /**
     * @brief release all nodes in btree
     */
    void release();

    /**
     * @brief find a value for key
     * @param K key: finding key
     * @param V val: return value
     * @return true if success
     */
    bool find(K key, V& val);

    /**
     * @brief find multiple values for key
     * @param K key: finding key
     * @param std::vector<V> vals: return values
     * @return true if success
     */
    bool multifind(K key, std::vector<V>& vals);

    /**
     * @brief check a key exists in tree
     * @return true if exists
     */
    bool exists(K key);

    /**
     * @brief create an iterator
     * @return iterator
     */
    iterator begin();

    /**
     * @brief create an iterator from a specific key
     * @return iterator
     */
    iterator begin(K key);

    /**
     * @brief insert a key-value pair to tree
     * @param K key: key to insert
     * @param V Val: value to insert
     * @return true if success
     */
    bool insert(K key, V val);

    /**
     * @brief remove a key-value pair
     * @param K key: key to remove
     */
    bool remove(K key);

    /**
     * @brief update value for a key
     * @param K key: key to update
     * @param V Val: new value
     */
    bool update(K key, V val);

    /**
     * @brief count the number of nodes
     * @return the number of nodes in tree
     */
    uint64_t countNodesDFS();

    /**
     * @brief traverse tree by depth first search
     * @details check link between nodes
     * @param bool dbg: if true cout key info
     */
    void traverseDFS(bool dbg = false);

    /**
     * @brief traverse tree by breadth first search
     * @details check link between nodes
     * @param bool dbg: if true cout key info
     */
    void traverseBFS(bool dbg = false);

    /**
     * @brief check nodes in a height are sorted
     * @return true it has no conflict
     */
    bool isSorted();

    /**
     * @brief get the number of keys
     * @return the number of keys in tree
     */
    std::size_t size();

    /**
     * @brief get byte size of memory consumption
     * @return byte size of memory
     */
    std::size_t getMemoryUsage();

    /**
     * @brief check whether a tree is empty
     * @return true if it is empty
     */
    bool isEmpty() { return root_ == nullptr; }

    /**
     * @brief check whether a tree has unique constraint
     * @return true if it has unique constraint
     */
    bool isUnique() { return unique_; }

  protected:
    Mutex   mx_root_;
    Node*   root_;
    bool    unique_;
};

}  // namespace storage

#include "storage/util/btree.inl"

#endif  // STORAGE_UTIL_BTREE_H_
