// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

// Related include
#include "concurrency/version_map.h"

bool VersionMap::remove(iterator& itr, std::vector<Version*>& deads) {
    bool success = false;
    if (itr) {
        Node* node = itr.node_;

        if (node) {
            Flag flag(NONE);
            success = removeBottomUp(node, itr.idx_, flag, deads);

            if (flag & REMOVED) {
                if (root_->num_key_ == 0 && root_->is_leaf_) root_ = nullptr;
                else {
                    root_ = root_->getChild(0);
                    root_->parent_ = nullptr;
                }
            }
            if (itr.node_->num_key_ == 0) itr.next();
        }
    }

    return success;
}

bool VersionMap::removeBottomUp(Node* node, int idx,
        Flag& flag, std::vector<Version*>& deads) {
    int num_dead = 0;
    for (int i = idx; i < node->num_key_; i++, num_dead++) {
        VersionMapItem item = node->getValue(i);
        if (item.tid_ >= 0) break;

        deads.push_back(item.version_);
    }
    node->num_key_ -= num_dead;

    if (node->num_key_ == 0) {
        flag = REMOVED;
        if (node->parent_) removeBottomUpBranch(node->parent_, node->idx_, flag);

        return true;
    } else {
        for (int i = 0; i < node->num_key_; i++) {
            node->setKey(i, node->getKey(i + num_dead));
            node->setValue(i, node->getValue(i + num_dead));
        }

        return false;
    }
}

void VersionMap::removeBottomUpBranch(Node* node, int idx, Flag& flag) {
    if (flag & REMOVED) {
        // get rid of the child
        for (int i = idx; i < node->num_key_ - 1; i++) {
            node->setChild(i, node->getChild(i+1));
            node->setKey(i, node->getKey(i+1));
        }

        if (idx != node->num_key_) {
            node->setChild(node->num_key_-1, node->getChild(node->num_key_));
        }
        node->num_key_--;

        if (node->num_key_ == 0) {
            // in case that one child remains
            // remove this node also
            if (node->next_) {
                if (node->next_->isFull()) {
                    node->next_->redistribute(node, false);
                    flag = (Flag) (flag - REMOVED);
                } else {
                    node->next_->merge(node->getChild(0), false);

                    if (node->prev_ != nullptr) node->prev_->next_ = node->next_;
                    node->next_->prev_ = node->prev_;
                    flag = (Flag) (flag | REMOVED);
                }
                flag = (Flag) (flag | RIGHT_UPDATE);
            } else if (node->prev_) {
                if (node->prev_->isFull()) {
                    node->prev_->redistribute(node, true);
                    flag = (Flag) (flag - REMOVED);
                } else {
                    node->prev_->merge(node->getChild(0), true);

                    if (node->next_ != nullptr) node->next_->prev_ = node->prev_;
                    node->prev_->next_ = node->next_;
                    flag = (Flag) (flag | REMOVED);
                }
                flag = (Flag) (flag | LEFT_UPDATE);
            } else {
                // this is a root node
                flag = REMOVED;
                return;
            }
        } else {
            node->setHighKey(node->getKey(node->num_key_-1));
            flag  = (Flag) (flag - REMOVED);
        }
    }
    
    if (flag & LEFT_UPDATE) {
        if (idx > 0) {
            node->setKey(idx-1, node->getChild(idx)->getLeftMostKey());
        }
        node->setHighKey(node->getKey(node->num_key_-1));
    }

    if (flag & RIGHT_UPDATE) {
        if (idx < node->num_key_) {
            node->setKey(idx, node->getChild(idx+1)->getLeftMostKey());
        }
        node->setHighKey(node->getKey(node->num_key_-1));
    }

    if (node->parent_ != nullptr) {
        removeBottomUpBranch(node->parent_, node->idx_, flag);
    }
}
