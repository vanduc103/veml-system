// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef STORAGE_UTIL_BTREE_INDEX_H_
#define STORAGE_UTIL_BTREE_INDEX_H_

// Project include
#include "storage/util/btree.h"
#include "common/types/eval_type.h"

namespace storage {

/**
 * @brief B+tree index for relational table
 * @author Ilju Lee, ijlee@kdb.snu.ac.kr
 */
class BTreeIndex {
  public:
    /**
     * @brief BTreeIndex constructor
     * @param std::string table_name: name of table
     * @param std::string name: name of index
     * @param bool unique: true if the tree has unique constraint
     */
    BTreeIndex(std::string table_name, std::string name, bool unique)
        : table_name_(table_name), name_(name), index_(unique) {}

    /**
     * @brief release index
     */
    void release() { index_.release(); }

    /**
     * @brief find a record for key
     * @param EvalValue key: finding key
     * @param LogicalPtr idx: logical address of indexed record
     */
    bool find(EvalValue key, LogicalPtr& idx) {
        return index_.find(key, idx);
    }

    /**
     * @brief find multiple records for key
     * @param EvalValue key: finding key
     * @param LptrVec idxs: logical addresses of indexed records
     */
    bool multiFind(EvalValue key, LptrVec& idxs) {
        return index_.multifind(key, idxs);
    }

    /**
     * @brief insert a value-record_id pair to index
     * @param EvalValue key: value of indexing key
     * @param LogicalPtr idx: logical address of indexed record
     * @return true if success
     */
    bool insert(EvalValue key, LogicalPtr idx) {
        return index_.insert(key, idx);
    }

    /**
     * @brief update a value-record_id pair in index
     * @param EvalValue key: value of indexing key
     * @param LogicalPtr idx: new logical address to update
     * @return true if success
     */
    bool update(EvalValue key, LogicalPtr idx) {
        return index_.update(key, idx);
    }

    /**
     * @brief remove a value-record_id pair from index
     * @param EvalValue key: value of indexing key
     * @return true if success
     */
    bool remove(EvalValue key) { return index_.remove(key); }

    /**
     * @brief get byte size of memory consumption
     * @return byte size of memory
     */
    size_t getMemoryUsage() { return index_.getMemoryUsage(); }

    /**
     * @brief check whether an index has unique constraint
     * @return true if it has unique constraint
     */
    bool isUnique() { return index_.isUnique(); }

    /**
     * @brief get name of index
     * @return index name
     */
    std::string getName() { return name_; }

    /**
     * @brief get name of indexed table
     * @return table name
     */
    std::string getTableName() { return table_name_; }

  private:
    std::string table_name_;
    std::string name_;

    BTree<EvalValue, LogicalPtr> index_;
};

}

#endif  // STORAGE_UTIL_BTREE_INDEX_H_
