// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef STORAGE_RELATION_COLUMN_TABLE_H_
#define STORAGE_RELATION_COLUMN_TABLE_H_

// Project include
#include "storage/relation/table.h"
#include "storage/base/paged_array_encoded.h"
#include "storage/util/bit_vector.h"

namespace storage {

/**
 * @brief column table data store in RTI
 * @author Ilju Lee, ijlee@kdb.snu.ac.kr
 */
class ColumnTable : public Table {
  public:
    /**
     * brief ColumnTable iterator
     */
    class iterator : public Table::iterator_wrapper {
      public:
        iterator(Table* table);
        iterator(Table* table, SgmtLptrVec& sgmt_ids);
        iterator(const iterator& other);
        iterator& operator=(const iterator& other);

        operator bool() { return itr_; }
        bool good()     { return itr_; }
        bool isValid()  { return itr_.isValid(); }
        void init()     { itr_.init(); }

        iterator& operator++() {
            this->next();
            return *this;
        }
        void next();

        // access functions
        LogicalPtr getLptr() const { return itr_.getLptr(); }
        void* getPtr() const;

        EvalValue getValue(unsigned fid);
        EvalVec getRecord();                 
        EvalVec getRecord(IntVec& fids);
        StrVec getRecordStr();               
        StrVec getRecordStr(IntVec& fids);

      private:
        BitVector::iterator itr_;
        std::vector<PABase*> cols_;
    };

    /**
     * @brief ColumnTable constructor
     * @param std::string name: table name
     * @param FieldInfoVec fields: field infos
     */
    ColumnTable(const std::string& name,
            FieldInfoVec& finfos);

    /**
     * @brief ColumnTable constructor
     * @details constructor for recovery
     * @param Table::Type type: type of table
     * @param std::string name: table name
     * @param FieldInfoVec fields: field infos
     * @param std::vector<uint32_t> pa_ids: paged-array ids for columns
     * @param uint32_t valid_id: paged-array id for valid bit_vector
     */
    ColumnTable(Table::Type type,
            const std::string& name, const FieldInfoVec& fields,
            std::vector<uint32_t> pa_ids, uint32_t valid_id);

    /**
     * @brief ColumnTable destructor
     */
    ~ColumnTable();

    /**
     * @brief insert a record to any unused slot in table
     * @param EvalVec values: values of each field
     * @return address of insert record
     */
    AddressPair insertRecord(EvalVec& values);

    /**
     * @brief insert a record to any unused slot in table
     * @param StrVec values: values of each field
     * @return address of insert record
     */
    AddressPair insertRecord(StrVec& values);

    /**
     * @brief insert a record to designated slot in table
     * @param LogicalPtr idx: index of slot to insert
     * @param EvalVec values: values of each field
     * @return address of insert record
     */
    AddressPair insertRecord(LogicalPtr idx, EvalVec& values);

    /**
     * @brief delete a record
     * @param LogicalPtr idx: index of slot to delete
     * @return true if success
     */
    bool deleteRecord(LogicalPtr idx);

    /**
     * @brief truncate table
     */
    void truncate();

    /**
     * temporary deprecated functions
     */
    bool updateRecord(LogicalPtr idx,
                    const IntVec& fids, const EvalVec& values);
    AddressPair insertRecord(Transaction& trans,
                    EvalVec& values);
    AddressPair insertRecord(Transaction& trans,
                    LogicalPtr idx, EvalVec& values);
    bool deleteRecord(Transaction& trans, LogicalPtr idx);
    bool updateRecord(Transaction& trans, LogicalPtr idx,
                    const IntVec& fids, const EvalVec& values);
    EvalValue getValue(Transaction& trans, LogicalPtr idx, int fid);
    EvalVec getRecord(Transaction& trans, LogicalPtr idx);
    EvalVec getRecord(Transaction& trans,
                LogicalPtr idx, IntVec& fids);

    /**
     * @brief get a value from a record
     * @param LogicalPtr idx: index of slot to read
     * @param int fid: id of field to read
     * @return single eval value
     */
    EvalValue getValue(LogicalPtr idx, int fid);

    /**
     * @brief get all values from a record
     * @param LogicalPtr idx: index of slot to read
     * @return all eval values of a record
     */
    EvalVec getRecord(LogicalPtr idx);

    /**
     * @brief get specific values from a record
     * @param LogicalPtr idx: index of slot to read
     * @param std:vector<int> fids: ids of field to read
     * @return requested values of a record
     */
    EvalVec getRecord(LogicalPtr idx, IntVec& fids);

    /**
     * @brief get all values from a record
     * @param LogicalPtr idx: index of slot to read
     * @return all eval values of a record
     */
    StrVec getRecordStr(LogicalPtr idx);

    /**
     * @brief get specific values from a record
     * @param LogicalPtr idx: index of slot to read
     * @param std:vector<int> fids: ids of field to read
     * @return requested values of a record
     */
    StrVec getRecordStr(LogicalPtr idx, IntVec& fids);

    /**
     * @brief return data as tensor data type
     */
    //torch::Tensor toTensor(std::string type);

    /**
     * check a slot is valid
     */
    bool isValid(LogicalPtr idx);

    /**
     * @brief compress all DYNAMIC segments of column table
     * @return true if success
     */
    bool compressDelta();

    /**
     * @brief compress specific DYNAMIC segments of column table
     * @param SegmentLptr sgmt_id: id of segment to compress
     * @return true if success
     */
    bool compressDelta(SegmentLptr sgmt_id);

    /**
     * @brief get id of paged-array for a column
     */
    uint32_t getPagedArrayId(int32_t fid = 0) const;

    /**
     * @brief get ids of all paged_arrays
     */
    void getPagedArrayIds(std::vector<uint32_t>& pa_ids) const;

    /**
     * @brief get ids of all segments
     */
    void getAllSgmtIds(SgmtLptrVec& sgmt_ids) const;

    /**
     * @brief return the number of records
     */
    size_t getNumRecords();

    /**
     * @brief get valid bit_vector
     */
    BitVector& getValid() { return valid_; }

    /**
     * @brief set the segments so that they can no longer allocate (for dbg)
     */
    void finalizeCurrentSgmt();

    /**
     * @brief create an iterator
     */
    iterator begin();

    /**
     * @brief create an iterator
     */
    iterator begin(SgmtLptrVec& sgmt_ids);

    /**
     * @brief get memory statistics
     */
    size_t getMemoryUsage(std::ostream& os, bool verbose=false, int level=0);

    /*
     * friend class
     */
    friend class Table;

  protected:
    DISALLOW_COMPILER_GENERATED(ColumnTable);

    /**
     * @brief insert a record to designated slot in table
     * @param LogicalPtr idx: index of slot to insert
     * @param EvalVec values: values of each field
     * @return address of insert record
     */
    AddressPair insert(LogicalPtr idx, const EvalVec& values);

    /**
     * @brief insert a record to any unused slot in table (transactional)
     * @param EvalVec values: values of each field
     * @return address of insert record
     */
    AddressPair insert(Transaction& trans,
                    LogicalPtr idx, const EvalVec& values);

    /*
     * member variables
     */
    BitVector valid_;
    std::vector<PABase*> columns_;
};

}  // namespace storage

#endif  // STORAGE_RELATION_COLUMN_TABLE_H
