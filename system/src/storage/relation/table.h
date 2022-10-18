// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef STORAGE_RELATION_TABLE_H_
#define STORAGE_RELATION_TABLE_H_

// Project include
#include "storage/def_const.h"
#include "storage/catalog.h"
#include "storage/util/partition_func.h"

namespace dan {
    class TableBinary;
}

namespace boost {
namespace archive {
    class binary_oarchive;
    class binary_iarchive;
}
}

namespace storage {

/**
 * @brief base implementation of relational table
 * @details 3 kinds of table are supported(RowTable, ColumnTable, StreamTable)
 * @author Ilju Lee, ijlee@kdb.snu.ac.kr
 */
class Table {
  public:
    /**
     * @brief an enum type of Table
     * @li ROW: RowTable
     * @li COLUMN: ColumnTable
     * @li STREAM: StreamTable
     * @li NUM_TYPE: null
     */
    enum Type { ROW, COLUMN, NUM_TYPE };

    class iterator_wrapper {
      public:
        // constructor
        iterator_wrapper(Table* table);
        iterator_wrapper(const FieldInfoVec& finfos);
        virtual ~iterator_wrapper() {}

        // valid function
        virtual operator bool() = 0;
        virtual bool good() = 0;
        virtual bool isValid() = 0;
        virtual void init() = 0;

        // iterate functions
        virtual iterator_wrapper& operator++() = 0;
        virtual void next() = 0;

        // access functions
        virtual LogicalPtr getLptr() const = 0;
        virtual void* getPtr() const = 0;

        virtual EvalValue getValue(unsigned fid) = 0;
        virtual EvalVec getRecord() = 0;
        virtual EvalVec getRecord(IntVec& fids) = 0;
        virtual StrVec getRecordStr() = 0;
        virtual StrVec getRecordStr(IntVec& fids) = 0;

      protected:
        FieldInfoVec finfos_;
    };

    class iterator {
      public:
        iterator(Table* table);
        iterator(Table* table, SgmtLptrVec& sgmt_ids);
        iterator(const iterator& other);
        ~iterator();

        // valid function
        operator bool() { return good(); }
        bool good()     { return itr_->good(); }
        bool isValid()  { return itr_->isValid(); }
        void init()     { itr_->init(); }

        // iterate functions
        iterator& operator++() {
            itr_->next();
            return *this;
        }
        void next() { itr_->next(); }

        // access functions
        LogicalPtr getLptr() const  { return itr_->getLptr(); }
        void* getPtr() const        { return itr_->getPtr(); }

        EvalValue getValue(unsigned fid)    { return itr_->getValue(fid); }
        EvalVec getRecord()                 { return itr_->getRecord(); }
        EvalVec getRecord(IntVec& fids)     { return itr_->getRecord(fids); }
        StrVec getRecordStr()               { return itr_->getRecordStr(); }
        StrVec getRecordStr(IntVec& fids)   { return itr_->getRecordStr(fids); }

      protected:
        Table::Type type_;
        iterator_wrapper* itr_;
    };

    typedef struct Index {
        Index() {}
        Index(uint32_t fid, BTreeIndex* index, bool unique)
            : fid_(fid), index_(index), unique_(unique) {}

        uint32_t fid_;
        BTreeIndex* index_;
        bool unique_;

      private:
        friend class boost::serialization::access;
        template <class Archive>
        void serialize(Archive & ar, const unsigned int version) {
            ar & fid_;
            ar & unique_;
        }
    } Index;
    typedef std::vector<Index> IndexVec;

    /**
     * @brief create a table
     * @param Type type: type of table
     * @param std::string name: name of table
     * @param CreateFieldVec fields: name, value type, offset of each field
     * @return created table
     */
    static Table* create(Type type,
                        const std::string& name,
                        CreateFieldVec& fields,
                        int64_t increment_fid = -1,
                        int64_t increment_start = 0);

    /**
     * @brief create a table with .schema file
     * @param std::string type: type of table
     * @param std::string name: name of table
     * @param std::string schema: path of .schema file
     * @param std::string delim: delimiter of .schema file
     * @return created table
     */
    static Table* createFromFile(std::string type, std::string name,
                    std::string schema, std::string delim);

    /**
     * @brief Table destructor
     * @details release index of table
     */
    virtual ~Table();

    /**
     * @brief set auto increment field and distribution
     * @return true if it is partitioned and have to be distribute
     */
    bool prepareInsert(EvalVec& values);

    /**
     * @brief insert a record to any unused slot in table
     * @param EvalVec values: values of each field
     * @return address of insert record
     */
    virtual AddressPair insertRecord(EvalVec& values) = 0;

    /**
     * @brief insert a record to any unused slot in table
     * @param StrVec values: values of each field
     * @return address of insert record
     */
    virtual AddressPair insertRecord(StrVec& values) = 0;

    /**
     * @brief insert a record batch to table
     * @param std::vector<EvalVec> batch: batch of record
     * @return address of insert record
     */
    bool insertRecord(std::vector<EvalVec>& batch);

    /**
     * @brief insert a record to designated slot in table
     * @param LogicalPtr idx: index of slot to insert
     * @param EvalVec values: values of each field
     * @return address of insert record
     */
    virtual AddressPair insertRecord(LogicalPtr idx, EvalVec& values) = 0;

    /**
     * @brief delete a record
     * @param LogicalPtr idx: index of slot to delete
     * @return true if success
     */
    virtual bool deleteRecord(LogicalPtr idx) = 0;

    bool deleteRecords(LptrVec& idxs);

    /**
     * @brief truncate table
     */
    virtual void truncate() = 0;

    /**
     * @brief update a record
     * @param LogicalPtr idx: index of slot to update
     * @param IntVec fids: ids of field to update
     * @param EvalVec values: new values
     * @return true if success
     */
    virtual bool updateRecord(LogicalPtr idx,
                    const IntVec& fids, const EvalVec& values) = 0;

    /**
     * @brief insert a record to any unused slot in table (transactional)
     * @param EvalVec values: values of each field
     * @return address of insert record
     */
    virtual AddressPair insertRecord(Transaction& trans, EvalVec& values) = 0;

    /**
     * @brief insert a record to designated slot in table (transactional)
     * @param LogicalPtr idx: index of slot to insert
     * @param EvalVec values: values of each field
     * @return address of insert record
     */
    virtual AddressPair insertRecord(Transaction& trans,
                            LogicalPtr idx, EvalVec& values) = 0;

    /**
     * @brief delete a record (transactional)
     * @param LogicalPtr idx: index of slot to delete
     * @return true if success
     */
    virtual bool deleteRecord(Transaction& trans, LogicalPtr idx) = 0;

    /**
     * @brief update a record (transactional)
     * @param LogicalPtr idx: index of slot to update
     * @param IntVec fids: ids of field to update
     * @param EvalVec values: new values
     * @return true if success
     */
    virtual bool updateRecord(Transaction& trans, LogicalPtr idx,
                            const IntVec& fids,
                            const EvalVec& values) = 0;

    /**
     * @brief get a value from a record
     * @param LogicalPtr idx: index of slot to read
     * @param int fid: id of field to read
     * @return single eval value
     */
    virtual EvalValue getValue(LogicalPtr idx, int fid) = 0;

    /**
     * @brief get all values from a record
     * @param LogicalPtr idx: index of slot to read
     * @return all eval values of a record
     */
    virtual EvalVec getRecord(LogicalPtr idx) = 0;

    /**
     * @brief get specific values from a record
     * @param LogicalPtr idx: index of slot to read
     * @param std:vector<int> fids: ids of field to read
     * @return requested values of a record
     */
    virtual EvalVec getRecord(LogicalPtr idx, IntVec& fids) = 0;

    /**
     * @brief get all values from a record
     * @param LogicalPtr idx: index of slot to read
     * @return all eval values of a record
     */
    virtual StrVec getRecordStr(LogicalPtr idx) = 0;

    /**
     * @brief get specific values from a record
     * @param LogicalPtr idx: index of slot to read
     * @param std:vector<int> fids: ids of field to read
     * @return requested values of a record
     */
    virtual StrVec getRecordStr(LogicalPtr idx, IntVec& fids) = 0;

    /**
     * check a slot is valid
     */
    virtual bool isValid(LogicalPtr idx) = 0;

    /**
     * @brief get a value from a record (transactional)
     * @param LogicalPtr idx: index of slot to read
     * @param int fid: id of field to read
     * @return single eval value
     */
    virtual EvalValue getValue(Transaction& trans,
                        LogicalPtr idx, int fid) = 0;

    /**
     * @brief get all values from a record (transactional)
     * @param LogicalPtr idx: index of slot to read
     * @return all eval values of a record
     */
    virtual EvalVec getRecord(Transaction& trans, LogicalPtr idx) = 0;

    /**
     * @brief get specific values from a record (transactional)
     * @param LogicalPtr idx: index of slot to read
     * @param std:vector<int> fids: ids of field to read
     * @return requested values of a record
     */
    virtual EvalVec getRecord(Transaction& trans,
                        LogicalPtr idx, IntVec& fids) = 0;

    /**
     * @brief return data as tensor data type
     */
    //virtual torch::Tensor toTensor(std::string type) = 0;

    /**
     * @brief create a btree index on table
     * @param uint32_t fid: id of field to generate index
     * @param bool unique: true if the field has unique constraint
     */
    void createIndex(uint32_t fid, bool unique = false);

    /**
     * @brief drop all indexes on table
     */
    void dropIndex();

    /**
     * @brief rebuild index
     */
    void rebuildIndex(int dop=8);

    /**
     * @brief insert value-record_id pair to index
     * @param EvalVec values: newly inserted record to table
     * @param LogicalPtr idx: index of record inserted
     * @return true if success
     */
    bool insertToIndex(const EvalVec& values, LogicalPtr idx);

    /**
     * @brief remove a record from index
     * @param LogicalPtr idx: index of record removed
     * @return true if success
     */
    bool removeFromIndex(LogicalPtr idx);

    /**
     * @brief check a specific index has unique constraint
     * @param uint32_t fid: id of field to check
     * @return if the index has unique constraint
     */
    bool uniqueIndex(uint32_t fid);
    
    /**
     * @brief search record_id for a value
     * @param EvalValue val: search value
     * @param uint32_t fid: id of field to search
     * @param LogicalPtr idx: searched index
     * @return false if the field does not have index
     */
    bool indexSearch(EvalValue val, uint32_t fid, LogicalPtr& idx);

    /**
     * @brief search multiple record_ids for a value
     * @param EvalValue val: search value
     * @param uint32_t fid: id of field to search
     * @param LptrVec idxs: list of searched index
     * @return false if the field does not have index
     */
    bool indexMultiSearch(EvalValue val, uint32_t fid, LptrVec& idxs);

    /**
     * @brief get filtered records using index
     * @param int64_t fid: id of field for indexing
     * @param EvalValue val: filter value
     * @param IntVec fids: id of fields to get
     * @return filtered data
     */
    std::vector<EvalVec> indexSearch(int64_t fid, EvalValue& val, IntVec fids);

    /**
     * @brief delete a record
     * @param EvalValue value: value for delete
     * @param uint32_t fid: if of field to search
     * @return true if success
     */
    bool deleteFromIndex(EvalValue value, uint32_t fid);

    /**
     * @brief import a csv file to table
     * @param std::string name: name of table to import
     * @param std::string csv: path of csv file
     * @param std::string delim: delimiter of csv file
     * @param size_t batch: size of batch
     * @param int dop: degree of parallelism
     * @param bool header: true if file contains header row
     * @param bool seq: true if data in file should be imported sequentially
     */
    static void import(std::string name, std::string csv, std::string delim,
                    bool seq=false, bool header=true, size_t batch=10000, int dop=8);

    /**
     * @brief import a csv file to table
     * @param std::string csv: path of csv file
     * @param std::string delim: delimiter of csv file
     * @param size_t batch: size of batch
     * @param int dop: degree of parallelism
     * @param bool header: true if file contains header row
     * @param bool seq: true if data in file should be imported sequential
     */
    bool importCSV(std::string csv, std::string delim,
                bool seq, bool header, size_t batch, int dop);

    /**
     * @brief create a table and import csv file
     * @param std::string type: type of table
     * @param std::string name: name of table
     * @param std::string schema: path of .schema file
     * @param std::string csv: path of csv file
     * @param std::string delim: delimiter of csv file
     * @param bool seq: true if data in file should be imported sequentially
     * @return created table
     */
    static Table* load(std::string type, std::string name, std::string schema,
                    std::string csv, std::string delim,
                    bool seq=false, bool header=true);

    /**
     * @brief create an iterator
     */
    iterator begin();

    /**
     * @brief create an iterator
     */
    iterator begin(SgmtLptrVec& sgmt_ids);

    /**
     * @brief partition table
     * @param int32_t fid: id of partitioning field
     * @param StrVec node_addrs: ip address of nodes
     */
    bool partition(int32_t fid, StrVec& node_addrs, int dop);

    /**
     * @brief generate partitions of table
     * @param int32_t fid: id of partitioning field
     * @param size_t num: number of partition
     * @param int dop: degree of parallelism for each partition
     */
    std::vector<Table*> generatePartition(int32_t fid, size_t num, int dop=1);

    /**
     * @brief set this table as partition after distributing data
     */
    void setPartition();

    /**
     * @brief distribute data to nodes
     * @param EvalVec values: values to distribute
     */
    void distribute(const EvalValue& part_val, const EvalVec& values);

    /**
     * @brief compress all DYNAMIC segments of column table
     * @return true if success
     */
    virtual bool compressDelta() {
        std::string err("wrong table type for compression, ");
        RTI_EXCEPTION(err);
        return false;
    }

    /**
     * @brief serialize table to binary
     */
    virtual void serialize(boost::archive::binary_oarchive& oa) const;

    /**
     * @brief deserialize table from binary
     */
    static Table* deserialize(boost::archive::binary_iarchive& ia);

    /**
     * @brief get name of table
     * @return table name
     */
    std::string getTableName() const { return name_; }

    /**
     * @brief set name of table
     * @param std::string name: name of table
     */
    void setTableName(std::string name) { name_ = name; }

    /**
     * @brief get type of table
     * @return table type 
     */
    Type getTableType() const { return type_; }

    /**
     * @brief get id of table
     * @return table id 
     */
    uint32_t getTableId() { return table_id_; }

    /**
     * @brief get id of paged-array in table
     * @param int32_t fid: id of field
     * @return id of paged-array
     */
    virtual uint32_t getPagedArrayId(int32_t fid = 0) const = 0;

    /**
     * @brief get ids of all paged_arrays
     */
    virtual void getPagedArrayIds(std::vector<uint32_t>& pa_ids) const = 0;

    /**
     * @brief get the number of fields in table
     * @return the number of fields
     */
    unsigned getNumFields() const { return field_infos_.size(); }

    /**
     * @brief get a field info
     * @param unsigned fid: id of field
     * @return field info
     */
    FieldInfo* getFieldInfo(unsigned fid) const;

    /**
     * @brief get all field infos
     * @return vector of field info
     */
    FieldInfoVec getFieldInfo() const;

    /**
     * @brief get all field infos
     * @param FieldInfoVec finfos: return vector
     */
    void getFieldInfo(FieldInfoVec& finfos) const;

    /**
     * @brief get field info as string vector
     */
    StrVec getFieldInfoStr() const;

    /**
     * @brief get name of field
     * @param int32_t fid: id of field
     * @return field name
     */
    std::string getFieldName(int32_t fid) const;

    /**
     * @brief get types of all field as string
     * @param StrVec return string vector
     */
    void getFieldAsString(StrVec& fields) const;

    /**
     * @brief get all ids of segments
     * @param SgmntLptrVec sgmt_ids: return ids vector
     */
    virtual void getAllSgmtIds(SgmtLptrVec& sgmt_ids) const = 0;

    /**
     * @brief return the number of records
     */
    virtual size_t getNumRecords() = 0;

    /**
     * @brief check the table is partitioned
     */
    bool isPartitioned() const { return part_nodes_.size() != 0; }

    /**
     * @brief get partition node for input record
     */
    std::string findNode(EvalVec& values);

    /**
     * @breif return if of partition field
     */
    int32_t getPartFid() const { return part_fid_; }

    /**
     * @brief return id of partition to distribute
     */
    int32_t getPartId(EvalValue& val) const { return part_func_(val); }
    
    /**
     * @brief return address of partition nodes
     */
    StrVec getPartNodes() const { return part_nodes_; }

    /**
     * @brief clear partition nodes
     */
    void clearPartNodes() { part_nodes_.clear(); }

    /**
     * @brief translate single std::string value to eval value
     * @param FieldInfo* fi: field info
     * @param std::string value: string to translate
     */
    static EvalValue translateValue(FieldInfo* fi, const std::string& value);

    /**
     * @brief translate std::string values to eval values
     * @details convert string to eval value as referencing field info
     * @param StrVec input values
     * @param EvalVec return translated values
     */
    void translateValues(const StrVec& values, EvalVec& evals);

    /**
     * @brief translate std::string values to eval values
     * @details convert string to eval value as referencing field info
     * @param StrVec input values
     * @param EvalVec return translated values
     */
    static void translateValues(const StrVec& values,
                    EvalVec& evals, FieldInfoVec finfo);

    /**
     * @brief translate eval values to std::string values
     * @details convert eval value to string as referencing field info
     * @param EvalVec input values
     * @param StrVec return translated values
     */
    void translateToStr(const EvalVec& evals, StrVec& values);

    /**
     * @brief translate single std::string value to record
     * @breif FieldInfoVec finfo: field info to parse
     * @param std::string: string to translate
     * @param EvalVec evals: return eval values
     */
    static void parseAndTranslate(const std::string str,
            FieldInfoVec finfo, EvalVec& evals, std::string delim = ",");

    static FieldInfoVec createFieldInfo(
                            std::string tname, const CreateFieldVec& fields);
    static CreateFieldVec createFieldInfo(const FieldInfoVec& fields);

    /**
     * @brief check if all column names exist in table
     * @param StrVec cnames: column names to check
     * @param std::string incorrect: failed column name
     * @return true if all cnames are valid
     */
    bool checkFields(const StrVec& cnames, std::string& incorrect);

    /**
     * @brief set the segments so that they can no longer allocate (for dbg)
     */
    virtual void finalizeCurrentSgmt() = 0;

    /**
     * @brief get memory statistics
     */
    virtual size_t getMemoryUsage(std::ostream& os,
                    bool verbose=false, int level=0) = 0;

  protected:
    DISALLOW_COMPILER_GENERATED(Table);

    /**
     * @brief table constructor
     * @param Type type: type of table
     * @param std::string name: name of table
     * @param FieldInfoVec fields: field infos
     */
    Table(const std::string& name, const FieldInfoVec& finfo);
    Table(std::string name, Type type, uint32_t table_id,
            FieldInfoVec& finfos, IndexVec& indexes,
            int64_t increment_fid, int64_t increment_start,
            int32_t part_fid, StrVec& part_nodes);

    /*
     * member variables
     */
    std::string  name_;
    Type         type_;
    uint32_t     table_id_;

    FieldInfoVec field_infos_;
    IndexVec     indexes_;

    int64_t      increment_fid_;
    std::atomic<int64_t> increment_start_;

    PartEvalFunc part_func_;
    int32_t      part_fid_;
    StrVec       part_nodes_;
};

}  // namespace storage

#endif  // STORAGE_RELATION_TABLE_H_
