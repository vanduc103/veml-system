// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef STORAGE_RELATION_ROW_TABLE_H_
#define STORAGE_RELATION_ROW_TABLE_H_

// Project include
#include "storage/relation/table.h"
#include "storage/base/paged_array.h"
#include "storage/base/paged_array_var.h"

namespace storage {

/**
 * @brief row table data store in RTI
 * @author Ilju Lee, ijlee@kdb.snu.ac.kr
 */
class RowTable : public Table {
  public:
    /**
     * @brief RowTable iterator
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
        void init()     { return itr_.init(); }

        iterator& operator++() {
            ++itr_;
            return *this;
        }
        void next() { ++itr_; }
        void nextPage() { itr_.nextPage(); }
        Page* getPage() { return itr_.getPage(); }

        // access functions
        LogicalPtr getLptr() const { return itr_.getLptr(); }
        PhysicalPtr operator*() const { return itr_.getPptr(); }
        void* getPtr() const { return (void*)itr_.getPptr(); }

        EvalValue getValue(unsigned fid);
        EvalVec getRecord();                 
        EvalVec getRecord(IntVec& fids);
        StrVec getRecordStr();               
        StrVec getRecordStr(IntVec& fids);

      private:
        PagedArray<Tuple_T>::iterator itr_;
    };

    /**
     * @brief RowTable constructor
     * @param std::string name: table name
     */
    RowTable(const std::string& name,
            FieldInfoVec& finfos);

    /**
     * @brief RowTable constructor
     * @details constructor for recovery
     */
    RowTable(std::string name, uint32_t table_id,
            FieldInfoVec& finfos, Table::IndexVec& indexes,
            int64_t increment_fid, int64_t increment_start,
            int32_t part_fid, StrVec& part_nodes,
            uint32_t fixed_slot_id, uint32_t var_slot_id);

    /**
     * @brief RowTable destructor
     */
    virtual ~RowTable() {
        Metadata::removePAInfo(fixed_slot_id_);
        Metadata::removePAInfo(var_slot_id_);
    }

    /**
     * @brief insert a record to any unused slot in table
     * @param EvalVec values: values of each field
     * @return address of insert record
     */
    virtual AddressPair insertRecord(EvalVec& values);

    /**
     * @brief insert a record to any unused slot in table
     * @param StrVec values: values of each field
     * @return address of insert record
     */
    virtual AddressPair insertRecord(StrVec& values);

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
     * @brief update a record
     * @param LogicalPtr idx: index of slot to update
     * @param IntVec fids: ids of field to update
     * @param EvalVec values: new values
     * @return true if success
     */
    bool updateRecord(LogicalPtr idx,
            const IntVec& fids, const EvalVec& values);

    /**
     * @brief insert a record to any unused slot in table (transactional)
     * @param EvalVec values: values of each field
     * @return address of insert record
     */
    virtual AddressPair insertRecord(Transaction& trans, EvalVec& values);

    /**
     * @brief insert a record to designated slot in table (transactional)
     * @param LogicalPtr idx: index of slot to insert
     * @param EvalVec values: values of each field
     * @return address of insert record
     */
    AddressPair insertRecord(Transaction& trans, LogicalPtr idx, EvalVec& values);

    /**
     * @brief delete a record (transactional)
     * @param LogicalPtr idx: index of slot to delete
     * @return true if success
     */
    bool deleteRecord(Transaction& trans, LogicalPtr idx);

    /**
     * @brief update a record (transactional)
     * @param LogicalPtr idx: index of slot to update
     * @param IntVec fids: ids of field to update
     * @param EvalVec values: new values
     * @return true if success
     */
    bool updateRecord(Transaction& trans, LogicalPtr idx,
                const IntVec& fids, const EvalVec& values);

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
     * check a slot is valid
     */
    bool isValid(LogicalPtr idx);

    /**
     * @brief get slot of record
     * @param LogicalPtr idx: index of slot to read
     * @return logical/physical address of slot
     */
    AddressPair getSlot(LogicalPtr idx);

    /**
     * @brief get a value from a record (transactional)
     * @param LogicalPtr idx: index of slot to read
     * @param int fid: id of field to read
     * @return single eval value
     */
    EvalValue getValue(Transaction& trans, LogicalPtr idx, int fid);

    /**
     * @brief get all values from a record (transactional)
     * @param LogicalPtr idx: index of slot to read
     * @return all eval values of a record
     */
    EvalVec getRecord(Transaction& trans, LogicalPtr idx);

    /**
     * @brief get specific values from a record (transactional)
     * @param LogicalPtr idx: index of slot to read
     * @param std:vector<int> fids: ids of field to read
     * @return requested values of a record
     */
    EvalVec getRecord(Transaction& trans, LogicalPtr idx, IntVec& fids);

    /**
     * @brief get slot of record (transactional
     * @param LogicalPtr idx: index of slot to read
     * @return logical/physical address of slot
     */
    AddressPair getSlot(Transaction& trans, LogicalPtr idx);

    /**
     * @brief return data as tensor data type
     */
    //torch::Tensor toTensor(std::string type);

    /**
     * @brief get byte size of record
     */
    uint16_t getSlotWidth() { return slot_width_; }
    
    /**
     * @brief get id of fixed-slot paged-array
     */
    uint32_t getPagedArrayId(int32_t fid = 0) const { return fixed_slot_id_; }

    /**
     * @brief get ids of all paged_arrays
     */
    void getPagedArrayIds(std::vector<uint32_t>& pa_ids) const;

    /**
     * @brief get ids of all segments
     */
    void getAllSgmtIds(SgmtLptrVec& sgmt_ids) const;

    /**
     * @brief get the current number of valid records
     * @return the number of valid records
     */
    size_t getNumRecords();

    /**
     * @brief create an iterator
     */
    iterator begin();

    /**
     * @brief create an iterator
     */
    iterator begin(SgmtLptrVec& sgmt_ids);

    /**
     * @brief set the segments so that they can no longer allocate (for dbg)
     */
    void finalizeCurrentSgmt();

    /**
     * @brief get byte size of memory consumption
     */
    size_t getMemoryUsage(std::ostream& os, bool verbose=false, int level=0);

    /**
     * @brief serialize table to binary
     */
    void serialize(boost::archive::binary_oarchive& oa) const;

  protected:
    DISALLOW_COMPILER_GENERATED(RowTable);

    /*
     * private util function
     */
    void setValueOnSlot(PhysicalPtr slot, const FieldInfo* fi, const EvalValue& val);
    static uint16_t getSlotWidth(const FieldInfoVec& fields);

    /*
     * member variables
     */
    PagedArray<Tuple_T>     fixed_slot_;
    PagedArrayVar           var_slot_;

    uint32_t                fixed_slot_id_;
    uint32_t                var_slot_id_;

    uint16_t                slot_width_;
};

inline EvalValue RowTable::iterator::getValue(uint32_t fid) {
    PhysicalPtr slot = itr_.getPptr();
    return PABase::getValueFromSlot(slot, finfos_[fid]);
}

inline EvalVec RowTable::iterator::getRecord() {
    PhysicalPtr slot = itr_.getPptr();

    EvalVec values;
    for (auto fi : finfos_) {
        values.push_back(
                PABase::getValueFromSlot(slot, fi));
    }
    return values;
}

inline EvalVec RowTable::iterator::getRecord(IntVec& fids) {
    PhysicalPtr slot = itr_.getPptr();

    EvalVec values;
    for (auto i : fids) {
        values.push_back(
                PABase::getValueFromSlot(slot, finfos_[i]));
    }

    return values;
}

inline StrVec RowTable::iterator::getRecordStr() {
    PhysicalPtr slot = itr_.getPptr();

    StrVec values;
    for (auto fi : finfos_) {
        values.push_back(
                PABase::getValueFromSlot(slot, fi).to_string());
    }
    return values;
}

inline StrVec RowTable::iterator::getRecordStr(IntVec& fids) {
    PhysicalPtr slot = itr_.getPptr();

    StrVec values;
    for (auto i : fids) {
        values.push_back(
                PABase::getValueFromSlot(slot,
                    finfos_[i]).to_string());
    }
    return values;
}

inline void RowTable::setValueOnSlot(PhysicalPtr slot,
        const FieldInfo* fi, const EvalValue& val) {
    ValueType type = fi->getValueType();
    EvalType etype = convertToEvalType(type);
    if(!((val.getEvalType()==etype)
            || (val.getEvalType()==ET_INTVEC && etype==ET_INTLIST)
            || (val.getEvalType()==ET_LONGVEC && etype==ET_LONGLIST)
            || (val.getEvalType()==ET_FLOATVEC && etype==ET_FLOATLIST) 
            || (val.getEvalType()==ET_DOUBLEVEC && etype==ET_DOUBLELIST))) {
        std::string err("value type error @RowTable::setValueOnSlot, ");
        RTI_EXCEPTION(err);
    }

    uint32_t offset = fi->getOffset();
    PhysicalPtr slot_offset = slot + offset;

    switch (type) {
    case VT_BIGINT:
        new (slot_offset) BigInt_T(val.getBigIntConst());
        break;

    case VT_INT:
        new (slot_offset) Integer_T(val.getBigIntConst());
        break;

    case VT_DOUBLE:
        new (slot_offset) Double_T(val.getDoubleConst());
        break;

    case VT_FLOAT:
        new (slot_offset) Float_T(val.getDoubleConst());
        break;

    case VT_DECIMAL:
        new (slot_offset) Decimal_T(val.getDecimalConst());
        break;

    case VT_VARCHAR:
    {
        if (val.getStringConst().isEmbedded()) {
            new (slot_offset) Varchar_T(val, val.getStringConst().size());
        } else {
            AddressPair vslot
                = var_slot_.allocateVarSlot(
                        val.getStringConst().size());
            new (slot_offset) Varchar_T(val, var_slot_.getId(), vslot);
        }
        break;
    }
    case VT_CHAR:
        new (slot_offset) Char_T(val.getStringConst(), fi->getSize());
        break;

    case VT_BOOL:
        new (slot_offset) Bool_T(val.getBoolConst());
        break;

    case VT_DATE:
        new (slot_offset) Date_T(val.getDateConst());
        break;

    case VT_TIME:
        new (slot_offset) Time_T(val.getTimeConst());
        break;

    case VT_TIMESTAMP:
        new (slot_offset) Timestamp_T(val.getTSConst());
        break;

    case VT_INTLIST:
    {
        if (val.getEvalType() == ET_INTLIST) {
            new (slot_offset) IntList_T(IntList(val, var_slot_id_));
        } else if (val.getEvalType() == ET_INTVEC) {
            IntList tmp_list;
            new (slot_offset) IntList_T(IntList(tmp_list, var_slot_id_));
            IntList& list = *reinterpret_cast<IntList*>(slot_offset);
            const IntVec& vec = val.getIntVecConst();
            list.insert(vec);
        }
        break;
    }

    case VT_LONGLIST:
    {
        if (val.getEvalType() == ET_LONGLIST) {
            new (slot_offset) LongList_T(LongList(val, var_slot_id_));
        } else if (val.getEvalType() == ET_LONGVEC) {
            LongList tmp_list;
            new (slot_offset) LongList_T(LongList(tmp_list, var_slot_id_));
            LongList& list = *reinterpret_cast<LongList*>(slot_offset);
            const LongVec& vec = val.getLongVecConst();
            list.insert(vec);
        }
        break;
    }

    case VT_FLOATLIST:
    {
        if (val.getEvalType() == ET_FLOATLIST) {
            new (slot_offset) FloatList_T(FloatList(val, var_slot_id_));
        } else if (val.getEvalType() == ET_FLOATVEC) {
            FloatList tmp_list;
            new (slot_offset) FloatList_T(FloatList(tmp_list, var_slot_id_));
            FloatList& list = *reinterpret_cast<FloatList*>(slot_offset);
            const FloatVec& vec = val.getFloatVecConst();
            list.insert(vec);
        }


        break;
    }

    case VT_DOUBLELIST:
    {
        if (val.getEvalType() == ET_DOUBLELIST) {
            new (slot_offset) DoubleList_T(DoubleList(val, var_slot_id_));
        } else if (val.getEvalType() == ET_DOUBLEVEC) {
            DoubleList tmp_list;
            new (slot_offset) DoubleList_T(DoubleList(tmp_list, var_slot_id_));
            DoubleList& list = *reinterpret_cast<DoubleList*>(slot_offset);
            const DoubleVec& vec = val.getDoubleVecConst();
            list.insert(vec);
        }


        break;
    }

    default:
        std::string err("invalid value type @RowTable::getValue, ");
        RTI_EXCEPTION(err);
        break;
    }
}

} // namespace storage

#endif  // STORAGE_ROW_TABLE_H_
