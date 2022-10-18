// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

// Related include
#include "storage/relation/row_table.h"

// Project include
#include "storage/util/btree_index.h"
#include "concurrency/transaction.h"
#include "persistency/archive_manager.h"

// C & C++ system include
#include <fstream>

namespace storage {

RowTable::iterator::iterator(Table* table)
    : Table::iterator_wrapper(table) {
    itr_ = reinterpret_cast<RowTable*>(table)->fixed_slot_.begin();
}

RowTable::iterator::iterator(Table* table, SgmtLptrVec& sgmt_ids)
    : Table::iterator_wrapper(table) {
    itr_ = reinterpret_cast<RowTable*>(table)->fixed_slot_.begin(sgmt_ids);
}

RowTable::iterator::iterator(const iterator& other)
        : Table::iterator_wrapper(other.finfos_) {
    this->itr_ = other.itr_;
}

RowTable::iterator& RowTable::iterator::operator=(const iterator& other) {
    this->finfos_ = other.finfos_;
    this->itr_ = other.itr_;

    return *this;
}

/*
 * constructor/destructor
 */
RowTable::RowTable(const std::string& name, FieldInfoVec& finfos)
        : Table(name, finfos),
        fixed_slot_(false, getSlotWidth(finfos)),
        slot_width_(getSlotWidth(finfos)) {
    fixed_slot_id_ = fixed_slot_.getId();
    var_slot_id_ = var_slot_.getId();
}

RowTable::RowTable(std::string name, uint32_t table_id,
        FieldInfoVec& finfos, Table::IndexVec& indexes,
        int64_t increment_fid, int64_t increment_start,
        int32_t part_fid, StrVec& part_nodes,
        uint32_t fixed_slot_id, uint32_t var_slot_id)
    : Table(name, Table::ROW, table_id, finfos, indexes,
            increment_fid, increment_start, part_fid, part_nodes),
    fixed_slot_(fixed_slot_id, false, getSlotWidth(finfos)),
    var_slot_(var_slot_id), slot_width_(getSlotWidth(finfos)) {
    fixed_slot_id_ = fixed_slot_.getId();
    var_slot_id_ = var_slot_.getId();
}

/*
 * dml interface
 */
AddressPair RowTable::insertRecord(EvalVec& values) {
    if (prepareInsert(values)) {
        return AddressPair();
    }

    // prepare slot image
    char* buf = new char[slot_width_];
    for (unsigned i = 0; i < field_infos_.size(); ++i) {
        const EvalValue& val = values[i];
        FieldInfo* fi = field_infos_[i];

        this->setValueOnSlot(buf, fi, val);
    }

    // insert tuple
    AddressPair slot = fixed_slot_.insertSlot(buf);

    // insert to index
    if (!insertToIndex(values, slot.lptr_)) {
        assert(fixed_slot_.erase(slot.lptr_));
        delete[] buf;
        return AddressPair();
    }

    delete[] buf;
    return slot;
}

AddressPair RowTable::insertRecord(StrVec& strvalues) {
    // translate std::string to EvalValue
    EvalVec values;
    translateValues(strvalues, values);

    return insertRecord(values);
}

AddressPair RowTable::insertRecord(LogicalPtr idx, EvalVec& values) {
    if (prepareInsert(values)) {
        return AddressPair();
    }

    // prepare slot image
    char* buf = new char[slot_width_];
    for (unsigned i = 0; i < field_infos_.size(); ++i) {
        const EvalValue& val = values[i];
        FieldInfo* fi = field_infos_[i];

        this->setValueOnSlot(buf, fi, val);
    }

    // insert tuple
    AddressPair slot = fixed_slot_.insertSlot(buf, idx);

    // insert to index
    if (!insertToIndex(values, slot.lptr_)) {
        fixed_slot_.erase(slot.lptr_);
        delete[] buf;
        return AddressPair();
    }

    delete[] buf;
    return slot;
}

bool RowTable::deleteRecord(LogicalPtr idx) {
    AddressPair slot = fixed_slot_.getSlot(idx);
    if (slot.pptr_ == nullptr)
        return false;

    for (auto fi : field_infos_) {
        if (fi->getValueType() == VT_VARCHAR) {
            Varchar_T* varchar = reinterpret_cast<Varchar_T*>(
                                    slot.pptr_ + fi->getOffset());
            varchar->release(&var_slot_);
        }

        if (fi->getValueType() == VT_INTLIST) {
            IntList_T* list = reinterpret_cast<IntList_T*>(
                                    slot.pptr_ + fi->getOffset());
            list->getRefValue().release();
        }

        if (fi->getValueType() == VT_LONGLIST) {
            LongList_T* list = reinterpret_cast<LongList_T*>(
                                    slot.pptr_ + fi->getOffset());
            list->getRefValue().release();
        }

        if (fi->getValueType() == VT_FLOATLIST) {
            FloatList_T* list = reinterpret_cast<FloatList_T*>(
                                    slot.pptr_ + fi->getOffset());
            list->getRefValue().release();
        }

        if (fi->getValueType() == VT_DOUBLELIST) {
            DoubleList_T* list = reinterpret_cast<DoubleList_T*>(
                                    slot.pptr_ + fi->getOffset());
            list->getRefValue().release();
        }
    }

    // remove from index
    bool success = removeFromIndex(idx);

    // remove from slot
    if (success) success &= fixed_slot_.erase(idx);

    return success;
}

void RowTable::truncate() {
    fixed_slot_.truncate();
    var_slot_.truncate();

    // release indexes and create again
    for (auto& index : indexes_) {
        // release old index
        index.index_->release();
        delete index.index_;

        // create new index
        std::string index_name(name_
                        + "_idx_" + std::to_string(index.fid_));
        BTreeIndex* new_index
                        = new BTreeIndex(name_, index_name, index.unique_);
        index.index_ = new_index;
    }
}

bool RowTable::updateRecord(LogicalPtr idx,
        const IntVec& fids, const EvalVec& values) {
    AddressPair slot = fixed_slot_.getSlot(idx);
    if (slot.pptr_ == nullptr)
        return false;

    for (unsigned i = 0; i < fids.size(); i++) {
        int idx = fids[i];
        FieldInfo* fi = field_infos_[idx];

        this->setValueOnSlot(slot.pptr_, fi, values[i]);
    }

    return true;
}

AddressPair RowTable::insertRecord(Transaction& trans, EvalVec& values) {
    // handle auto increment field
    if (increment_fid_ >= 0) {
        int64_t id = increment_start_.fetch_add(1);
        values.insert(values.begin() + increment_fid_, BigInt(id));
    }
    
    if (values.size() != field_infos_.size()) {
        std::string err("wrong number of values @RowTable::insert, ");
        RTI_EXCEPTION(err);
    }

    char* buf = new char[slot_width_];
    for (unsigned i = 0; i < field_infos_.size(); ++i) {
        const EvalValue& val = values[i];
        FieldInfo* fi = field_infos_[i];

        this->setValueOnSlot(buf, fi, val);
    }

    // insert tuple
    AddressPair slot = fixed_slot_.insertSlot(trans, buf);

    // insert to index
    insertToIndex(values, slot.lptr_);

    delete[] buf;
    return slot;
}

AddressPair RowTable::insertRecord(Transaction& trans, LogicalPtr idx, EvalVec& values) {
    // handle auto increment field
    if (increment_fid_ >= 0) {
        int64_t id = increment_start_.fetch_add(1);
        values.insert(values.begin() + increment_fid_, BigInt(id));
    }
    
    // check the number of fields
    if (values.size() != field_infos_.size()) {
        std::string err("wrong number of values @RowTable::insert, ");
        RTI_EXCEPTION(err);
    }

    char* buf = new char[slot_width_];
    for (unsigned i = 0; i < field_infos_.size(); ++i) {
        const EvalValue& val = values[i];
        FieldInfo* fi = field_infos_[i];

        this->setValueOnSlot(buf, fi, val);
    }

    // insert tuple
    AddressPair slot = fixed_slot_.insertSlot(trans, buf, idx);

    // insert to index
    insertToIndex(values, slot.lptr_);

    delete[] buf;
    return slot;
}

bool RowTable::deleteRecord(Transaction& trans, LogicalPtr idx) {
    AddressPair slot = fixed_slot_.getSlot(idx);
    if (slot.pptr_ == nullptr)
        return false;

    // TODO: release var slot

    // remove from index
    bool success = removeFromIndex(idx);

    // remove from slot
    if (success) success &= fixed_slot_.erase(trans, idx);

    return success;
}

bool RowTable::updateRecord(Transaction& trans, LogicalPtr idx,
        const IntVec& field_ids, const EvalVec& values) {
    AddressPair slot = fixed_slot_.getSlot(idx);
    if (slot.pptr_ == nullptr) return false;

    char* buf = new char[slot_width_];
    memcpy(buf, slot.pptr_, slot_width_);
    for (unsigned i = 0; i < field_ids.size(); ++i) {
        const EvalValue& val = values[i];
        FieldInfo* fi = field_infos_[field_ids[i]];

        this->setValueOnSlot(buf, fi, val);
    }

    EvalValue value(buf);
    fixed_slot_.update(trans, idx, value);
    //delete[] buf;
    return true;
}

/*
 * search interface
 */
EvalValue RowTable::getValue(LogicalPtr idx, int fid) {
    PhysicalPtr slot = fixed_slot_.getSlot(idx).pptr_;
    if (slot == nullptr) {
        std::string err("wrong index @RowTable::getValue, ");
        RTI_EXCEPTION(err);
    }

    return PABase::getValueFromSlot(slot, field_infos_[fid]);
}

EvalVec RowTable::getRecord(LogicalPtr idx) {
    PhysicalPtr slot = fixed_slot_.getSlot(idx).pptr_;
    if (slot == nullptr) {
        std::string err("wrong index @RowTable::getValue, ");
        RTI_EXCEPTION(err);
    }
    EvalVec values;
    for (auto fi : field_infos_) {
        values.push_back(PABase::getValueFromSlot(slot, fi));
    }
    return values;
}

EvalVec RowTable::getRecord(LogicalPtr idx, IntVec& fids) {
    PhysicalPtr slot = fixed_slot_.getSlot(idx).pptr_;
    if (slot == nullptr) {
        std::string err("wrong index @RowTable::getValue, ");
        RTI_EXCEPTION(err);
    }
    EvalVec values;
    for (auto fid : fids) {
        values.push_back(PABase::getValueFromSlot(slot, field_infos_[fid]));
    }

    return values;
}

StrVec RowTable::getRecordStr(LogicalPtr idx) {
    PhysicalPtr slot = fixed_slot_.getSlot(idx).pptr_;
    if (slot == nullptr) {
        std::string err("wrong index @RowTable::getRecordStr, ");
        RTI_EXCEPTION(err);
    }

    StrVec values;
    for (auto fi : field_infos_) {
        values.push_back(PABase::getValueFromSlot(slot, fi).to_string());
    }

    return values;
}

StrVec RowTable::getRecordStr(LogicalPtr idx, IntVec& fids) {
    PhysicalPtr slot = fixed_slot_.getSlot(idx).pptr_;
    if (slot == nullptr) {
        std::string err("wrong index @RowTable::getValue, ");
        RTI_EXCEPTION(err);
    }

    StrVec values;
    for (auto fid : fids) {
        values.push_back(
                PABase::getValueFromSlot(slot, field_infos_[fid]).to_string());
    }

    return values;
}

bool RowTable::isValid(LogicalPtr idx) {
    return fixed_slot_.isValid(idx);
}

AddressPair RowTable::getSlot(LogicalPtr idx) {
    return fixed_slot_.getSlot(idx);
}

EvalValue RowTable::getValue(Transaction& trans, LogicalPtr idx, int fid) {
    PhysicalPtr slot = fixed_slot_.getSlot(trans, idx).pptr_;
    if (slot == nullptr) {
        std::string err("wrong index @RowTable::getValue, ");
        err += getTableName();
        err += ", ";
        err += std::to_string(idx);
        err += '\n';
        RTI_EXCEPTION(err);
    }

    return PABase::getValueFromSlot(slot, field_infos_[fid]);
}

EvalVec RowTable::getRecord(Transaction& trans, LogicalPtr idx) {
    PhysicalPtr slot = fixed_slot_.getSlot(trans, idx).pptr_;
    if (slot == nullptr) {
        std::string err("wrong index @RowTable::getValue, ");
        RTI_EXCEPTION(err);
    }

    EvalVec values;
    for (auto fi : field_infos_) {
        values.push_back(PABase::getValueFromSlot(slot, fi));
    }

    return values;
}

EvalVec RowTable::getRecord(Transaction& trans, LogicalPtr idx, IntVec& fids) {
    PhysicalPtr slot = fixed_slot_.getSlot(trans, idx).pptr_;
    if (slot == nullptr) {
        std::string err("wrong index @RowTable::getValue, ");
        RTI_EXCEPTION(err);
    }

    EvalVec values;
    for (auto fid : fids) {
        values.push_back(PABase::getValueFromSlot(slot, field_infos_[fid]));
    }

    return values;
}

/**torch::Tensor RowTable::toTensor(std::string type) {
    torch::TensorOptions options;
    if (type == "int") {
        options = torch::TensorOptions().dtype(torch::kInt32);
    } else if (type == "long") {
        options = torch::TensorOptions().dtype(torch::kInt64);
    } else if (type == "float") {
        options = torch::TensorOptions().dtype(torch::kFloat32);
    } else if (type == "double") {
        options = torch::TensorOptions().dtype(torch::kFloat64);
    } else {
        std::string err("invalid value type @RowTable::toTensor, ");
        RTI_EXCEPTION(err);
    }

    auto itr = this->begin();
    uint32_t num_fields = this->getNumFields();

    std::vector<torch::Tensor> tensors;
    for ( ; itr; itr.nextPage()) {
        uint32_t num_records = itr.getPage()->getNumSlots();
        void* ptr = reinterpret_cast<void*>(*itr);
        torch::Tensor t = torch::from_blob(ptr,
                            {num_records, num_fields}, options);
        tensors.push_back(t);
    }
    return at::cat(at::TensorList(tensors), 0);
}*/

AddressPair RowTable::getSlot(Transaction& trans, LogicalPtr idx) {
    return fixed_slot_.getSlot(trans, idx);
}

void RowTable::getPagedArrayIds(std::vector<uint32_t>& pa_ids) const {
    pa_ids.push_back(fixed_slot_.getId());
    pa_ids.push_back(var_slot_.getId());
}

void RowTable::getAllSgmtIds(SgmtLptrVec& sgmt_ids) const {
    sgmt_ids = fixed_slot_.getAllSgmtIds();
}

size_t RowTable::getNumRecords() {
    return fixed_slot_.getNumElement();
}

/*
 * begin iterator
 */
RowTable::iterator RowTable::begin() {
    return iterator(this);
}

RowTable::iterator RowTable::begin(SgmtLptrVec& sgmt_ids) {
    return iterator(this, sgmt_ids);
}

/*
 * for dbg/test
 */
void RowTable::finalizeCurrentSgmt() {
    fixed_slot_.finalizeCurrentSgmt();
}

size_t RowTable::getMemoryUsage(std::ostream& os, bool verbose, int level) {
    size_t fixed_usage = fixed_slot_.getMemoryUsage();
    size_t var_usage = var_slot_.getMemoryUsage();

    std::vector<std::pair<int, size_t> > index_usage;
    size_t index_total = 0;
    for (auto index : indexes_) {
        size_t index_memory = index.index_->getMemoryUsage();
        index_total += index_memory;
        index_usage.emplace_back(index.fid_, index_memory);
    }

    size_t total = sizeof(Table) + fixed_usage + var_usage + index_total;

    if (verbose) {
        std::string indent = "";
        for (int i = 0; i < level; i++) indent += '\t';

        os << indent << "Table Name: " << name_ << " => "
            << memory_unit_str(total) << '\n';

        os << indent << '\t' << "Fixed Slot: "
            << memory_unit_str(fixed_usage) << '\n';
        os << indent << '\t' << "Var Slot: "
            << memory_unit_str(var_usage) << '\n';

        for (auto itr : index_usage) {
            os << indent << '\t' << "Index(" << itr.first << "): "
                << memory_unit_str(itr.second) << '\n';
        }
    }

    return total;
}

void RowTable::serialize(boost::archive::binary_oarchive& oa) const {
    Table::serialize(oa);
    oa << fixed_slot_id_ << var_slot_id_;
}

uint16_t RowTable::getSlotWidth(const FieldInfoVec& fields) {
    FieldInfo* last_fi = fields.back();
    uint16_t slot_width = last_fi->getOffset() + last_fi->getSize();
    if (slot_width < 8)
        slot_width = 8;
    return slot_width;
}

}   // namespace storage
