// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

// Related include
#include "storage/relation/column_table.h"

// Project include
#include "storage/util/btree_index.h"
#include "persistency/archive_manager.h"

// C & C++ system include
#include <fstream>

// Other include
#include <boost/serialization/vector.hpp>

namespace storage {

ColumnTable::iterator::iterator(Table* table)
        : Table::iterator_wrapper(table),
        itr_(reinterpret_cast<ColumnTable*>(table)->valid_.begin()) {
    ColumnTable* col_table = reinterpret_cast<ColumnTable*>(table);

    for (auto& col : col_table->columns_) {
        cols_.push_back(col);
    }
}

ColumnTable::iterator::iterator(Table* table, SgmtLptrVec& sgmt_ids)
        : Table::iterator_wrapper(table),
        itr_(reinterpret_cast<ColumnTable*>(table)->valid_.begin(sgmt_ids)) {
    ColumnTable* col_table = reinterpret_cast<ColumnTable*>(table);

    for (auto& col : col_table->columns_) {
        cols_.push_back(col);
    }
}

ColumnTable::iterator::iterator(const iterator& other)
        : Table::iterator_wrapper(other.finfos_),
        itr_(other.itr_) {
    this->cols_ = other.cols_;
}

ColumnTable::iterator&
ColumnTable::iterator::operator=(const iterator& other) {
    this->finfos_ = other.finfos_;
    this->itr_ = other.itr_;
    this->cols_ = other.cols_;

    return *this;
}

void ColumnTable::iterator::next() {
    while (itr_) {
        itr_.next();
        if (itr_.isValid()) break;
    }

    if (!itr_) return;
}

void* ColumnTable::iterator::getPtr() const {
    std::string err("invalide operation @ColumnTable::iterator::getPtr, ");
    RTI_EXCEPTION(err);

    return nullptr;
}

EvalValue ColumnTable::iterator::getValue(uint32_t fid) {
    return cols_[fid]->getValue(itr_.getLptr());
}

EvalVec ColumnTable::iterator::getRecord() {
    EvalVec ret;
    ret.reserve(cols_.size());
    LogicalPtr lptr = itr_.getLptr();
    for (auto& col : cols_) {
        ret.push_back(col->getValue(lptr));
    }
    return ret;
}

EvalVec ColumnTable::iterator::getRecord(IntVec& fids) {
    EvalVec ret;
    ret.reserve(fids.size());
    LogicalPtr lptr = itr_.getLptr();
    for (auto fid : fids) {
        ret.push_back(cols_[fid]->getValue(lptr));
    }
    return ret;
}

StrVec ColumnTable::iterator::getRecordStr() {
    StrVec ret;
    ret.reserve(cols_.size());
    LogicalPtr lptr = itr_.getLptr();
    for (auto& col : cols_) {
        ret.push_back(col->getValue(lptr).to_string());
    }
    return ret;
}

StrVec ColumnTable::iterator::getRecordStr(IntVec& fids) {
    StrVec ret;
    ret.reserve(fids.size());
    LogicalPtr lptr = itr_.getLptr();
    for (auto fid : fids) {
        ret.push_back(cols_[fid]->getValue(lptr).to_string());
    }
    return ret;
}

/**torch::Tensor ColumnTable::toTensor(std::string type) {
    std::string err("invalide operation @ColumnTable::toTensor, ");
    RTI_EXCEPTION(err);

    torch::Tensor t;
    return t;
}*/

/*
 * dml interface
 */
AddressPair ColumnTable::insertRecord(EvalVec& values) {
    if (prepareInsert(values)) {
        return AddressPair();
    }

    LogicalPtr idx = valid_.assignLptr();
    return insert(idx, values);
}

AddressPair ColumnTable::insertRecord(StrVec& strvalues) {
    // translate std::string to EvalValue
    EvalVec values;
    translateValues(strvalues, values);

    return insertRecord(values);
}

AddressPair ColumnTable::insertRecord(LogicalPtr idx, EvalVec& values) {
    if (prepareInsert(values)) {
        return AddressPair();
    }

    if (valid_.isValid(idx)) {
        std::string err("invalid index @ColumnTable::insertRecord, ");
        RTI_EXCEPTION(err);
    }

    return insert(idx, values);
}

bool ColumnTable::deleteRecord(LogicalPtr idx) {
    if (!valid_.isValid(idx)) {
        std::string err("invalid index @ColumnTable::deleteRecord, ");
        RTI_EXCEPTION(err);
    }

    // remove from index
    bool success = removeFromIndex(idx);
    if (!success) return false;
    
    valid_.setInvalid(idx);
    return true;
}

void ColumnTable::truncate() {
    for (unsigned i = 0; i < columns_.size(); i++) {
        ValueType vt = field_infos_[i]->getValueType();
        VALUE_TYPE_EXECUTION(vt,
            PagedArrayEncoded<TYPE>* column\
                = reinterpret_cast<PagedArrayEncoded<TYPE>*>(columns_[i]);\
            column->truncate();
        )
    }
    valid_.truncate();

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

bool ColumnTable::updateRecord(LogicalPtr idx,
        const IntVec& fids, const EvalVec& values) {
    std::string err("Not supporting Error: ColumnTable::updateRecord, ");
    RTI_EXCEPTION(err);

    return false;
}

AddressPair ColumnTable::insertRecord(Transaction& trans, EvalVec& values) {
    std::string err("Not supporting Error: ColumnTable::insertRecord, ");
    RTI_EXCEPTION(err);

    return AddressPair();
}

AddressPair ColumnTable::insertRecord(Transaction& trans,
                LogicalPtr idx, EvalVec& values) {
    std::string err("Not supporting Error: ColumnTable::insertRecord, ");
    RTI_EXCEPTION(err);

    return AddressPair();
}

bool ColumnTable::deleteRecord(Transaction& trans, LogicalPtr idx) {
    std::string err("Not supporting Error: ColumnTable::deleteRecord, ");
    RTI_EXCEPTION(err);

    return false;
}

bool ColumnTable::updateRecord(Transaction& trans, LogicalPtr idx,
        const IntVec& fids, const EvalVec& values) {
    std::string err("Not supporting Error: ColumnTable::updateRecord, ");
    RTI_EXCEPTION(err);

    return false;
}

/*
 * search interface
 */
EvalValue ColumnTable::getValue(LogicalPtr idx, int fid) {
    if (!valid_.isValid(idx)) {
        std::string err("invalid index @ColumnTable::getValue, ");
        RTI_EXCEPTION(err);
    }

    ValueType vt = field_infos_[fid]->getValueType();
    VALUE_TYPE_EXECUTION(vt,
        PagedArrayEncoded<TYPE>* column\
            = reinterpret_cast<PagedArrayEncoded<TYPE>*>(columns_[fid]);\
        return column->getValue(idx);
    )

    return EvalValue();
}

EvalVec ColumnTable::getRecord(LogicalPtr idx) {
    EvalVec ret;
    ret.reserve(columns_.size());

    for (unsigned i = 0; i < columns_.size(); i++) {
        ValueType vt = field_infos_[i]->getValueType();
        VALUE_TYPE_EXECUTION(vt,
            PagedArrayEncoded<TYPE>* column\
                = reinterpret_cast<PagedArrayEncoded<TYPE>*>(columns_[i]);\
            ret.push_back(column->getValue(idx));
        )
    }

    return ret;
}

EvalVec ColumnTable::getRecord(LogicalPtr idx, IntVec& fids) {
    EvalVec ret;
    ret.reserve(fids.size());

    for (auto f : fids) {
        ValueType vt = field_infos_[f]->getValueType();
        VALUE_TYPE_EXECUTION(vt,
            PagedArrayEncoded<TYPE>* column\
                = reinterpret_cast<PagedArrayEncoded<TYPE>*>(columns_[f]);\
            ret.push_back(column->getValue(idx));
        )
    }

    return ret;
}

StrVec ColumnTable::getRecordStr(LogicalPtr idx) {
    StrVec ret;
    ret.reserve(columns_.size());

    for (unsigned i = 0; i < columns_.size(); i++) {
        ValueType vt = field_infos_[i]->getValueType();
        VALUE_TYPE_EXECUTION(vt,
            PagedArrayEncoded<TYPE>* column\
                = reinterpret_cast<PagedArrayEncoded<TYPE>*>(columns_[i]);\
            ret.push_back(column->getValue(idx).to_string());
        )
    }

    return ret;
}

StrVec ColumnTable::getRecordStr(LogicalPtr idx, IntVec& fids) {
    StrVec ret;
    ret.reserve(fids.size());

    for (auto f : fids) {
        ValueType vt = field_infos_[f]->getValueType();
        VALUE_TYPE_EXECUTION(vt,
            PagedArrayEncoded<TYPE>* column\
                = reinterpret_cast<PagedArrayEncoded<TYPE>*>(columns_[f]);\
            ret.push_back(column->getValue(idx).to_string());
        )
    }

    return ret;
}

bool ColumnTable::isValid(LogicalPtr idx) {
    return valid_.isValid(idx);
}

EvalValue ColumnTable::getValue(Transaction& trans, LogicalPtr idx, int fid) {
    std::string err("Not supporting Error: ColumnTable::getValue, ");
    RTI_EXCEPTION(err);

    return EvalValue();
}

EvalVec ColumnTable::getRecord(Transaction& trans, LogicalPtr idx) {
    std::string err("Not supporting Error: ColumnTable::Record, ");
    RTI_EXCEPTION(err);

    return EvalVec();
}

EvalVec ColumnTable::getRecord(Transaction& trans,
            LogicalPtr idx, IntVec& fids) {
    std::string err("Not supporting Error: ColumnTable::Record, ");
    RTI_EXCEPTION(err);

    return EvalVec();
}

/*
 * delta management
 */
bool ColumnTable::compressDelta() {
    bool success = true;
    uint32_t i = 0;
    uint32_t num_sgmts = columns_[0]->getNumSgmts();

    while (success && i < num_sgmts) {
        success = this->compressDelta(i++);
    }

    if (!success) {
        std::string err("error occurred during compressing delta, ");
        RTI_EXCEPTION(err);
    }

    return success;
}

bool ColumnTable::compressDelta(SegmentLptr sgmt_id) {
    bool success = true;
    uint32_t i = 0;
    uint32_t num_fields = field_infos_.size();

    while (success && i < num_fields) {
        VALUE_TYPE_EXECUTION(field_infos_[i]->getValueType(),
            PagedArrayEncoded<TYPE>* column\
                = reinterpret_cast<PagedArrayEncoded<TYPE>*>(columns_[i]);\
            success &= column->compressDelta(sgmt_id))
        i++;
    }

    return success;
}

/*
 * getter
 */
uint32_t ColumnTable::getPagedArrayId(int32_t fid) const {
    return columns_[fid]->getId();
}

void ColumnTable::getPagedArrayIds(std::vector<uint32_t>& pa_ids) const {
    pa_ids.push_back(valid_.getId());
    for (auto c : columns_) {
        pa_ids.push_back(c->getId());
    }
}

void ColumnTable::getAllSgmtIds(SgmtLptrVec& sgmt_ids) const {
    sgmt_ids = columns_[0]->getAllSgmtIds();
}

size_t ColumnTable::getNumRecords() {
    return valid_.getNumElement();
}

/*
 * for dbg/test
 */
void ColumnTable::finalizeCurrentSgmt() {
    valid_.finalizeCurrentSgmt();
}

ColumnTable::iterator ColumnTable::begin() {
    return iterator(this);
}

ColumnTable::iterator ColumnTable::begin(SgmtLptrVec& sgmt_ids) {
    return iterator(this, sgmt_ids);
}

size_t ColumnTable::getMemoryUsage(std::ostream& os, bool verbose, int level) {
    size_t valid_usage = valid_.getMemoryUsage();

    std::vector<std::pair<unsigned, size_t> > usage;
    size_t col_total = 0;
    for (unsigned i = 0; i < columns_.size(); i++) {
        size_t col_usage = columns_[i]->getMemoryUsage();
        col_total += col_usage;
        usage.emplace_back(i, col_usage);
    }

    size_t total = sizeof(Table) + valid_usage + col_total;

    if (verbose) {
        std::string indent = "";
        for (int i = 0; i < level; i++) indent += '\t';

        os << indent << "Table Name: " << name_ << " => "
            << memory_unit_str(total) << '\n';

        for (auto itr : usage) {
            os << indent << '\t' << "Column(" << itr.first << "): "
                << memory_unit_str(itr.second) << '\n';
        }
    }

    return total;
}

bool partition(int32_t fid, StrVec& node_addrs) { return true; }
void distribute(const EvalValue& part_val, const EvalVec& values) {}
void distribute(const EvalValue& part_val, const StrVec& values) {}

ColumnTable::ColumnTable(const std::string& name, FieldInfoVec& finfos)
        : Table(name, finfos) {
    for (unsigned i = 0; i < finfos.size(); i++) {
        VALUE_TYPE_EXECUTION(finfos[i]->getValueType(),
            PagedArrayEncoded<TYPE>* column = new PagedArrayEncoded<TYPE>();\
            columns_.push_back(column);)
    }
}

ColumnTable::ColumnTable(Table::Type type,
        const std::string& name, const FieldInfoVec& fields,
        std::vector<uint32_t> pa_ids, uint32_t valid_id)
    : Table(name, fields), valid_(valid_id) {
        /*
    // deserialize valid
    {
        std::string archive_name
            = persistency::ArchiveManager::createArchiveName(valid_id);
        std::ifstream ifs(archive_name);
        boost::archive::binary_iarchive ia(ifs);
        //valid_.deserialize(ia);
    }

    // deserialize columns
    {
        if (fields.size() != pa_ids.size()) {
            std::string err("wrong number of columns for ColumnTable::deserialize");
            RTI_EXCEPTION(err);
        }

        for (unsigned i = 0; i < fields.size(); i++) {
            std::string archive_name
                = persistency::ArchiveManager::createArchiveName(pa_ids[i]);
            std::ifstream ifs(archive_name);
            boost::archive::binary_iarchive ia(ifs);

            VALUE_TYPE_EXECUTION(fields[i]->getValueType(),
                PagedArrayEncoded<TYPE>* column = new PagedArrayEncoded<TYPE>(pa_ids[i]);\
                column->deserialize(ia);\
                columns_.push_back(column);)
        }
    }
    */
}

ColumnTable::~ColumnTable() {
    for (auto c : columns_) {
        Metadata::removePAInfo(c->getId());
        delete c;
    }

    Metadata::removePAInfo(valid_.getId());
}

AddressPair ColumnTable::insert(LogicalPtr idx, const EvalVec& values) {
    for (unsigned i = 0; i < columns_.size(); i++) {
        ValueType vt = field_infos_[i]->getValueType();
        VALUE_TYPE_EXECUTION(vt,
            PagedArrayEncoded<TYPE>* column\
                = reinterpret_cast<PagedArrayEncoded<TYPE>*>(columns_[i]);\
            TYPE::eval_type value = values[i].get<TYPE::eval_type>();\
            column->insert(value, idx);
        )
    }

    // insert to index
    if (!insertToIndex(values, idx)) {
        valid_.setInvalid(idx);
        return AddressPair();
    }

    return {idx, nullptr};
}

AddressPair ColumnTable::insert(Transaction& trans,
        LogicalPtr idx, const EvalVec& values) {
    for (unsigned i = 0; i < columns_.size(); i++) {
        ValueType vt = field_infos_[i]->getValueType();
        VALUE_TYPE_EXECUTION(vt,
            PagedArrayEncoded<TYPE>* column\
                = reinterpret_cast<PagedArrayEncoded<TYPE>*>(columns_[i]);\
            TYPE::eval_type value = values[i].get<TYPE::eval_type>();\
            column->insert(trans, value, idx);
        )
    }

    return {idx, nullptr};
}

}  // namespace storage
