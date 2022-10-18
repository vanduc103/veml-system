// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

// Related include
#include "storage/relation/table.h"

// Project include
#include "storage/catalog.h"
#include "storage/relation/row_table.h"
#include "storage/relation/column_table.h"
#include "storage/util/btree_index.h"
#include "storage/util/util_func.h"
#include "storage/util/data_transfer.h"
#include "persistency/archive_manager.h"
#include "concurrency/rti_thread.h"

// C & C++ system include
#include <fstream>
#include <random>

// Other include
#include <grpcpp/grpcpp.h>
#include <grpc/support/log.h>
#include <grpcpp/resource_quota.h>
#include <boost/serialization/vector.hpp>
#include <boost/algorithm/string.hpp>

// gRPC generated file
#include "interface/interface.grpc.pb.h"

namespace storage {

Table::iterator_wrapper::iterator_wrapper(Table* table)
        : finfos_(table->getFieldInfo()) {}

Table::iterator_wrapper::iterator_wrapper(const FieldInfoVec& finfos)
        : finfos_(finfos) {}

Table::iterator::iterator(Table* table)
        : type_(table->getTableType()) {
    if (type_ == Table::ROW) {
        itr_ = new RowTable::iterator(
                reinterpret_cast<RowTable*>(table)->begin());
    } else if (type_ == Table::COLUMN) {
        itr_ = new ColumnTable::iterator(
                reinterpret_cast<ColumnTable*>(table)->begin());
    }
}

Table::iterator::iterator(Table* table, SgmtLptrVec& sgmt_ids)
        : type_(table->getTableType()) {
    if (type_ == Table::ROW) {
        itr_ = new RowTable::iterator(
                reinterpret_cast<RowTable*>(table)->begin(sgmt_ids));
    } else if (type_ == Table::COLUMN) {
        itr_ = new ColumnTable::iterator(
                reinterpret_cast<ColumnTable*>(table)->begin(sgmt_ids));
    }
}

Table::iterator::~iterator() {
    delete itr_;
}

/*
 * create/release table
 */
Table* Table::create(Type type,
            const std::string& name, CreateFieldVec& fields,
            int64_t increment_fid, int64_t increment_start) {
    // check name duplication
    Table* check = Metadata::getTableFromName(name.c_str());
    if (check) {
        return nullptr;
    }

    // set auto increment field
    if (increment_fid >= 0) {
        if (fields[increment_fid].value_type_ != VT_BIGINT
            && fields[increment_fid].value_type_ != VT_INT) {
            std::string err("value type for auto increment should be integer");
            RTI_EXCEPTION(err);
        }
    }
    FieldInfoVec fis = Table::createFieldInfo(name, fields);

    Table* table = nullptr;
    switch (type) {
        case ROW:
            table = new RowTable(name, fis);
            break;
        case COLUMN:
            table = new ColumnTable(name, fis);
            break;
        default: {
            std::string err("invalid table type @Table::create, ");
            RTI_EXCEPTION(err);
        }
    }

    assert(table != nullptr);

    table->type_ = type;
    table->table_id_ = Metadata::addNewTable(table);
    table->increment_fid_ = increment_fid;
    table->increment_start_.store(increment_start);

    return table;
}

Table* Table::createFromFile(std::string type,
        std::string name, std::string schema, std::string delim) {
    // create table
    CreateFieldVec fields;

    std::ifstream ifs(schema);
    std::string buf;
    while(getline(ifs, buf)) {
        StrVec values;
        boost::algorithm::split(values, buf, boost::is_any_of(delim));

        if (values.size() != 2) {
            std::string err("wrong format of schema file");
            RTI_EXCEPTION(err);
        }

        // parsed values
        // values[0]: field name
        // values[1]: field type
        fields.push_back(CreateFieldInfo(values[0], getValueType(values[1])));
    }
    
    if (type.compare("ROW") == 0) {
        return Table::create(ROW, name, fields);
    } else if (type.compare("COLUMN") == 0) {
        return Table::create(COLUMN, name, fields);
    } else {
        std::string err("invalid table type, only 'ROW' or 'COLUMN' is valid");
        RTI_EXCEPTION(err);
    }

    return nullptr;
}

Table::~Table() {
    for (auto fi : field_infos_) {
        delete fi;
    }
    dropIndex();
}

/**
 * insert record api
 */
bool Table::prepareInsert(EvalVec& values) {
    // handle auto increment field
    if (increment_fid_ >= 0) {
        int64_t id = increment_start_.fetch_add(1);
        values.insert(values.begin() + increment_fid_, BigInt(id));
    }

    // check the number of fields
    if (values.size() != field_infos_.size()) {
        std::string err("wrong number of values @able::prepareInsert, ");
        RTI_EXCEPTION(err);
    }

    // distribute data if table is partitioned
    if (part_fid_ >= 0) {
        distribute(values[part_fid_], values);
        return true;
    }

    return false;
}

bool Table::insertRecord(std::vector<EvalVec>& batch) {
    class Worker : public RTIThread {
      public:
        Worker(Table* table, std::vector<EvalVec> batch)
            : table_(table), batch_(batch) {}

        void* run() {
            for (auto& record : batch_) {
                table_->insertRecord(record);
            }
            return nullptr;
        }

      private:
        Table* table_;
        std::vector<EvalVec> batch_;
    };

    std::vector<Worker> workers;
    unsigned division = batch.size() / 4;
    for (unsigned i = 0; i < 4; i++) {
        if (i == 3) {
            workers.push_back(Worker(this,
                        std::vector<EvalVec>(batch.begin() + i * division,
                            batch.end())));
        } else {
            workers.push_back(Worker(this,
                        std::vector<EvalVec>(batch.begin() + i * division,
                            batch.begin() + (i+1) * division)));
        }
    }

    for (auto& w : workers) {
        w.start();
    }

    for (auto& w : workers) {
        w.join();
    }

    return true;
}

bool Table::deleteRecords(LptrVec& idxs) {
    bool success = true;
    for (auto idx : idxs) {
        success &= deleteRecord(idx);
        if (!success) return false;
    }

    return success;
}

/*
 * index management
 */
void Table::createIndex(uint32_t fid, bool unique) {
    std::string index_name(name_ + "_idx_" + std::to_string(fid));
    BTreeIndex* index =  new BTreeIndex(name_, index_name, unique);
    indexes_.push_back(Index(fid, index, unique));
}

void Table::dropIndex() {
    for (auto index : indexes_) {
        if (index.index_ != nullptr) {
            index.index_->release();
            delete index.index_;
        }
    }
}

void Table::rebuildIndex(int dop) {
    for (auto& index : indexes_) {
        std::string index_name(name_ + "_idx_" + std::to_string(index.fid_));
        index.index_ = new BTreeIndex(name_, index_name, index.unique_);
    }

    auto itr = this->begin();
    for ( ; itr; ++itr) {
        LogicalPtr lptr = itr.getLptr();
        for (auto& index : indexes_) {
            EvalValue value = itr.getValue(index.fid_);
            if (value.getEvalType() == ET_STRING) {
                String val_clone = value.getStringConst().clone();
                index.index_->insert(val_clone, lptr);
            } else {
                index.index_->insert(value, lptr);
            }
        }
    }
}

bool Table::insertToIndex(const EvalVec& values, LogicalPtr idx) {
    bool success = true;
    for (auto index : indexes_) {
        uint32_t field_id = index.fid_;
        if (values[field_id].getEvalType() == ET_STRING) {
            String val_clone = values[field_id].getStringConst().clone();
            success &= index.index_->insert(val_clone, idx);
        } else {
            success &= index.index_->insert(values[field_id], idx);
        }

        if (!success) return false;
    }

    return true;
}

bool Table::removeFromIndex(LogicalPtr idx) {
    bool success = true;
    for (auto index : indexes_) {
        EvalValue key = this->getValue(idx, index.fid_);
        success &= index.index_->remove(key);

        if (!success) return false;
    }

    return true;
}

bool Table::uniqueIndex(uint32_t fid) {
    for (auto index : indexes_) {
        if (index.fid_ == fid) {
            return index.unique_;
        }
    }

    std::string err("wrong fid for searching index, ");
    RTI_EXCEPTION(err)
    
    return false;
}

bool Table::indexSearch(EvalValue val, uint32_t fid, LogicalPtr& idx) {
    for (auto index : indexes_) {
        if (index.fid_ == fid) {
            return index.index_->find(val, idx);
        }
    }
    return false;
}

bool Table::indexMultiSearch(EvalValue val, uint32_t fid, LptrVec& idxs) {
    for (auto index : indexes_) {
        if (index.fid_ == fid) {
            return index.index_->multiFind(val, idxs);
        }
    }
    return false;
}

std::vector<EvalVec> Table::indexSearch(
        int64_t fid, EvalValue& val, IntVec fids) {
    std::vector<EvalVec> ret;

    for (auto index : indexes_) {
        if (index.fid_ == fid) {
            if (fids.size() == 0) {
                size_t num_fields = getFieldInfo().size();
                for (unsigned i = 0; i < num_fields; i++) {
                    fids.push_back(i);
                }
            }

            if (index.unique_) {
                LogicalPtr idx = LOGICAL_NULL_PTR;
                if (index.index_->find(val, idx)) {
                    if (idx != LOGICAL_NULL_PTR) {
                        ret.push_back(this->getRecord(idx, fids));
                    }
                }
            } else {
                LptrVec idxs;
                if (index.index_->multiFind(val, idxs)) {
                    for (auto idx : idxs) {
                        ret.push_back(this->getRecord(idx, fids));
                    }
                }
            }
        }
    }

    return ret;
}

bool Table::deleteFromIndex(EvalValue val, uint32_t fid) {
    LptrVec idxs;
    if (this->indexMultiSearch(val, fid, idxs)) {
        for (auto idx : idxs) {
            this->deleteRecord(idx);
        }
        return true;
    }
    return false;
}

/*
 * import
 */
void Table::import(std::string name, std::string csv, std::string delim,
                bool seq, bool header, size_t batch, int dop) {
    Table* table = Metadata::getTableFromName(name);

    if (table == nullptr) {
        std::string err("wrong table name @Table::import, ");
        RTI_EXCEPTION(err);
    }
    table->importCSV(csv, delim, seq, header, batch, dop);
}

bool Table::importCSV(std::string csv, std::string delim,
        bool seq, bool header, size_t batch, int dop) {
    class InsertWorker : public RTIThread {
      public:
        InsertWorker(Table* table, int start, int end,
                StrVec& batch, std::string delim)
            : table_(table), start_(start), end_(end),
            batch_(batch), delim_(delim) {}

        void* run() {
            FieldInfoVec finfo = table_->getFieldInfo();
            for (int i = start_; i < end_; i++) {
                EvalVec values;
                Table::parseAndTranslate(batch_[i], finfo, values, delim_);
                table_->insertRecord(values);
            }

            return nullptr;
        }

      private:
        Table* table_;
        int start_;
        int end_;
        StrVec& batch_;
        std::string delim_;
    };

    // open csv file
    std::ifstream data(csv);
    if (data.good() == false) {
        std::string err("invalid csv path: " + csv + "\n");
        RTI_EXCEPTION(err);
    }
    std::string buf;
    StrVec batch_buf;

    // skip header
    if (header) getline(data, buf);

    // sequential load
    if (seq) dop = 1;

    while (getline(data, buf)) {
        batch_buf.push_back(buf);
        if (batch_buf.size() == batch) {
            int size = batch / dop;
            int remains = batch % dop;

            // generate insert workers
            std::vector<InsertWorker> workers;
            int start = 0;
            for (int i = 0; i < dop; i++) {
                int end = start + size;
                if (i < remains) end++;

                workers.push_back(InsertWorker(
                            this, start, end, batch_buf, delim));
                start = end;
            }

            // run insert workers
            for (auto& w: workers) {
                w.start();
            }

            for (auto& w: workers) {
                w.join();
            }

            // clear batch
            batch_buf.clear();
        }
    }

    // process remains
    if (batch_buf.size() > 0) {
        int size = batch_buf.size() / dop;
        int remains = batch_buf.size() % dop;

        // generate insert workers
        std::vector<InsertWorker> workers;
        int start = 0;
        for (int i = 0; i < dop; i++) {
            int end = start + size;
            if (i < remains) end++;

            workers.push_back(InsertWorker(
                        this, start, end, batch_buf, delim));
            start = end;
        }

        // run insert workers
        for (auto& w: workers) {
            w.start();
        }

        for (auto& w: workers) {
            w.join();
        }

        // clear batch
        batch_buf.clear();
    }

    return true;
}

Table* Table::load(std::string type,
        std::string name, std::string schema, std::string csv,
        std::string delim, bool seq, bool header) {
    Table* table = Table::createFromFile(type, name, schema, delim);
    Table::import(name, csv, delim, seq, header);

    return table;
}

Table::iterator Table::begin() {
    return iterator(this);
}

Table::iterator Table::begin(SgmtLptrVec& sgmt_ids) {
    return iterator(this, sgmt_ids);
}

/**
 * table partition
 */
bool Table::partition(int32_t fid, StrVec& node_addrs, int dop) {
    if (node_addrs.size() == 0 || dop <= 0) {
        return false;
    }

    // 1. set partitioning info
    part_fid_ = fid;
    part_nodes_ = node_addrs;
    PartitionFunc* func = HashMod::create(part_nodes_.size());
    part_func_ = PartitionFunc::generate(func);

    // 2. generate local partitions
    std::vector<Table*> partitions
        = generatePartition(fid, part_nodes_.size(), dop);

    // 3. transfer partitions to nodes
    TransferRequester requester(dop);
    for (unsigned i = 0; i < partitions.size(); i++) {
        requester.add(part_nodes_[i], partitions[i], name_);
    }
    requester.run();

    // 4. truncate data
    truncate();
    for (auto p : partitions) {
        Metadata::releaseTable(p->getTableName());
    }

    return true;
}

std::vector<Table*> Table::generatePartition(int32_t fid, size_t num, int dop) {
    // generate partition func
    PartitionFunc* func = HashMod::create(num);
    auto part_func = PartitionFunc::generate(func);

    // 1. scan table and collect lptrs for each partition
    SgmtLptrVec sgmt_ids;
    this->getAllSgmtIds(sgmt_ids);
    size_t num_sgmts = sgmt_ids.size();

    // 1-1. generate and run scan workers
    class ScanWorker : public RTIThread {
      public:
        ScanWorker(Table* t, SgmtLptrVec sgmt_ids, int32_t fid,
                PartEvalFunc& func, size_t num)
            : table_(t), sgmt_ids_(sgmt_ids), fid_(fid),
            func_(func), lptrs_(num, LptrVec()) {}

        void* run() {
            auto itr = table_->begin(sgmt_ids_);
            for ( ; itr; ++itr) {
                int part_id = -1;
                if (fid_ < 0) {
                    part_id = itr.getLptr() % lptrs_.size();
                } else {
                    part_id = func_(itr.getValue(fid_));
                }
                lptrs_[part_id].push_back(itr.getLptr());
            }
            return nullptr;
        }

        LptrVec& getLptrs(unsigned i) { return lptrs_[i]; }

      private:
        Table* table_;
        SgmtLptrVec sgmt_ids_;
        int32_t fid_;
        PartEvalFunc& func_;
        std::vector<LptrVec> lptrs_;
    };

    int division = num_sgmts / dop;
    int mod = num_sgmts % dop;
    int start, end = 0;
    std::vector<ScanWorker> sworkers;
    for (int i = 0; i < dop; i++) {
        start = end;
        end = start + division;
        if (i < mod) end++;

        SgmtLptrVec part_sgmt_ids(
                        sgmt_ids.begin()+start, sgmt_ids.begin()+end);
        sworkers.push_back(ScanWorker(this,
                    part_sgmt_ids, fid, part_func, num));
    }

    for (auto& w: sworkers) w.start();
    for (auto& w: sworkers) w.join();

    // 1-2. merge scanned result
    std::vector<LptrVec> lptrs(num, LptrVec());
    for (auto& w: sworkers) {
        for (unsigned i = 0; i < num; i++) {
            LptrVec& l = w.getLptrs(i);
            std::copy(l.begin(), l.end(), std::back_inserter(lptrs[i]));
        }
    }

    // 2. create partition tables
    FieldInfoVec fields = this->getFieldInfo();
    CreateFieldVec cfinfos =Table::createFieldInfo(fields);

    std::vector<Table*> partitions;
    for (unsigned i = 0; i < num; i++) {
        std::string pname = name_ + "$PART$" + std::to_string(i);
        Table* t = Table::create(this->getTableType(), pname, cfinfos);
        for (auto idx : indexes_) {
            t->createIndex(idx.fid_, idx.unique_);
        }

        partitions.push_back(t);
    }

    // 3. generate and run insert workers
    class InsertWorker : public RTIThread {
      public:
        InsertWorker(Table* src, Table* dest,
                int start, int end, LptrVec& lptrs)
            : src_(src), dest_(dest),
            start_(start), end_(end), lptrs_(lptrs) {}

        void* run() {
            for (int i = start_; i < end_; i++) {
                EvalVec record = src_->getRecord(lptrs_[i]);
                dest_->insertRecord(record);
            }
            return nullptr;
        }

      private:
        Table* src_;
        Table* dest_;
        int start_;
        int end_;
        LptrVec& lptrs_;
    };

    std::vector<InsertWorker> iworkers;
    for (unsigned part = 0; part < num; part++) {
        size_t num_records = lptrs[part].size();
        int division = num_records / dop;
        int mod = num_records % dop;
        int start, end = 0;
        for (int i = 0; i < dop; i++) {
            start = end;
            end = start + division;
            if (i < mod) end++;

            iworkers.push_back(
                    InsertWorker(this, partitions[part],
                        start, end, lptrs[part]));
        }
    }

    for (auto& w: iworkers) w.start();
    for (auto& w: iworkers) w.join();

    return partitions;
}

void Table::setPartition() {
    this->rebuildIndex();
    this->clearPartNodes();
}

void Table::distribute(const EvalValue& part_val, const EvalVec& values) {
    // select node to send data
    int part_id = part_func_(part_val);
    if (part_id < 0 || part_id >= (int)part_nodes_.size()) {
        std::string err("invalid partition id @RowTable::partition, ");
        RTI_EXCEPTION(err);
    }

    using grpc::Channel;
    using grpc::ClientAsyncResponseReader;
    using grpc::ClientContext;
    ClientContext ctx;
    dan::InsertRecord request;
    dan::Str response;

    // set partitioned table name
    request.set_tname(name_);
    auto node = std::unique_ptr<dan::DANInterface::Stub>(
                    dan::DANInterface::NewStub(
                        grpc::CreateChannel(part_nodes_[part_id],
                            grpc::InsecureChannelCredentials()))); 

    // request
    generatePBRecord(request.mutable_values(), values);
    node->insert_record(&ctx, request, &response);
}

/*
 * serialize/deserialize
 */
void Table::serialize(boost::archive::binary_oarchive& oa) const {
    oa << name_;
    oa << type_;
    oa << table_id_;

    CreateFieldVec cfi = Table::createFieldInfo(field_infos_);
    oa << cfi;

    oa << indexes_;
    
    oa << increment_fid_;
    int64_t increment_start = increment_start_.load();
    oa << increment_start;

    oa << part_fid_;
    oa << part_nodes_;
}

Table* Table::deserialize(boost::archive::binary_iarchive& ia) {
    std::string name;
    Table::Type type;
    uint32_t table_id;
    CreateFieldVec fields;
    IndexVec indexes;
    int64_t increment_fid, increment_start;
    int32_t part_fid;
    StrVec part_nodes;

    ia >> name;
    ia >> type;
    ia >> table_id;
    ia >> fields;
    ia >> indexes;
    ia >> increment_fid;
    ia >> increment_start;
    ia >> part_fid;
    ia >> part_nodes;

    for (auto& index : indexes) {
        index.index_ = nullptr;
    }

    FieldInfoVec finfos = Table::createFieldInfo(name, fields);
    Table* t = nullptr;

    switch (type) {
        case ROW:
            uint32_t fixed_slot_id, var_slot_id;
            ia >> fixed_slot_id >> var_slot_id;
            t = new RowTable(name, table_id, finfos, indexes,
                    increment_fid, increment_start,
                    part_fid, part_nodes,
                    fixed_slot_id, var_slot_id);
            break;
        default: {
            std::string err("table deserialization is not implemented yet");
            RTI_EXCEPTION(err);
        }
    }
    return t;
}

/*
 * constructor/destructor
 */
Table::Table(const std::string& name, const FieldInfoVec& finfo)
    : name_(name), field_infos_(finfo), part_fid_(-1) {}

Table::Table(std::string name, Type type, uint32_t table_id,
        FieldInfoVec& finfos, IndexVec& indexes,
        int64_t increment_fid, int64_t increment_start,
        int32_t part_fid, StrVec& part_nodes)
    : name_(name), type_(type), field_infos_(finfos), indexes_(indexes),
    increment_fid_(increment_fid), increment_start_(increment_start),
    part_fid_(part_fid), part_nodes_(part_nodes) {
    table_id_ = Metadata::addNewTable(this, table_id);
}

/*
 * public, getter functions
 */
FieldInfo* Table::getFieldInfo(unsigned fid) const {
    return field_infos_[fid];
}

FieldInfoVec Table::getFieldInfo() const {
    return field_infos_;
}

void Table::getFieldInfo(FieldInfoVec& finfos) const {
    for (auto fi : field_infos_) {
        finfos.push_back(fi);
    }
}

StrVec Table::getFieldInfoStr() const {
    StrVec ret;
    for (const auto fi : field_infos_) {
        ret.push_back(fi->getFieldName());
        ret.push_back(fi->getValueTypeName());
    }

    return ret;
}

std::string Table::getFieldName(int32_t fid) const {
    return field_infos_[fid]->getFieldName();
}

void Table::getFieldAsString(StrVec& fields) const {
    for (auto fi : field_infos_) {
        switch (fi->getValueType()) {
            case VT_BIGINT:
                fields.push_back("BIGINT");
                break;
            case VT_INT:
                fields.push_back("INT");
                break;
            case VT_DOUBLE:
                fields.push_back("DOUBLE");
                break;
            case VT_FLOAT:
                fields.push_back("FLOAT");
                break;
            case VT_VARCHAR:
                fields.push_back("STRING");
                break;
            default:
                fields.push_back("IMPL_TODO");
        }
    }
}

std::string Table::findNode(EvalVec& values) {
    // select node to send data
    int part_id = -1;
    if (part_fid_ < 0) {
        part_id = std::rand() % part_nodes_.size();
    } else {
        part_id = part_func_(values[part_fid_]);
    }

    if (part_id < 0 || part_id >= (int)part_nodes_.size()) {
        std::string err("invalid partition id @RowTable::partition, ");
        RTI_EXCEPTION(err);
    }

    return part_nodes_[part_id];
}

EvalValue Table::translateValue(FieldInfo* fi, const std::string& value) {
    switch (fi->getValueType()) {
        case VT_BIGINT:
        case VT_INT:
             return BigInt(std::stol(value.c_str()));
        case VT_DECIMAL:
             return Decimal(value.c_str());
        case VT_FLOAT:
        case VT_DOUBLE:
            return Double(std::stod(value.c_str()));
        case VT_CHAR:
        case VT_VARCHAR:
            return String(value.c_str());
        case VT_DATE:
            return Date(value.c_str());
        case VT_TIME:
            return Time(value.c_str());
        case VT_TIMESTAMP:
            return Timestamp(value.c_str());
        case VT_BOOL:
            if (value.compare("True") == 0)
                return Bool(true);
            if (value.compare("False") == 0)
                return Bool(false);
        case VT_INTLIST: {
            IntVec vec_l;
            int pos_s = 1;
            while (pos_s > 0) {
                int pos_e = value.find(',', pos_s);
                std::string v;
                if (pos_e < 0)
                    v = std::string(value.c_str()+pos_s,
                            value.size()-pos_s-1);
                else
                    v = std::string(value.c_str()+pos_s, pos_e-pos_s);
                vec_l.push_back(std::stoi(v));
                pos_s = pos_e + 1;
            }
            return vec_l;
        }
        case VT_LONGLIST: {
            LongVec vec_l;
            int pos_s = 1;
            while (pos_s > 0) {
                int pos_e = value.find(',', pos_s);
                std::string v;
                if (pos_e < 0)
                    v = std::string(value.c_str()+pos_s,
                            value.size()-pos_s-1);
                else
                    v = std::string(value.c_str()+pos_s, pos_e-pos_s);
                vec_l.push_back(std::stol(v));
                pos_s = pos_e + 1;
            }
            return vec_l;
        }
        case VT_FLOATLIST: {
            FloatVec vec_d;
            int pos_s = 1;
            while (pos_s > 0) {
                int pos_e = value.find(',', pos_s);
                std::string v;
                if (pos_e < 0)
                    v = std::string(value.c_str()+pos_s,
                            value.size()-pos_s-1);
                else
                    v = std::string(value.c_str()+pos_s, pos_e-pos_s);
                vec_d.push_back(std::stof(v));
                pos_s = pos_e + 1;
            }
            return vec_d;
        }
        case VT_DOUBLELIST: {
            DoubleVec vec_d;
            int pos_s = 1;
            while (pos_s > 0) {
                int pos_e = value.find(',', pos_s);
                std::string v;
                if (pos_e < 0)
                    v = std::string(value.c_str()+pos_s,
                            value.size()-pos_s-1);
                else
                    v = std::string(value.c_str()+pos_s, pos_e-pos_s);
                vec_d.push_back(std::stod(v));
                pos_s = pos_e + 1;
            }
            return vec_d;
        }
        default: {
            std::string err("invalid value type @Table::translateValue, ");
            RTI_EXCEPTION(err);
        }
    }
}

void Table::translateValues(const StrVec& values, EvalVec& evals) {
    translateValues(values, evals, this->getFieldInfo());
}

void Table::translateValues(
        const StrVec& values, EvalVec& evals, FieldInfoVec finfos) {
    if (finfos.size() != values.size()) {
        std::string err("incorrect number of fields @Table::translateValues, ");
        RTI_EXCEPTION(err);
    }
    
    evals.reserve(values.size());
    for (unsigned i = 0; i < finfos.size(); i++) {
        evals.push_back(translateValue(finfos[i], values[i]));
    }
}

void Table::translateToStr(const EvalVec& evals, StrVec& values) {
    if (field_infos_.size() != evals.size()) {
        std::string err("incorrect number of fields @Table::translateToStr, ");
        RTI_EXCEPTION(err);
    }

    values.reserve(evals.size());
    for (auto& val : evals) {
        values.push_back(val.to_string());
    }
}

void Table::parseAndTranslate(const std::string str,
        FieldInfoVec finfos, EvalVec& evals, std::string delim) {
    int pos_s = 0;
    for (unsigned i = 0; i < finfos.size(); i++) {
        if (finfos[i]->getValueType() == VT_INTLIST
                || finfos[i]->getValueType() == VT_LONGLIST
                || finfos[i]->getValueType() == VT_FLOATLIST
                || finfos[i]->getValueType() == VT_DOUBLELIST) {
            // find opening/closing double quotes
            pos_s = str.find('"', pos_s) + 1;
            int pos_e = str.find('"', pos_s);
            
            // extract list and translate
            std::string list(str.c_str() + pos_s, pos_e-pos_s);
            evals.push_back(translateValue(finfos[i], list));

            // skip two character(closing double quotes and delimiter)
            pos_s = pos_e + 2;
        } else {
            // find next delimiter
            int pos_e = str.find(delim.c_str(), pos_s);

            // extract data and translate
            std::string data;
            if (pos_e < 0) {
                data = std::string(str.c_str()+pos_s);
            } else {
                data = std::string(str.c_str()+pos_s, pos_e-pos_s);
            }
            evals.push_back(translateValue(finfos[i], data));

            // skip one character(delimiter)
            pos_s = pos_e + 1;
        }
    }
}

FieldInfoVec Table::createFieldInfo(
                std::string tname, const CreateFieldVec& cfields) {
    FieldInfoVec ret;
    uint32_t offset = 0;
    int i = 0;
    for (const auto& cfi : cfields) {
        std::string type_name = getNameOfValueType(cfi.value_type_);
        FieldInfo* fi = new FieldInfo(tname.c_str(),
                            cfi.field_name_.c_str(), i, cfi.value_type_,
                            type_name.c_str(), cfi.size_, offset);
        ret.push_back(fi);
        ++i;
        offset += cfi.size_;
    }

    return ret;
}

CreateFieldVec Table::createFieldInfo(const FieldInfoVec& fields) {
    CreateFieldVec cfi;
    for (const auto fi: fields) {
        if (fi->getValueType() == VT_CHAR) {
            cfi.push_back(CreateFieldInfo(fi->getFieldName(),
                                fi->getValueType(), fi->getSize()));
        } else {
            cfi.push_back(CreateFieldInfo(
                            fi->getFieldName(), fi->getValueType()));
        }
    }

    return cfi;
}

bool Table::checkFields(const StrVec& cnames, std::string& incorrect) {
    for (auto& cname : cnames) {
        unsigned i = 0;
        for ( ; i < field_infos_.size(); i++) {
            if (cname == field_infos_[i]->getFieldName()) break;
        }

        if (i == field_infos_.size()) {
            incorrect = cname;
            return false;
        }
    }

    return true;
}

}  // namespace storage
