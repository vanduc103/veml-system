// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

// Related include
#include "storage/graph/graph_delta.h"

// Project include
#include "storage/util/util_func.h"

namespace storage {

GraphDelta::iterator::iterator(GraphDelta* delta, bool eprop)
    : iterator(delta, eprop, 0, (int)delta->hash_table_.size()) {}

GraphDelta::iterator::iterator(
        GraphDelta* delta, bool eprop, int start, int end)
    : delta_(delta), list_idx_(0), list_size_(0),
    bucket_idx_(start), end_idx_(end),
    bucket_itr_(delta->hash_table_[start].bucket_->begin()),
    is_ended_(true), use_eprop_(eprop) {
    // find first bucket that contains edge
    while (bucket_idx_ < end_idx_) {
        bucket_itr_ = delta_->hash_table_[bucket_idx_].bucket_->begin();
        if (bucket_itr_) {
            // set current slot
            src_ = bucket_itr_.getValue(0).getBigInt().get();
            dest_ = &(bucket_itr_.getValue(1).getLongList());
            if (use_eprop_) eprop_ = &(bucket_itr_.getValue(2).getLongList());
            list_size_ = dest_->size();

            is_ended_ = false;
            break;
        }
        ++bucket_idx_;
    }
}

int64_t GraphDelta::iterator::getSrc() {
    return src_;
}

int64_t GraphDelta::iterator::getDest() {
    return (*dest_)[list_idx_];
}

int64_t GraphDelta::iterator::getEprop() {
    return (*eprop_)[list_idx_];
}

LongVec GraphDelta::iterator::getDests() {
    LongList& dest_list = this->getDestList();
    LongVec ret;
    dest_list.copyTo(ret);

    return ret;
}

LongVec GraphDelta::iterator::getEprops() {
    LongList& eprop_list = this->getEpropList();
    LongVec ret;
    eprop_list.copyTo(ret);

    return ret;
}

int32_t GraphDelta::iterator::getBucketIdx() {
    return bucket_idx_;
}

LogicalPtr GraphDelta::iterator::getLptr() {
    return bucket_itr_.getLptr();
}

LongList& GraphDelta::iterator::getDestList() {
    return *dest_;
}

LongList& GraphDelta::iterator::getEpropList() {
    if (!use_eprop_) {
        std::string err("invalid request @GraphDelta::iterator::getEpropList,");
        RTI_EXCEPTION(err);
    }
    return *eprop_;
}

LongList* GraphDelta::iterator::getDestListPtr() {
    return dest_;
}

LongList* GraphDelta::iterator::getEpropListPtr() {
    if (!use_eprop_) {
        return nullptr;
    }
    return eprop_;
}

EvalVec GraphDelta::iterator::getRecord() {
    return bucket_itr_.getRecord();
}

size_t GraphDelta::iterator::size() {
    return bucket_itr_.getValue(1).getLongList().size();
}

GraphDelta::iterator& GraphDelta::iterator::operator++() {
    if (is_ended_) {
        return *this;
    }

    // move to next edge in adjacent list
    list_idx_++;

    if (list_idx_ >= list_size_) {
        // move to next adjacent list
        this->nextList();
    }
    return *this;
}

void GraphDelta::iterator::nextList(bool skip_empty) {
    if (is_ended_) {
        return;
    }

    // move to next adjacent list
    ++bucket_itr_;
    while (!bucket_itr_) {
        // move to next bucket
        if (++bucket_idx_ >= end_idx_) {
            // end of iteration
            is_ended_ = true;
            return;
        }
        bucket_itr_ = delta_->hash_table_[bucket_idx_].bucket_->begin();
    }

    // set current slot
    src_ = bucket_itr_.getValue(0).getBigInt().get();
    dest_ = &(bucket_itr_.getValue(1).getLongList());
    if (use_eprop_) eprop_ = &(bucket_itr_.getValue(2).getLongList());
    list_size_ = dest_->size();
    list_idx_ = 0;
}

GraphDelta::GraphDelta(std::string name, uint32_t id,
        bool eprop, uint32_t init)
    : name_(name + "$DELTA$" + std::to_string(id)),
    id_(id), eprop_(eprop), hash_divisor_(init) {
    // create delta table
    // |src_vertex_id|adj_list|edge_property_id|
    CreateFieldVec fields;
    fields.push_back(CreateFieldInfo("SRC_ID", ValueType::VT_BIGINT));
    fields.push_back(CreateFieldInfo("ADJLIST", ValueType::VT_LONGLIST));
    if (eprop_)
        fields.push_back(CreateFieldInfo("EPROP", ValueType::VT_LONGLIST));

    hash_table_.reserve(hash_divisor_);
    for (uint32_t i = 0; i < hash_divisor_; ++i) {
        RowTable* t = reinterpret_cast<RowTable*>(
                        Table::create(Table::ROW,
                            name_ + "$" + std::to_string(i), fields));
        t->createIndex(0, true);

        hash_table_.push_back(Bucket(t, i));
    }
}

GraphDelta::GraphDelta(std::string name, uint32_t id, bool eprop,
            uint32_t hash_divisor, std::vector<uint32_t>& hash_tid)
    : name_(name), id_(id), eprop_(eprop), hash_divisor_(hash_divisor) {
    for (unsigned i = 0; i < hash_divisor; i++) {
        RowTable* t = reinterpret_cast<RowTable*>(
                            Metadata::getTable(hash_tid[i]));
        hash_table_.push_back(Bucket(t, i));
    }
}

void GraphDelta::releaseBuckets() {
    for (unsigned i = 0; i < hash_table_.size(); ++i) {
        Metadata::releaseTableById(hash_table_[i].bucket_->getTableId());
    }
}

/**
 * insert new edge to delta
 */
bool GraphDelta::insertEdge(int64_t src, int64_t dest, int64_t eprop) {
    // get adjacent list for src vertex
    EvalVec list = this->getList(src);

    // add dest vertex to adjacent list
    LongList& dest_list = list[1].getLongList();
    dest_list.push_back(dest);

    // add edge property idx
    if (eprop_) {
        LongList& eprop_list = list[2].getLongList();
        eprop_list.push_back(eprop);
    }

    return true;
}

/**
 * insert edge list to delta
 */
void GraphDelta::insertList(EvalVec& list) {
    unsigned num_fields = eprop_? 3 : 2;
    if (list.size() == num_fields && list[0].getEvalType() == ET_BIGINT) {
        int64_t vid = list[0].getBigInt().get();
        RowTable* bucket = hash_table_[getHashVal(vid)].bucket_;
        bucket->insertRecord(list);
    } else {
        std::string err("wrong values in GraphDelta::insertList");
        RTI_EXCEPTION(err);
    }
}

/**
 * remove edge list in delta
 */
void GraphDelta::remove(int64_t src) {
    RowTable* bucket = hash_table_[getHashVal(src)].bucket_;

    LogicalPtr src_idx;
    if (!bucket->indexSearch(BigInt(src), 0, src_idx)) {
        // src-id does not exists in delta
        return;
    }

    bucket->deleteRecord(src_idx);
}

void GraphDelta::remove(int32_t hashid, LongVec& srcids) {
    RowTable* bucket = hash_table_[hashid].bucket_;

    for (auto s : srcids) {
        LogicalPtr src_idx;
        if (!bucket->indexSearch(BigInt(s), 0, src_idx)) {
            // src-id does not exists in delta
            return;
        }

        bucket->deleteRecord(src_idx);
    }
}

/**
 * get vertex-id of neighboring nodes
 */
void GraphDelta::adjacentList(int64_t src, LongVec& dest) {
    RowTable* bucket = hash_table_[getHashVal(src)].bucket_;

    LogicalPtr src_idx;
    if (!bucket->indexSearch(BigInt(src), 0, src_idx)) {
        // src-id does not exists in delta
        return;
    }

    // get adjacent list
    LongList &dest_list = bucket->getValue(src_idx, 1).getLongList();

    // if size of list is zero, return empty list
    size_t size = dest_list.size();
    if (!size)
        return;

    dest_list.copyTo(dest);
}

/**
 * get vertex-id of neighboring nodes and index of edge properties
 */
void GraphDelta::adjacentList(int64_t src, LongVec& dest, LongVec& eprop) {
    RowTable* bucket = hash_table_[getHashVal(src)].bucket_;

    LogicalPtr src_idx;
    if (!bucket->indexSearch(BigInt(src), 0, src_idx)) {
        // src-id does not exists in delta
        return;
    }

    // get adjacent list
    LongList &dest_list = bucket->getValue(src_idx, 1).getLongList();
    LongList &eprop_list = bucket->getValue(src_idx, 2).getLongList();

    // if size of list is zero, return empty list
    size_t size = dest_list.size();
    if (!size)
        return;

    dest_list.copyTo(dest);
    eprop_list.copyTo(eprop);
}

size_t GraphDelta::size() {
    size_t num = 0;
    auto itr = this->begin();
    for ( ; itr; itr.nextList()) {
        num += itr.size();
    }

    return num;
}

void GraphDelta::serialize(boost::archive::binary_oarchive& oa) const {
    oa << name_ << id_ << eprop_ << hash_divisor_;
    for (auto& b : hash_table_) {
        uint32_t bucket_tid = b.bucket_->getTableId();
        oa << bucket_tid;
    }
}

std::vector<GraphDelta::iterator*> GraphDelta::beginMulti(int dop) {
    int num_bucket = hash_table_.size();
    int division = num_bucket / dop;
    int mod = num_bucket % dop;
    
    std::vector<iterator*> ret;
    int start, end = 0;
    for (int i = 0; i < dop; i++) {
        start = end;
        end += division;
        if (i < mod) end++;

        ret.push_back(new iterator(this, eprop_, start, end));
    }

    return ret;
}

size_t GraphDelta::getMemoryUsage(std::ostream& os, bool verbose, int level) {
    size_t total = 0;
    std::vector<std::pair<unsigned, size_t> > bucket_usage;
    for (unsigned i = 0; i < hash_table_.size(); i++) {
        size_t bucket_mem
            = hash_table_[i].bucket_->getMemoryUsage(os, false, level+1);
        total += bucket_mem;
        bucket_usage.emplace_back(i, bucket_mem);
    }

    if (verbose) {
        std::string indent = "";
        for (int i = 0; i < level; i++) indent += '\t';

        os << indent << "Delta Name: " << name_ << " => "
            << memory_unit_str(total) << '\n';
        for (auto itr : bucket_usage) {
            os << indent << '\n' << "Bucket(" << itr.first << "): "
                << memory_unit_str(itr.second) << '\n';
        }
    }

    return total + sizeof(this);
}

EvalVec GraphDelta::getList(int64_t vid) {
    RowTable* bucket = hash_table_[getHashVal(vid)].bucket_;

    // search list with vertex-id
    LogicalPtr idx = LOGICAL_NULL_PTR;
    while (idx == LOGICAL_NULL_PTR) {
        if (!bucket->indexSearch(BigInt(vid), 0, idx)) {
            // if vertex-id does not exist, insert new list
            EvalVec values = {BigInt(vid), LongList()};
            if (eprop_) values.push_back(LongList());

            idx = bucket->insertRecord(values).lptr_;
        }
    }

    return bucket->getRecord(idx);
}

/**
 * multi-layer architecture
 */
size_t GraphDelta::MergeInfo::size() {
    size_t size = 0;
    for (unsigned i = 0; i < map_.size(); i++) {
        PartitionMap& partition_map = map_[i];
        auto itr1 = partition_map.begin();
        for ( ; itr1 != partition_map.end(); ++itr1) {
            auto itr2 = itr1->second.edges_.begin();
            for ( ; itr2 != itr1->second.edges_.end(); ++itr2) {
                size += itr2->second.first->size();
            }
        }
    }
    
    return size;
}

size_t GraphDelta::MergeInfo::size_to_deletes() {
    size_t size = 0;
    auto itr = deletes_.begin();
    for ( ; itr != deletes_.end(); ++itr) {
        size += itr->second.size();
    }

    return size;
}

void GraphDelta::collectMerge(LongVec& min, LongVec& max,
        GraphMain& main, MergeInfo& merge_info, size_t least) {
    for (auto itr = this->begin(); itr; itr.nextList()) {
        int64_t src = itr.getSrc();
        int32_t vpid = src >> 32;

        // find partition for src-id
        int32_t partition_id = main.findPartitionId(src);
        auto part_itr = merge_info.map_[vpid].find(partition_id);
        if (part_itr == merge_info.map_[vpid].end()) {
            part_itr = merge_info.map_[vpid].insert(std::make_pair(
                        partition_id, MergeInfo::Partition(vpid))).first;
        }

        // collect edge-list
        MergeInfo::Partition& partition = part_itr->second;
        partition.edges_[src]
            = std::make_pair(itr.getDestListPtr(), itr.getEpropListPtr());

        // set partition info
        if (partition.min_ > src) partition.min_ = src;
        if (partition.max_ < src) partition.max_ = src;

        partition.size_ += itr.getDestList().size();
        if (!partition.qualified_ && partition.size_ >= least) {
            partition.qualified_ = true;
        }
    }

    // remove unqualified partition
    for (auto& map : merge_info.map_) {
        for (auto itr = map.cbegin(); itr != map.cend(); ) {
            const MergeInfo::Partition& partition = itr->second;
            int32_t vpid = partition.vpid_;
            if (!partition.qualified_) {
                itr = map.erase(itr);
            } else {
                // set global min/max
                if (min[vpid] > partition.min_) min[vpid] = partition.min_;
                if (max[vpid] < partition.max_) max[vpid] = partition.max_;

                // collect src-ids to delete
                auto sitr = partition.edges_.begin();
                for ( ; sitr != partition.edges_.end(); ++sitr) {
                    int64_t srcid = sitr->first;
                    int32_t hashid = getHashVal(srcid);

                    auto ditr = merge_info.deletes_.find(hashid);
                    if (ditr == merge_info.deletes_.end()) {
                        ditr = merge_info.deletes_.insert(
                                std::make_pair(hashid, LongVec())).first;
                    }
                    ditr->second.push_back(srcid);
                }
                ++itr;
            }
        }
    }
}

} // namespace storage
