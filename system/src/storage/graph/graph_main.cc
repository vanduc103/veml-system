// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

// Related include
#include "storage/graph/graph_main.h"

// Project include
#include "persistency/archive_manager.h"

namespace storage {

/**
 * GraphPartition::iterator
 */
GraphPartition::iterator::iterator(GraphPartition* partition)
        : partition_(partition), src_idx_(0),
        num_src_(partition->src_.getNumElement()),
        dest_idx_(0), dest_end_(0), is_ended_(true) {
    while (src_idx_ < num_src_ && is_ended_) {
        dest_idx_ = (src_idx_ == 0)? 0 : partition_->src_[src_idx_-1].get();
        dest_end_ = partition_->src_[src_idx_].get();

        if (dest_end_ > dest_idx_) is_ended_ = false;
        else src_idx_++;
    }
}

int64_t GraphPartition::iterator::getSrc() {
    return partition_->min_ + src_idx_;
}

int64_t GraphPartition::iterator::getDest() {
    return partition_->dest_[dest_idx_].get();
}

int64_t GraphPartition::iterator::getEprop() {
    return partition_->eprop_[dest_idx_].get();
}

LongVec GraphPartition::iterator::getDests() {
    LongVec ret;
    partition_->dest_.copyTo(ret, dest_idx_, dest_end_);
    return ret;
}

LongVec GraphPartition::iterator::getEprops() {
    LongVec ret;
    partition_->eprop_.copyTo(ret, dest_idx_, dest_end_);
    return ret;
}

GraphPartition::iterator& GraphPartition::iterator::operator++() {
    if (++dest_idx_ >= dest_end_) {
        proceed();
    }
    return *this;
}

void GraphPartition::iterator::nextList(bool skip_empty) {
    dest_idx_ = dest_end_;
    proceed(skip_empty);
}

void GraphPartition::iterator::proceed(bool skip_empty) {
    while (++src_idx_ < num_src_) {
        dest_end_ = partition_->src_[src_idx_].get();
        if (dest_end_ > dest_idx_ || !skip_empty) break;
    }

    is_ended_ = (src_idx_ == num_src_);
}

/**
 * GraphPartition
 */
GraphPartition::GraphPartition(bool use_eprop)
    : use_eprop_(use_eprop), id_(-1), size_(0),
    min_(std::numeric_limits<int64_t>::max()),
    max_(std::numeric_limits<int64_t>::min()) {}

GraphPartition::GraphPartition(bool use_eprop, int64_t start, int64_t end,
        std::map<int64_t, std::pair<LongList*, LongList*> >& edges)
    : use_eprop_(use_eprop), min_(start), max_(end) {
    int pos = 0;
    for (int i = 0; i < end - start; i++) {
        auto itr = edges.find(start + i);
        if (itr == edges.end()) {
            // skip for thir src-id
            src_.insert(BigInt(pos), i);
        } else {
            // get dest/eprop list
            LongVec dest, eprop;
            itr->second.first->copyTo(dest);
            if (use_eprop) itr->second.second->copyTo(eprop);

            // insert dest-id/eprop
            for (unsigned j = 0; j < dest.size(); j++) {
                if (use_eprop) eprop_.insert(BigInt(eprop[j]), pos);
                dest_.insert(BigInt(dest[j]), pos++);
            }

            src_.insert(BigInt(pos), i);
        }
    }

    size_ = pos;
}

GraphPartition::GraphPartition(bool use_eprop, int64_t start, int64_t end,
        LongVec& src, LongVec& dest, LongVec& eprop,
        std::map<int64_t, std::pair<LongList*, LongList*> >& edges) 
    : use_eprop_(use_eprop), min_(start), max_(end) {
    int pos = 0;
    for (int i = 0; i < end - start; i++) {
        LongVec new_dest, new_eprop;
        
        // get dest/eprop from new edge-list
        auto itr = edges.find(start + i);
        if (itr != edges.end()) {
            // get dest/eprop list
            itr->second.first->copyTo(new_dest);
            if (use_eprop) itr->second.second->copyTo(new_eprop);

        }

        // get dest/eprop from old edge-list
        if (i < (int)src.size()) {
            int64_t s = i == 0? 0 : src[i-1];
            int64_t e = src[i];
            new_dest.insert(new_dest.end(), dest.begin()+s, dest.begin()+e);
            if (use_eprop) {
                new_eprop.insert(
                        new_eprop.end(), eprop.begin()+s, eprop.begin()+e);
            }
        }

        // insert edges to new partition
        for (unsigned j = 0; j < new_dest.size(); j++) {
            if (use_eprop) eprop_.insert(BigInt(new_eprop[j]), pos);
            dest_.insert(BigInt(new_dest[j]), pos++);
        }

        src_.insert(BigInt(pos), i);
    }

    size_ = pos;
}

GraphPartition::GraphPartition(uint32_t src_pa_id, uint32_t dest_pa_id,
            uint32_t eprop_pa_id, bool use_eprop, int32_t id,
            int64_t size, int64_t min, int64_t max) 
    : src_(src_pa_id), dest_(dest_pa_id), eprop_(eprop_pa_id),
    use_eprop_(use_eprop), id_(id), size_(size), min_(min), max_(max) {}

size_t GraphPartition::adjacentList(int64_t src, LongVec& dest) {
    // get offset in list;
    src -= min_;
    
    // get position of dest array
    int64_t from = (src == 0)? 0 : src_[src-1].get();
    int64_t to = src_[src].get();

    // get dest ids
    size_t count = to - from;
    dest_.copyTo(dest, from , to);

    return count;
}

size_t GraphPartition::adjacentList(int64_t src, LongVec& dest, LongVec& eprop) {
    // get offset in list;
    src -= min_;
    
    // get position of dest array
    int64_t from = (src == 0)? 0 : src_[src-1].get();
    int64_t to = src_[src].get();

    // get dest/eprop ids
    size_t count = to - from;
    dest_.copyTo(dest, from , to);
    eprop_.copyTo(eprop, from , to);

    return count;
}

size_t GraphPartition::size(int64_t src) {
    if (size_ == 0 || src < min_ || src >=max_) return 0;
    // get offset in list;
    src -= min_;
    
    // get position of dest array
    int64_t from = (src == 0)? 0 : src_[src-1].get();
    int64_t to = src_[src].get();

    return to - from;
}

void GraphPartition::copyTo(LongVec& src, LongVec& dest,
        LongVec& eprop, int64_t start, int64_t end) {
    if (start == end) return;
    if (start < min_ && end < min_) return;

    // in case of start is less than min of partition
    if (start < min_) {
        for (int i = 0; i < min_-start; i++) {
            src.push_back(0);
        }
    }

    // calculate offset
    start = start>min_? start-min_ : 0;
    end -= min_;
    int64_t dest_start = start == 0? 0 : src_[start-1].get();
    int64_t dest_end = -1;

    // copy src list
    auto sitr = src_.begin(start);
    for ( ; sitr && (int64_t)sitr.getLptr() < end; ++sitr) {
        dest_end = sitr.getValue().get();
        src.push_back(dest_end - dest_start);
    }

    // copy dest list
    auto ditr = dest_.begin(dest_start);
    for ( ; ditr && (int64_t)ditr.getLptr() < dest_end; ++ditr) {
        int64_t d = ditr.getValue().get();
        dest.push_back(d);
    }

    // copy eprop list
    if (use_eprop_) {
        auto eitr = eprop_.begin(dest_start);
        for ( ; eitr && (int64_t)eitr.getLptr() < dest_end; ++eitr) {
            int64_t e = eitr.getValue().get();
            eprop.push_back(e);
        }
    }
}

void GraphPartition::getPagedArrayIds(std::vector<uint32_t>& pa_ids) const {
    pa_ids.push_back(src_.getId());
    pa_ids.push_back(dest_.getId());
    pa_ids.push_back(eprop_.getId());
}

void GraphPartition::serialize(boost::archive::binary_oarchive& oa) const {
    uint32_t src_id = src_.getId();
    uint32_t dest_id = dest_.getId();
    uint32_t eprop_id = eprop_.getId();

    oa << src_id << dest_id << eprop_id;
    oa << use_eprop_ << id_ << size_ << min_ << max_;
}

size_t GraphPartition::getMemoryUsage() {
    size_t total = src_.getMemoryUsage()
        + dest_.getMemoryUsage() + eprop_.getMemoryUsage();

    total += sizeof(this);
    total -= (3 * sizeof(IntPA));

    return total;
}

/**
 * GraphVPPartition
 */
void GraphVPPartition::truncate() {
    for (auto p : partitions_) {
        delete p;
    }
    partitions_.clear();
}

size_t GraphVPPartition::adjacentList(int64_t src, LongVec& dest) {
    int32_t pid = findPartitionId(src);
    if (pid < 0) return 0;

    return partitions_[pid]->adjacentList(src, dest);
}

size_t GraphVPPartition::adjacentList(
        int64_t src, LongVec& dest, LongVec& eprop) {
    int32_t pid = findPartitionId(src);
    if (pid < 0) return 0;

    return partitions_[pid]->adjacentList(src, dest, eprop);
}

void GraphVPPartition::addPartition(int32_t pos, GraphPartition* partition) {
    partitions_.insert(partitions_.begin() + pos, partition);
    size_ += partition->size();
    partition->setId(pos);
    if (min_ > partition->getMin()) min_ = partition->getMin();
    if (max_ < partition->getMax()) max_ = partition->getMax();
}

void GraphVPPartition::removePartition(int32_t pos) {
    auto itr = partitions_.begin() + pos;
    GraphPartition* partition = *itr;

    size_ -= partition->size();
    partitions_.erase(itr);
    delete partition;
}

GraphPartition* GraphVPPartition::getPartition(int id) {
    return partitions_[id];
}

int64_t GraphVPPartition::getMin(int32_t id) {
    if (id == -1) return min_;
    return partitions_[id]->getMin();
}

int64_t GraphVPPartition::getMax(int32_t id) {
    if (id == -1) return max_;
    return partitions_[id]->getMax();
}

void GraphVPPartition::getPagedArrayIds(std::vector<uint32_t>& pa_ids) const {
    for (auto p : partitions_) {
        p->getPagedArrayIds(pa_ids);
    }
}

void GraphVPPartition::serialize(boost::archive::binary_oarchive& oa) const {
    oa << min_ << max_ << size_;
    size_t num_partition = partitions_.size();
    oa << num_partition;
    for (auto p : partitions_) {
        p->serialize(oa);
    }
}

bool GraphVPPartition::deserialize(boost::archive::binary_iarchive& ia) {
    ia >> min_ >> max_ >> size_;
    size_t num_partition;
    ia >> num_partition;
    for (unsigned i = 0; i < num_partition; i++) {
        uint32_t src_id, dest_id, eprop_id;
        bool use_eprop;
        int32_t id;
        int64_t size, min, max;

        ia >> src_id >> dest_id >> eprop_id;
        ia >> use_eprop >> id >> size >> min >> max;

        GraphPartition* gp = new GraphPartition(src_id, dest_id, eprop_id,
                                use_eprop, id, size, min, max);
        partitions_.push_back(gp);
    }

    return true;
}

void GraphVPPartition::orderCheck() {
    if (partitions_.size() == 0) return;

    size_t size = 0;
    size += partitions_[0]->size();
    if (min_ != partitions_[0]->min_) {
        std::cout << "[ERROR] global min range error" << std::endl;
    }

    if (partitions_[0]->getId() != 0) {
        std::cout << "[ERROR] partition id error" << std::endl;
    }

    for (unsigned i = 1; i < partitions_.size(); i++) {
        if (partitions_[i]->getId() != (int)i) {
            std::cout << "[ERROR] partition id error" << std::endl;
        }

        if (partitions_[i-1]->max_ > partitions_[i]->min_) {
            std::cout << "[ERROR] main partition order error" << std::endl;
        }
        size += partitions_[i]->size();
    }

    if (max_ != partitions_.back()->max_) {
        std::cout << "[ERROR] global max range error" << std::endl;
    }

    if (size != size_) {
        std::cout << "[ERROR] graph main size error" << std::endl;
    }
}

int32_t GraphVPPartition::findPartitionId(int64_t id) {
    if (id < min_) return -1;
    if (id >= max_) return -2;
    int32_t l = 0;
    int32_t h = (int32_t)partitions_.size();
    if (h == 0) return -1;
    while (h > l) {
        size_t m = (h + l) / 2;
        if (id < getMax(m))
            h = m;
        else
            l = m + 1;
    }
    if (l != (int32_t)partitions_.size()) return l;
    return -1;
}

/**
 * GraphMain::iterator
 */
GraphMain::iterator::iterator(GraphMain& main)
    : iterator(main.getAllPartitions()) {}

GraphMain::iterator::iterator(std::vector<GraphPartition*> partitions)
        : partition_idx_(0), partitions_(partitions), is_ended_(true) {
    while (partition_idx_ < (int)partitions_.size() && is_ended_) {
        partition_itr_ = GraphPartition::iterator(
                            partitions_[partition_idx_]);
        is_ended_ = partition_itr_.isEnded();
        if (is_ended_) ++partition_idx_;
    }
}

int64_t GraphMain::iterator::getSrc() {
    return partition_itr_.getSrc();
}

int64_t GraphMain::iterator::getDest() {
    return partition_itr_.getDest();
}

int64_t GraphMain::iterator::getEprop() {
    return partition_itr_.getEprop();
}

LongVec GraphMain::iterator::getDests() {
    return partition_itr_.getDests();
}

LongVec GraphMain::iterator::getEprops() {
    return partition_itr_.getEprops();
}

GraphMain::iterator& GraphMain::iterator::operator++() {
    if (partition_itr_) {
        ++partition_itr_;
    }

    proceed();
    return *this;
}

void GraphMain::iterator::nextList(bool skip_empty) {
    if (partition_itr_) {
        partition_itr_.nextList(skip_empty);
    }

    proceed();
}

void GraphMain::iterator::proceed() {
    if (partition_itr_.isEnded()) {
        while (++partition_idx_ < (int)partitions_.size()) {
            partition_itr_ = GraphPartition::iterator(
                                partitions_[partition_idx_]);
            if (!partition_itr_.isEnded()) break;
        }
        is_ended_ = partition_itr_.isEnded();
    }
}

/**
 * GraphMain
 */
GraphMain::GraphMain(int numvp) {
    for (int i = 0; i < numvp; i++) {
        vp_partitions_.push_back(GraphVPPartition());
    }
}

void GraphMain::truncate() {
    for (auto& vp_partition : vp_partitions_) {
        vp_partition.truncate();
    }
}

void GraphMain::addNewVPPartition() {
    vp_partitions_.push_back(GraphVPPartition());
}

size_t GraphMain::adjacentList(int64_t src, LongVec& dest) {
    int32_t vpid = src >> 32;
    return vp_partitions_[vpid].adjacentList(src, dest);
}

size_t GraphMain::adjacentList(int64_t src, LongVec& dest, LongVec& eprop) {
    int32_t vpid = src >> 32;
    return vp_partitions_[vpid].adjacentList(src, dest, eprop);
}

void GraphMain::addPartition(
        int32_t vpid, int32_t pos, GraphPartition* partition) {
    vp_partitions_[vpid].addPartition(pos, partition);
}

void GraphMain::removePartition(int32_t vpid, int32_t pos) {
    vp_partitions_[vpid].removePartition(pos);
}

GraphPartition* GraphMain::getPartition(int32_t vpid, int id) {
    return vp_partitions_[vpid].getPartition(id);
}

GraphMain::iterator GraphMain::begin() {
    return iterator(*this);
}

std::vector<GraphMain::iterator*> GraphMain::beginMulti(int dop) {
    std::vector<GraphPartition*> partitions = this->getAllPartitions();
    std::vector<GraphMain::iterator*> itrs;

    int num_partitions = partitions.size();
    int division = num_partitions / dop;
    int mod = num_partitions % dop;

    int start, end = 0;
    for (int i = 0; i < dop; i++) {
        start = end;
        end += division;
        if (i < mod) end++;

        std::vector<GraphPartition*> parts(partitions.begin()+start,
                                            partitions.begin()+end);
        itrs.push_back(new iterator(parts));
    }

    return itrs;
}

int64_t GraphMain::getMin(int32_t vpid, int32_t id) {
    return vp_partitions_[vpid].getMin(id);
}

int64_t GraphMain::getMax(int32_t vpid, int32_t id) {
    return vp_partitions_[vpid].getMax(id);
}

std::vector<GraphPartition*> GraphMain::getAllPartitions() {
    std::vector<GraphPartition*> ret;
    for (auto& vp_partition : vp_partitions_) {
        auto partition = vp_partition.getPartitions();
        std::copy(partition.begin(), partition.end(), std::back_inserter(ret));
    }

    return ret;
}

size_t GraphMain::numPartition(int vpid) {
    if (vpid >= 0) {
        return vp_partitions_[vpid].numPartition();
    }

    size_t num = 0;
    for (auto& vp_partition : vp_partitions_) {
        num += vp_partition.numPartition();
    }

    return num;
}

size_t GraphMain::size() {
    size_t size = 0;
    for (auto& vp_partition : vp_partitions_) {
        size += vp_partition.size();
    }

    return size;
}

void GraphMain::getPagedArrayIds(std::vector<uint32_t>& pa_ids) const {
    for (auto& vp_partition : vp_partitions_) {
        vp_partition.getPagedArrayIds(pa_ids);
    }
}

void GraphMain::serialize(boost::archive::binary_oarchive& oa) const {
    size_t numvp = vp_partitions_.size();
    oa << numvp;
    for (auto& vp_partition : vp_partitions_) {
        vp_partition.serialize(oa);
    }
}

bool GraphMain::deserialize(boost::archive::binary_iarchive& ia) {
    size_t numvp;
    ia >> numvp;

    for (unsigned i = 0; i < numvp; i++) {
        addNewVPPartition();
    }

    for (auto& vp_partition : vp_partitions_) {
        vp_partition.deserialize(ia);
    }
    
    return true;
}

size_t GraphMain::getMemoryUsage(std::ostream& os, bool verbose, int level) {
    std::vector<GraphPartition*> partitions = this->getAllPartitions();

    size_t total = 0;
    for (auto p : partitions) {
        total += p->getMemoryUsage();
    }

    return total;
}

void GraphMain::orderCheck() {
    for (auto& vp_partition : vp_partitions_) {
        vp_partition.orderCheck();
    }
}

int32_t GraphMain::findPartitionId(int64_t id) {
    int32_t vpid = id >> 32;
    int32_t pid = vp_partitions_[vpid].findPartitionId(id);
    return pid;
}

}  // namespace storage
