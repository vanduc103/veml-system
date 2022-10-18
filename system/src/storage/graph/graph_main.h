// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef STORAGE_GRAPH_GRAPH_MAIN_H_
#define STORAGE_GRAPH_GRAPH_MAIN_H_

// Related include
#include "storage/graph/graph_iterator.h"
#include "storage/base/paged_array.h"

namespace storage {

class GraphPartition {
  public:
    typedef PagedArray<BigInt_T> IntPA;

    class iterator {
      public:
        iterator() : partition_(nullptr) {}
        iterator(GraphPartition* partition);

        // getter
        int64_t getSrc();
        int64_t getDest();
        int64_t getEprop();

        LongVec getDests();
        LongVec getEprops();

        int64_t getMin() { return partition_->getMin(); }
        int64_t getMax() { return partition_->getMax(); }

        // move iterator
        iterator& operator++();
        void next() { ++(*this); }
        void nextList(bool skip_empty=true);

        operator bool() const { return !is_ended_; }
        bool isEnded() { return is_ended_; }

      private:
        void proceed(bool skip_empty=true);

        GraphPartition* partition_;

        int32_t src_idx_;
        int32_t num_src_;

        int32_t dest_idx_;
        int32_t dest_end_;

        bool is_ended_;
    };

    GraphPartition(bool use_erop);
    GraphPartition(bool use_eprop, int64_t start, int64_t end,
            std::map<int64_t, std::pair<LongList*, LongList*> >& edges);
    GraphPartition(bool use_eprop, int64_t start, int64_t end,
            LongVec& src, LongVec& dest, LongVec& eprop,
            std::map<int64_t, std::pair<LongList*, LongList*> >& edges);
    GraphPartition(uint32_t src_pa_id, uint32_t dest_pa_id,
            uint32_t eprop_pa_id, bool use_eprop, int32_t id,
            int64_t size, int64_t min, int64_t max);

    size_t adjacentList(int64_t src, LongVec& dest);
    size_t adjacentList(int64_t src, LongVec& dest, LongVec& eprop);
    
    void setId(int32_t id) { id_ = id; }
    int32_t getId() { return id_; }

    int64_t getMin() { return min_; }
    int64_t getMax() { return max_; }

    size_t size()    { return size_; }
    size_t size(int64_t src);

    bool isFull(uint32_t add=0) { return add + size_ > NUM_EDGES_FOR_SPLIT; }

    void copyTo(LongVec& src, LongVec& dest, LongVec& eprop,
            int64_t start, int64_t end = std::numeric_limits<int64_t>::max());

    void getPagedArrayIds(std::vector<uint32_t>& pa_ids) const;

    void serialize(boost::archive::binary_oarchive& oa) const;

    size_t getMemoryUsage();

    friend class GraphVPPartition;
    friend class GraphMain;
  protected:
    /**
     * CSR structure
     */
    IntPA src_;
    IntPA dest_;
    IntPA eprop_;

    bool use_eprop_;

    int32_t id_;
    int64_t size_;
    int64_t min_;
    int64_t max_;
};

class GraphVPPartition {
  public:
    GraphVPPartition()
        : min_(std::numeric_limits<int64_t>::max()),
        max_(std::numeric_limits<int64_t>::min()), size_(0) {}

    void truncate();

    size_t adjacentList(int64_t src, LongVec& dest);
    size_t adjacentList(int64_t src, LongVec& dest, LongVec& eprop);

    void addPartition(int32_t pos, GraphPartition* partition);
    void removePartition(int32_t pos);

    GraphPartition* getPartition(int id);

    int64_t getMin(int32_t id=-1);
    int64_t getMax(int32_t id=-1);

    std::vector<GraphPartition*> getPartitions() { return partitions_; }
    size_t numPartition() { return partitions_.size(); }
    size_t size() { return size_; }
    
    void getPagedArrayIds(std::vector<uint32_t>& pa_ids) const;

    /**
     * @brief serialize graph
     */
    void serialize(boost::archive::binary_oarchive& oa) const;
    bool deserialize(boost::archive::binary_iarchive& ia);

    /**
     * only use for debug/test
     */
    void orderCheck();

    friend class GraphMain;
  protected:
    int32_t findPartitionId(int64_t id);

    std::vector<GraphPartition*> partitions_;
    int64_t min_;
    int64_t max_;
    size_t size_;
};

class GraphMain {
  public:
    class iterator : public GraphIterator {
      public:
        iterator(GraphMain& main);
        iterator(std::vector<GraphPartition*> partitions);

        // getter
        int64_t getSrc();
        int64_t getDest();
        int64_t getEprop();

        LongVec getDests();
        LongVec getEprops();

        int64_t getMin() { 
          int64_t min = -1;
          if(!partitions_.empty())
            min = partitions_[0]->getMin(); 
          return min;
        }
        int64_t getMax() {
          int64_t max = -1;
          if(!partitions_.empty())
            max = partitions_[partitions_.size()-1]->getMax(); 
          return max;
        }

        // move iterator
        iterator& operator++();
        void next() { ++(*this); }
        void nextList(bool skip_empty);

        operator bool() const { return !is_ended_; }
        bool isEnded() { return is_ended_; }

      private:
        void proceed();

        int32_t partition_idx_;
        std::vector<GraphPartition*> partitions_;
        GraphPartition::iterator partition_itr_;

        bool is_ended_;
    };

    GraphMain(int numvp=0);
    void truncate();
    void addNewVPPartition();

    size_t adjacentList(int64_t src, LongVec& dest);
    size_t adjacentList(int64_t src, LongVec& dest, LongVec& eprop);

    void addPartition(int32_t vpid,
            int32_t pos, GraphPartition* partition);
    void removePartition(int32_t vpid, int32_t pos);

    GraphPartition* getPartition(int32_t vpid, int id);

    iterator begin();
    std::vector<iterator*> beginMulti(int dop);

    int64_t getMin(int32_t vpid, int32_t id=-1);
    int64_t getMax(int32_t vpid, int32_t id=-1);

    std::vector<GraphPartition*> getAllPartitions();
    size_t numPartition(int32_t vpid = -1);
    size_t size(); 

    void getPagedArrayIds(std::vector<uint32_t>& pa_ids) const;

    /**
     * @brief serialize graph
     */
    void serialize(boost::archive::binary_oarchive& oa) const;
    bool deserialize(boost::archive::binary_iarchive& ia);

    size_t getMemoryUsage(std::ostream& os, bool verbose, int level);

    /**
     * only use for debug/test
     */
    void orderCheck();

    friend class GraphDelta;
  protected:
    int32_t findPartitionId(int64_t id);

    std::vector<GraphVPPartition> vp_partitions_;
};

}  // namespace storage

#endif  // STORAGE_GRAPH_GRAPH_MAIN_H_
