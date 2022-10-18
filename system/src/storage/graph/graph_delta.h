// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef STORAGE_GRAPH_GRAPH_DELTA_H_
#define STORAGE_GRAPH_GRAPH_DELTA_H_

// Related include
#include "storage/graph/graph_main.h"
#include "storage/graph/graph_iterator.h"

// Project include
#include "storage/relation/row_table.h"

#include <utility>

namespace storage {

class GraphDelta {
  public:
    class iterator : public GraphIterator {
      public:
        iterator(GraphDelta* delta, bool eprop);
        iterator(GraphDelta* delta, bool eprop, int start, int end);

        // getter
        int64_t getSrc();
        int64_t getDest();
        int64_t getEprop();
        LongVec getDests();
        LongVec getEprops();
        int32_t getBucketIdx();
        LogicalPtr getLptr();

        LongList& getDestList();
        LongList& getEpropList();

        LongList* getDestListPtr();
        LongList* getEpropListPtr();

        EvalVec getRecord();
        int64_t getMin() { return -1; }
        int64_t getMax() { return -1; }

        size_t size();

        // move iterator
        iterator& operator++();
        void next() { ++(*this); }
        void nextList(bool skip_empty=true);
        operator bool() const { return !is_ended_; }
        bool isEnded() { return is_ended_; }

      private:
        GraphDelta* delta_;

        int64_t src_;
        LongList* dest_;
        LongList* eprop_;

        int32_t list_idx_;
        int32_t list_size_;

        int32_t bucket_idx_;
        int32_t end_idx_;
        RowTable::iterator bucket_itr_;

        bool is_ended_;
        bool use_eprop_;
    };  // GraphDelta::iterator

    /*
     * name: graph name
     * id: delta id
     * prop: flag for using edge property
     * init: initial hash size
     */
    GraphDelta(std::string name, uint32_t id, bool eprop,
            uint32_t init = GRAPH_DELTA_INIT_SIZE);

    GraphDelta(std::string name, uint32_t id, bool eprop,
            uint32_t hash_divisor, std::vector<uint32_t>& hash_tid);

    ~GraphDelta() {
        releaseBuckets();
    }
    void releaseBuckets();

    /*
     * dml interface
     */
    bool insertEdge(int64_t src, int64_t dest, int64_t eprop = -1);
    void insertList(EvalVec& list);
    void remove(int64_t src);
    void remove(int32_t hashid, LongVec& srcids);

    /**
     * search
     */
    void adjacentList(int64_t src, LongVec& dest);
    void adjacentList(int64_t src, LongVec& dest, LongVec& eprop);

    /**
     * return the number of edges in delta
     */
    size_t size();
    
    /**
     * @brief serialize graph
     */
    void serialize(boost::archive::binary_oarchive& oa) const;

    /**
     * get graph delta iterator
     */
    iterator begin() { return iterator(this, eprop_); }
    std::vector<iterator*> beginMulti(int dop);

    /**
     * get memory usage
     */
    size_t getMemoryUsage(std::ostream& os, bool verbose, int level);

  protected:
    EvalVec getList(int64_t vid);

    uint32_t getHashSize()          { return hash_divisor_; }
    uint32_t getHashVal(int64_t id) { return id % hash_divisor_; }

    struct Bucket {
        Bucket(RowTable* bucket, uint32_t id)
            : bucket_(bucket), id_(id) {} 
        RowTable* bucket_;
        uint32_t  id_;
    };

    /**
     * multi-layer architecture
     */
    class MergeInfo {
      public:
        MergeInfo(unsigned numvp)
            : map_(std::vector<PartitionMap>(numvp, PartitionMap())) {}

        struct Partition {
            Partition(int vpid) : vpid_(vpid), qualified_(false), size_(0),
                min_(std::numeric_limits<int64_t>::max()),
                max_(std::numeric_limits<int64_t>::min()) {}

            // id of vertex property
            int32_t vpid_;

            // edge-lists to be moved to main layer
            // <srcid, <dests, eprop>>
            std::map<int64_t, std::pair<LongList*, LongList*> > edges_;

            // flag for whether the partition is large enough to merge
            bool qualified_;

            // size of merge info
            size_t size_;

            // local min/max
            int64_t min_;
            int64_t max_;
        };

        size_t size();
        size_t size_to_deletes();

        // <partition_id, merge_info>
        typedef std::map<int32_t, Partition> PartitionMap;

        // merge information for each partition
        // id of vector means vertex property-id
        std::vector<PartitionMap> map_;

        // src_ids to deletes
        // <hash_id, src_id>
        std::map<int32_t, LongVec> deletes_;
    };

    void collectMerge(LongVec& min, LongVec& max,
            GraphMain& main, MergeInfo& merge_info, size_t least);

    std::string name_;
    uint32_t    id_; 
    bool        eprop_;

    uint32_t            hash_divisor_;
    std::vector<Bucket> hash_table_;

    friend class Graph;
    friend class iterator;
};

}  // namespace storage

#endif  // STORAGE_GRAPH_GRAPH_DELTA_H_
