// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef STORAGE_GRAPH_GRAPH_H_
#define STORAGE_GRAPH_GRAPH_H_

// Related include
#include "storage/graph/graph_delta.h"
//#include "storage/graph/graph_training_data.h"
#include "storage/graph/graph_main.h"
#include "storage/graph/graph_iterator.h"

// Project include
#include "common/def_const.h"
#include "storage/catalog.h"
#include "storage/relation/table.h"
#include "storage/util/partition_func.h"
#include "concurrency/rti_thread.h"
#include "concurrency/mutex_lock.h"
#include "storage/graph/subgraph.h"

#include "util/timer.h"

namespace dan {
    class GraphBinary;
}

namespace storage {

class RowTable;

class Graph {
  public:
    typedef std::unordered_map<int64_t, std::pair<LongVec, LongVec> > EdgeMap;

    enum PartitionType {
        EDGE_CUT_HASH,
        EDGE_CUT_HASH_PROP,
        VERTEX_CUT_RANGE_SRC,
        VERTEX_CUT_RANGE_DST,
        VERTEX_CUT_HASH_SRC,
        VERTEX_CUT_HASH_DST,
        NUM_PARTITION_TYPE,
    };

    struct Partition {
        Partition() : num_edges(0) {}
        EdgeMap edges;
        LongSet src_set;
        LongSet dest_set;
        uint64_t num_edges;
    };

    class iterator {
      public:
        iterator(Graph* graph, bool use_eprop, bool incoming);
        iterator(Graph* graph, bool use_eprop, bool incoming,
                    GraphIterator* delta_itr, GraphIterator* main_itr);

        // getter
        int64_t getSrcIdx();
        int64_t getDestIdx();
        int64_t getEpropIdx();

        LongVec getDestList();
        LongVec getEpropList();

        EvalValue getSrcKey();
        EvalValue getDestKey();

        EvalVec getSrc(IntVec fids);
        EvalVec getDest(IntVec fids);
        EvalVec getEprop();

        int64_t getMin() { return itr_[itr_idx_]->getMin(); }
        int64_t getMax() { return itr_[itr_idx_]->getMax(); }

        // move iterator
        iterator& operator++();
        void next() { ++(*this); }
        void nextList(bool skip_empty=true);
        operator bool() const { return !is_ended_; }

      private:
        Graph* graph_;
        std::vector<GraphIterator*> itr_;
        int32_t itr_idx_;

        bool is_ended_;
        bool use_eprop_;
        bool incoming_;
    };

    /**
     * @brief create new graph
     * @param std::string name: name of graph 
     * @param std::string vpname: name of vertex property
     * @param CreateFieldVec vpfields: fields of vertex property
     * @param int32_t vpfid: field id of primary key in vertex_property
     * @param Table::Type vptype: table type of vertex property
     * @param CreateFieldVec epfields: fields of edge property
     * @param Table::Type eptype: table type of edge property
     * @param bool use_incoming: whether using incoming edge
     * @param bool merge_bot_on: if set, periodically merge delta to main
     * @param bool is_partition: whther this is a partition of a graph
     * @param int32_t part_type: type of partitioning
     * @param StrVec nodes: address of partitions
     */
    static Graph* create(const std::string& name,
            // config about vertex property table
            std::string vpname, CreateFieldVec vpfields,
            int32_t vpfid, Table::Type vptype = Table::ROW, 
            // config about edge property table
            CreateFieldVec epfields = {}, Table::Type eptype = Table::ROW, 
            // cofig about others
            bool use_incoming = false, bool merge_bot_on = false,
            bool is_partition = false, int32_t part_type = NUM_PARTITION_TYPE,
            StrVec nodes={});

    /**
     * @brief deallocate graph
     */
    virtual ~Graph();
    
    /**
     * @brief truncate graph
     */
    void truncate();

    /**
     * @brief define a new vertex type
     * @param std::string vpname: name of vertex property
     * @param CreateFieldVec& vpfields: fields of vertex property
     * @param int32_t vpfid: field id of primary key in vertex_property
     * @param Table::Type vptype: table type of vertex property
     */
    bool defineVertex(std::string vpname, CreateFieldVec& vpfields,
            int32_t vpfid = 0, Table::Type vptype = Table::ROW);

    /**
     * @brief check whether a vertex property exists
     * @param std::string vpname: name of vertex property
     */
    bool vertexPropExists(std::string vpname);

    /**
     * @brief get id of a vertex property
     * @param std::string vpname: name of vertex property
     */
    int32_t findVertexPropId(std::string vpname);

    /**
     * @brief import vertex from csv file
     * @param std::string vpname: name of vertex property
     * @param std::string csv: path of csv file
     * @param std::string delim: delimiter
     * @param bool header: if true, file contains header row
     */
    bool importVertex(std::string vpname,
            std::string csv, std::string delim, bool header);

    /**
     * @brief insert a vertex with its property vector
     *  1. insert vertex to vprop table
     *  2. if it is failed(already exists), get id from vprop table
     * @param EvalVec& values: vertex property
     * @param int32_t vpid: type of vertex
     */
    int64_t insertVertex(EvalVec& values, int32_t vpid = 0);
    bool insertVertexBatch(std::vector<EvalVec>& values,
            IntVec& vpids, int32_t vpid = 0, int32_t dop = 8);

    /**
     * @brief import edge from csv file
     * @param std::string csv: path of csv file
     * @param int32_t src_vpid: type of source vertex
     * @param int32_t dst_vpid: type of destination vertex
     * @param std::string delim: delimiter
     * @param bool header: if true, file contains header row
     */
    bool importEdge(std::string csv, int32_t src_vpid, int32_t dst_vpid,
            std::string delim, bool header, int batch, int dop);

    /**
     * @brief insert edge to graph
     * @param int64_t src: id of source vertex
     * @param int64_t dest: id of destination vertex 
     * @param EvalVec& eprop: property values of edge
     * @param bool ignore_in: if set, do not insert incoming edge
     * @param bool only_in: if set, do not insert outgoing edge
     */
    bool insertEdge(int64_t src_id, int64_t dest_id,
                EvalVec& eprop, bool ignore_in, bool only_in);
    
    /**
     * @brief insert edge to graph
     * @param EvalValue src: primary key value of source vertex
     * @param EvalValue dest: primary key value of destination vertex
     * @param EvalVec& eprop: property values of edge
     * @param int32_t src_vpid: type of source vertex
     * @param int32_t dest_vpid: type of destination vertex
     * @param bool ignore_in: if set, do not insert incoming edge
     * @param bool only_in: if set, do not insert outgoing edge
     */
    bool insertEdge(EvalValue src, EvalValue dest, EvalVec& eprop,
            int32_t src_vpid = 0, int32_t dest_vpid = 0,
            bool ignore_in = false, bool only_in = false);

    /**
     * @brief insert edge to graph
     * @param EvalVec& src: property of source vertex
     * @param EvalVec& dest: property of destination vertex
     * @param EvalVec& eprop: property values of edge
     * @param int32_t src_vpid: type of source vertex
     * @param int32_t dest_vpid: type of destination vertex
     */
    bool insertEdge(EvalVec& src, EvalVec& dest, EvalVec& eprop,
            int32_t src_vpid = 0, int32_t dest_vpid = 0);

    /**
     * @brief insert edge batch to graph
     * @param EvalVec& src: list of source primary key
     * @param EvalVec& dest: list of destinatin primary key
     * @param std::vector<EvalVec>& eprop: list of edge property
     * @param IntVec& src_vpid: list of sourve vertex type
     * @param IntVec& dest_vpid: list of destination vertex type
     * @param int dop: degree of parallelism
     */
    bool insertEdgeBatch(EvalVec& src, EvalVec& dest,
            std::vector<EvalVec>& erop,
            IntVec& src_vpid, IntVec& dest_vpid,
            bool ignore_in = false, bool only_in = true, int dop = 16);

    /**
     * @brief get the number of vertex
     */
    size_t getNumVertex();

    /**
     * @brief get vertex of graph
     * @param EvalValue& value: primary key value of vertex
     * @param IntVec fids: list of field-ids to get
     * @param int32_t vpid: type of vertex
     */
    EvalVec getVertex(EvalValue& vkey,
                int32_t vpid, int64_t vid, IntVec& fids);

    /**
     * @brief get all vertex in graph
     * @param EvalVec& vkey: list of vertex key
     * @param IntVec& vpid: list of vertex property id
     * @param std::vector<EvalVec>& vprop: list of vertex property
     */
    void getVertexList(EvalVec& vkey, IntVec& vpid,
            std::vector<EvalVec>& vprop, IntVec fids={});

    /**
     * @brief check whether a vertex exists
     * @param EvalValue& vkey: key value of vertex
     * @param int32_t vpid: id of vertex property
     */
    bool vertexExists(EvalValue& vkey, int32_t vpid);

    /**
     * @brief check a vertex-id is valid
     */
    bool validVertexId(int64_t vid);

    /**
     * @brief get the number of edges
     */
    size_t getNumEdges();

    /**
     * @brief get neighboring vertices
     * @param EvalValue src: primary key value of source vertex
     * @param EvalVec& dest: list of  destination vertex's primary key
     * @bool incoming: if set, collect incoming edges
     * @int32_t vpid: type of source vertex
     */
    void adjacentList(EvalValue src,
            EvalVec& dest, bool incoming, int32_t vpid);

    /**
     * @brief get neighboring vertices
     * @param EvalValue src: primary key value of source vertex
     * @param std::vector<EvalVec>& dest: list of destination vertex property
     * @param IntVec& fids: list of field-ids to get
     * @param EvalVec& eprop: list of edge property
     * @bool incoming: if set, collect incoming edges
     * @int32_t vpid: type of source vertex
     */
    void adjacentList(EvalValue src, std::vector<EvalVec>& dest,
            IntVec& fids, EvalVec& eprop, bool incoming, int32_t vpid);

    /**
     * @brief get all edges in graph
     * @param EvalVec& src: list of source vertex key
     * @param EvalVec& dest: list of destination vertex key
     */
    void getEdgeList(EvalVec& src, EvalVec& dest);

    /**
     * @brief generate graph iterator
     */
    iterator begin(bool incoming=false);
    std::vector<iterator> beginMulti(int dop, bool incoming=false);

    /**
     * bfs
     */
    void bfs(int64_t src, int64_t dest);

    /**
     * dfs
     */
    void dfs(int64_t src, int64_t dest);

    /**
     * graph algorithm - triangle counting
    */
    
    /**
    * community algorithms
    */
    uint64_t triangleCounting(int64_t from_vertex, int64_t to_vertex);

    /**
     * graph algorithm - triangle counting for undirected graph
    */

    uint64_t triangleCounting(int64_t from_vertex, int64_t to_vertex, bool is_directed);

    /**
     * conventional algorithm section
     * graph algorithm - pagerank
     * 
     */
    void pagerank(double damping_factor,
            int64_t num_iteration, int64_t num_vertex, double* pagerank);

    /**
     * @brief return a path from random walk w/ restart given a seed_node real id
     * @param uint32_t k: number of iterations
     * @param EvalValue& seed_node: seeding node with its real id
     * @param int32_t vpid: vertex property id
     * @param IntVec& paths: returned array with lptr_ids of path
     */ 
    void randomWalkR(uint32_t k, EvalValue& seed_node,
            int32_t vpid, std::unordered_set<int64_t>& path, double rp=0.2);

    /**
     * @brief return a path from random walk w/ restart given a seed_node real id
     * @param uint32_t k: number of iterations
     * @param int64_t seed_node: seeding node with its lptr
     * @param int32_t vpid: vertex property id
     * @param IntVec& paths: returned array with lptr_ids of path
     */ 
    void randomWalkR(uint32_t k, int64_t seed_id,
            std::unordered_set<int64_t>& path, double rp=0.2);
    
    /**
     * @brief return a path from random walk w/ restart given a seed_node real id
     * @param uint32_t k: number of iterations
     * @param EvalValue& seed_node: seeding node with its real id
     * @param EvalVec& paths: returned array with real ids of path
     */ 
    void randomWalkR(uint32_t k, EvalValue& seed_node,
            int32_t vpid, EvalVec& path, double rp=0.2);

    /**
     * @brief return a subgraph using multiple randomWalk steps
     * @param uint32_t k: number of random walk iterations
     */
    SubGraph* randomWalkSample(uint32_t k, std::vector<IntVec>& vfids,
            IntVec& efids, double rp=0.2, uint32_t first_k=50, uint32_t dop=4);

    /**
     * @brief return a subgraph wherein all dest nodes of edges are in the seeding nodes. 
     * @param LongVec seeding_nodes: a list of seeding node lptr ids 
     * @param IntVec& efids: extracted columns of e properties
     */
    void sampleNodeSubgraph(std::unordered_set<int64_t> &seeding_nodes,
                        IntVec& efids, 
                        std::map<int64_t, int64_t>& newv2oldv, 
                        std::unordered_map<int64_t, LongVec>& src,
                        std::unordered_map<int64_t, LongVec>& dest, 
                        EvalVec& edata,
                        std::unordered_map<int32_t, int64_t>& num_nodes);
    
    void sampleNodeSubgraph(std::unordered_set<int64_t> &seeding_nodes, IntVec& efids, 
                        std::map<int64_t, int64_t>& newv2oldv, 
                        std::unordered_map<int64_t, LongVec>& src, std::unordered_map<int64_t, LongVec>& dest, 
                        EvalVec& edata, std::unordered_map<int32_t, int64_t>& num_nodes, uint32_t dop);
    
    /**
     * @brief return a subgraph wherein all dest nodes of edges are in the seeding nodes. 
     * @param LongVec seeding_nodes: a list of seeding nodes 
     * @param IntVec& vfids: extracted columns of v properties
     * @param IntVec& efids: extracted columns of e properties
     */
    SubGraph* sampleNodeSubgraph(LongVec &seeding_nodes,                        
                        IntVec& vfids,
                        IntVec& efids,
                        uint32_t dop = 1);

     /**
     * @brief return a subgraph wherein all dest nodes of edges are in the seeding nodes. 
     * @param LongVec seeding_nodes: a list of seeding nodes 
     * @param IntVec& vfids: extracted columns of v properties
     * @param IntVec& efids: extracted columns of e properties
     */
    SubGraph* sampleHeteroSubGraph(LongVec &seeding_nodes,                        
                        std::vector<IntVec>& vfids,
                        IntVec& efids,
                        uint32_t dop = 1);

    SubGraph* sample_neighbors(LongVec &nodes,
                        IntVec& vfids, 
                        IntVec& efids,
                        int32_t fanout,
                        bool incoming = true);

    /**
     * multi-layer architecture
     */
    void startMergeBot();
    void stopMergeBot();

    /**
     * @brief serialize graph to binary
     */
    void serialize(boost::archive::binary_oarchive& oa) const;

    /**
     * @brief deserialize graph from binary
     */
    static Graph* deserialize(boost::archive::binary_iarchive& ia);

    /**
     * @brief partition graph
     * @param std::string type: type of partitioning function
     * @param StrVec& node_addrs: ip address of nodes
     */
    bool partition(std::string type, StrVec& nodes, int32_t dop);

    /**
     * @brief generate graph partitions in local
     * @param std::string type: type of partition
     * @param int num: the number of partitions to create
     * @param int dop: degree of parallelism
     */
    std::vector<Graph*> generatePartition(std::string type, int num, int dop);

    /**
     * get name of the graph
     */
    std::string getName() const { return name_; }

    /**
     * set name of the graph
     */
    void setName(std::string name) { name_ = name; }

    /**
     * get list of vertex property names
     */
    StrVec getVPName() const;

    /**
     * get list of vertex property fields
     */
    std::vector<StrVec> getVPFields() const;

    /**
     * get list of edge property fields
     */
    StrVec getEPFields() const;

    /**
     * check this graph use incoming edge
     */
    bool useIncoming() const { return use_incoming_; }

    /**
     * get id of the graph
     */
    uint32_t getId() const { return id_; }

    /**
     * @brief check the graph is partitioned
     */
    bool isPartitioned() const {
        return (!is_partition_) && part_nodes_.size() != 0;
    }
    
    /**
     * @brief return address of partition nodes
     */
    StrVec getPartNodes() const { return part_nodes_; }

    /**
     * @brief find partition node for input src vertex
     */
    StrVec findNode(EvalValue& src, bool incoming=false);

    /**
     * @brief get ids of all paged_array in main
     */
    void getPagedArrayIds(std::vector<uint32_t>& pa_ids) const;
    
    /**
     * only use for debug/test
     */
    void mergeManually(size_t least=NUM_EDGES_FOR_MERGE);
    void mergeManually(size_t least, size_t num_split);
    size_t getMemoryUsage(std::ostream& os, bool verbose=false, int level=0);
    void mainOrder();

    int64_t getLastMergeTimestamp() { return last_merge_timestamp_; }
    void setLastMergeTimestamp() { last_merge_timestamp_ = util::Timer::currentTimestamp(); }
    
    friend class MergeBot;
    //friend class TrainingDataManager;

  protected:
    DISALLOW_COMPILER_GENERATED(Graph);
    Graph(const std::string& name,
            std::string vpname, CreateFieldVec vpfields,
            int32_t vpfid, Table::Type vptype, 
            CreateFieldVec epfields, Table::Type eptype, 
            bool use_incoming, bool merge_bot_on,
            bool is_partition, int32_t part_type, StrVec nodes);

    Graph(const std::string& name, uint32_t id,
            std::vector<Table*>& vertex_prop, std::vector<int32_t> vertex_fid,
            Table* edge_prop, bool use_eprop, bool use_incoming,
            GraphDelta* delta, GraphDelta* delta_in, uint32_t delta_id,
            bool merge_bot_on, PartitionType part_type, StrVec& part_nodes,
            uint32_t part_embed, uint64_t part_filter, bool is_partition);

    bool insertEdge(int64_t src_id, int64_t dest_id,
                int64_t eprop_idx, bool ignore_in, bool only_in);

    int64_t getVertexId(EvalValue& value, int32_t vpid);
    EvalValue getVertex(int64_t vertex_id);
    EvalVec getVertex(int64_t vertex_id, IntVec& fids);
    void adjacentList(int64_t src, LongVec& dest, bool incoming);
    void adjacentList(int64_t src,
            LongVec& dest, LongVec& eprop, bool incoming);

    /**
     * multi-layer architecture
     */
    class MergeBot : public RTIThread {
      public:
        MergeBot(Graph* graph) : graph_(graph) {}
        void* run();

      private:
        Graph* graph_;
    };

    class MergeWorker : public RTIThread {
      public:
        MergeWorker(int vpid, int pos, int64_t start, bool use_eprop)
            : vpid_(vpid), pos_(pos), start_(start), end_(-1),
            new_(nullptr), use_eprop_(use_eprop) {}

        void collect(GraphPartition* old);
        void enroll(int64_t src, std::pair<LongList*, LongList*> edge);
        void setEnd(int64_t end) { end_ = end; }

        int64_t min() { return start_; }
        int64_t max() { return end_; }
        int pos()     { return pos_; }

        GraphPartition* get() { return new_; }

        void* run();

      private:
        int vpid_;
        int pos_;
        int64_t start_;
        int64_t end_;

        LongVec old_src_;
        LongVec old_dest_;
        LongVec old_eprop_;

        GraphPartition* new_;
        std::map<int64_t, std::pair<LongList*, LongList*> > edges_;

        bool use_eprop_;
    };

    void mergeDelta(size_t least = NUM_EDGES_FOR_MERGE, size_t num_split = NUM_EDGES_FOR_SPLIT);
    static void gen_merge_worker_front_back(int vpid,
                    int pos, int64_t min, int64_t max,
                    GraphDelta::MergeInfo& merge_info,
                    std::vector<MergeWorker>& workers,
                    bool use_eprop, size_t num_split = NUM_EDGES_FOR_SPLIT);
    static void gen_merge_worker_replace(int vpid,
                    int64_t min, int64_t max, GraphMain& main,
                    GraphDelta::MergeInfo& merge_info,
                    std::vector<MergeWorker>& workers,
                    bool use_eprop, size_t num_split = NUM_EDGES_FOR_SPLIT);
    static void insert_merge_partition(int vpid, GraphMain& main,
                    std::vector<MergeWorker>& workers);

    /**
     * @brief distribe vertex to partition node
     */
    static void distributeVertex(std::string& gname,
                    EvalVec& vprop, int32_t vpid, bool is_replica,
                    std::unique_ptr<dan::DANInterface::Stub>& client);

    static void distributeVertexBatch(std::string& gname,
                    dan::Table* vprop, dan::IntVec* vpids, int32_t vpid,
                    std::unique_ptr<dan::DANInterface::Stub>& client);

    static void distributeEdgeBatch(std::string& gname,
                    dan::Record* src, dan::Record* dest,
                    dan::IntVec* src_vpid, dan::IntVec* dest_vpid,
                    dan::Table* eprops, bool use_incoming, bool only_in, 
                    std::unique_ptr<dan::DANInterface::Stub>& client);

    // partitioning method, assuming #nodes are fixed after initializing
    void edgeCutHash(std::vector<Graph*>& targets, int dop);
    void vertexCut(std::vector<Graph*>& targets, bool ignore_in = true,
            bool range_partition = true, int32_t dop = 1);

    // util func
    void fillFids(IntVec& fids, int32_t vpid);
    bool validVPId(int32_t vpid);

    Table* getVPTable(std::string vpname);
    FieldInfo* getVPFinfo(int32_t vpid);
    FieldInfoVec getEPFinfo();

    // basic variable
    std::string name_;
    uint32_t    id_;
    
    // vertex property variable.
    std::vector<Table*> vertex_prop_;
    std::vector<int32_t> vertex_fid_;

    // edge property variable. 
    Table*      edge_prop_;
    bool        use_eprop_;

    // flag for incoming edge
    bool        use_incoming_;

    // delta variable
    GraphDelta* delta_;
    GraphDelta* delta_in_;
    uint32_t    delta_id_;
    Mutex       mx_insert_;

    // main variable
    GraphMain main_;
    GraphMain main_in_;

    // multi-layer architecture
    MergeBot merge_bot_;
    Mutex mx_merge_;
    std::atomic<bool> is_merging_;
    std::atomic<bool> merge_bot_on_;
    Mutex mx_serializing_;

    // partitioning info
    PartEvalFunc    part_func_;
    PartitionType   part_type_;
    StrVec          part_nodes_;

    uint32_t        part_embed_;
    uint64_t        part_filter_;
    bool            is_partition_;
    int64_t         last_merge_timestamp_;
};

/** 
 * find partition slot for distributing vertex/edges
 * dop: number of threads to distribute
 * partitions: #partition = dop * num_worker_nodes
 * part_id: a block that a vertex belongs to
 */
inline int32_t findVertexPartition(
        std::vector<Graph::Partition>& partitions,
        int32_t dop, int32_t part_id) {
    part_id = part_id * dop;
    size_t num_ver = partitions[part_id].src_set.size();
    if (num_ver > 0) {
        int32_t tem_part_id = part_id;

        // find the slot with smallest number of vertices
        for (int i = 1; i < dop; i++) {
            size_t current_size = partitions[part_id + i].src_set.size();
            if (current_size == 0) {
                tem_part_id = part_id + i;
                break;
            }
            if (current_size < num_ver) {
                tem_part_id = part_id + i;
                num_ver = current_size;
            }
        }
        part_id = tem_part_id;
    }

    return part_id;
}

}  // namespace storage


#endif  // STORAGE_GRAPH_GRAPH_H_
