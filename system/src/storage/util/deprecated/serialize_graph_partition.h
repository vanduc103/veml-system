// Copyright 2021 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef STORAGE_UTIL_SERIALIZE_GRAPH_PARTITION_H_
#define STORAGE_UTIL_SERIALIZE_GRAPH_PARTITION_H_

// Project include
#include "storage/graph/graph.h"
#include "persistency/archive_manager.h"
#include "storage/util/graph_distribute_base.h"

// Other include
//#include <boost/asio.hpp>
//#include <boost/asio/io_service.hpp>
//#include <boost/asio/ip/tcp.hpp>

namespace storage{

class SerializeGraphPartition : public GraphDistributeBase{
  public:
    class DistVertex : public RTIThread {
      public:
        DistVertex(std::string name,
                std::vector<Table*>& vprop, std::set<int64_t>& vertex,
                std::unique_ptr<dan::DANInterface::Stub>& connection)
            : name_(name), vprop_(vprop),
            vertex_(vertex), connection_(connection) {}

        void* run();

      private:
        std::string name_;
        std::vector<Table*>& vprop_;
        std::set<int64_t> vertex_;
        std::unique_ptr<dan::DANInterface::Stub>& connection_;
    };

    class DistEdge : public RTIThread {
      public:
        DistEdge(std::string name, std::string partition_gname,
                std::vector<Table*>& vprop, std::vector<int32_t>& vfid,
                Table* eprop, bool use_eprop, bool use_incoming,
                bool ignore_in, Graph::EdgeMap& edges,
                std::set<int64_t>& vertex,
                std::unique_ptr<dan::DANInterface::Stub>& connection)
            : name_(name), partition_gname_(partition_gname),
            vprop_(vprop), vfid_(vfid), eprop_(eprop),
            use_eprop_(use_eprop), use_incoming_(use_incoming),
            ignore_in_(ignore_in), edges_(edges),
            vertex_(vertex), connection_(connection){}

        void* run();

      private:
        std::string name_;
        std::string partition_gname_;
        std::vector<Table*>& vprop_;
        std::vector<int32_t>& vfid_;
        Table* eprop_;
        bool use_eprop_;
        bool use_incoming_;
        bool ignore_in_;
        Graph::EdgeMap& edges_;
        std::set<int64_t> vertex_;
        std::unique_ptr<dan::DANInterface::Stub>& connection_;
    };

    /**
     * @brief create new graph distribute strategy using boost serialization
     * @param std::string gname: name of graph 
     * @param std::vector vprop: reference to vertex_prop_ in graph
     * @param std::vector vertex_fid: reference to vertex_fid_ in graph
     * @param Graph::Partition partition:
     *              reference to a list of partitions defined in a caller func
     * @param Table edge_prop: reference to edge_prop_ table in a graph
     * @param bool use_eprop: similar to use_eprop_ property in a graph
     * @param bool use_incoming: whether to store data in delta_in_,
     *                          similar to use_incoming_ property in a graph
     * @param bool std::unique_ptr<dan::DANInterface::Stub> connection:
     *                          grpc connection to target node
     * @param std::string path: path to output folder
     */
    SerializeGraphPartition(std::string gname,
            std::vector<Table*>& vprop, std::vector<int32_t>& vertex_fid,
            Graph::Partition& partition, Table* edge_prop,
            bool use_eprop, bool use_incoming, bool ignore_in,
            std::unique_ptr<dan::DANInterface::Stub>& connection,
            unsigned int part_idx);
                            
    ~SerializeGraphPartition();

    Graph* createGraph(Graph* orig_graph);

    // run workers
    void distributeVertices();
    void distributeEdges();
    void waitVDistribute();
    void waitEDistribute();
    
    static void distributeGraph(std::string& gname,
            std::string& partition_gname,
            std::unique_ptr<dan::DANInterface::Stub>& connection);

  protected:
    std::unique_ptr<dan::DANInterface::Stub>& connection_;
    DistVertex* vertex_worker_;
    DistEdge* edge_worker_;
    unsigned int part_idx_;
};

}  // namespace storage

#endif  // STORAGE_UTIL_SERIALIZE_GRAPH_PARTITION_H_
