#ifndef STORAGE_UTIL_GRPC_GRAPH_DISTRIBUTE_H_
#define STORAGE_UTIL_GRPC_GRAPH_DISTRIBUTE_H_

#include "storage/graph/graph.h"
#include "storage/util/graph_distribute_base.h"

#include <grpcpp/grpcpp.h>
#include <grpc/support/log.h>
#include <grpcpp/resource_quota.h>

namespace storage{

class GRPCGraphDistribute : public GraphDistributeBase{
    public:
    
        class DistVertex : public RTIThread {
            public:
                DistVertex(std::string name,
                        std::vector<Table*>& vprop, std::set<int64_t>& vertex,
                        std::unique_ptr<dan::DANInterface::Stub>& client)
                    : name_(name), vprop_(vprop), vertex_(vertex), client_(client){}

                void* run();

            private:
                std::string name_;
                std::vector<Table*>& vprop_;
                std::set<int64_t>& vertex_;
                std::unique_ptr<dan::DANInterface::Stub>& client_;
    
        };

        class DistEdge : public RTIThread {
            public:
                DistEdge(std::string name,
                        std::vector<Table*>& vprop, std::vector<int32_t>& vfid,
                        Table* eprop, bool use_eprop, bool use_incoming,
                        bool ignore_in, Graph::EdgeMap& edges, std::set<int64_t>& vertex,
                        std::unique_ptr<dan::DANInterface::Stub>& client)
                    : name_(name), vprop_(vprop), vfid_(vfid), eprop_(eprop),
                    use_eprop_(use_eprop), use_incoming_(use_incoming),
                    ignore_in_(ignore_in), edges_(edges),
                    vertex_(vertex), client_(client) {}
                void* run();

            private:
                std::string name_;
                std::vector<Table*>& vprop_;
                std::vector<int32_t>& vfid_;
                Table* eprop_;
                bool use_eprop_;
                bool use_incoming_;
                bool ignore_in_;
                Graph::EdgeMap& edges_;
                std::set<int64_t>& vertex_;
                std::unique_ptr<dan::DANInterface::Stub>& client_;
        };

        /**
         * @brief create new graph distribute strategy via grpc
         * @param std::string gname: name of graph 
         * @param std::vector vprop: reference to vertex_prop_ in graph
         * @param std::vector vertex_fid: reference to vertex_fid_ in graph
         * @param Graph::Partition partition: reference to a list of partitions defined in a caller func
         * @param Table edge_prop: reference to edge_prop_ table in a graph
         * @param bool use_eprop: similar to use_eprop_ property in a graph
         * @param bool use_incoming: whether to store data in delta_in_, similar to use_incoming_ property in a graph
         * @param bool ignore_in: whether to skip data storing to delta_in_
         * @param bool std::unique_ptr<dan::DANInterface::Stub> connection: grpc connection to target node
         */
        GRPCGraphDistribute(std::string gname, std::vector<Table*>& vprop,
                            std::vector<int32_t>& vertex_fid, Graph::Partition& partition, 
                            Table* edge_prop, bool use_eprop, bool use_incoming, bool ignore_in, 
                            std::unique_ptr<dan::DANInterface::Stub>& connection, unsigned int part_idx = 0);

        ~GRPCGraphDistribute();

        void distributeVertices();
        void distributeEdges();
        void waitVDistribute();
        void waitEDistribute();

        static void distributeVertexBatch(std::string& gname,
                            dan::Table* vprop, dan::IntVec* vpids, int32_t vpid,
                            std::unique_ptr<dan::DANInterface::Stub>& client);

        static void distributeEdgeBatch(std::string& gname, dan::Record* src, dan::Record* dest,
                            dan::IntVec* src_vpid, dan::IntVec* dest_vpid,
                            dan::Table* eprops, bool use_incoming, bool only_in, 
                            std::unique_ptr<dan::DANInterface::Stub>& client);
    protected:
        std::unique_ptr<dan::DANInterface::Stub>& connection_;
        DistVertex* vertex_worker_;
        DistEdge* edge_worker_;
        unsigned int part_idx_;
                                
};
};

#endif  // STORAGE_UTIL_GRPC_GRAPH_DISTRIBUTE_H_
