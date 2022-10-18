// project include
#ifndef STORAGE_UTIL_SERIALIZE_GRPCGRAPH_DISTRIBUTE_H_
#define STORAGE_UTIL_SERIALIZE_GRPCGRAPH_DISTRIBUTE_H_

#include "storage/graph/graph.h"
#include "storage/util/graph_distribute_base.h"

#include <boost/asio.hpp>
#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/tcp.hpp>

namespace storage{
class SerializeGRPCGraphDistribute : public GraphDistributeBase{
    public:
        class DistVertex : public RTIThread {
            public:
                DistVertex(std::string name,
                        std::vector<Table*>& vprop, std::set<int64_t>& vertex,
                        std::unique_ptr<dan::DANInterface::Stub>& connection)
                    : name_(name), vprop_(vprop), vertex_(vertex), connection_(connection) {}

                void* run();

            private:
                std::string name_;
                std::vector<Table*>& vprop_;
                std::set<int64_t>& vertex_;
                std::unique_ptr<dan::DANInterface::Stub>& connection_;
        };

        class DistEdge : public RTIThread {
            public:
                DistEdge(std::string name,
                        std::vector<Table*>& vprop, std::vector<int32_t>& vfid,
                        Table* eprop, bool use_eprop, bool use_incoming,
                        bool ignore_in, Graph::EdgeMap& edges, std::set<int64_t>& vertex,
                        std::unique_ptr<dan::DANInterface::Stub>& connection)
                    : name_(name), vprop_(vprop), vfid_(vfid), eprop_(eprop),
                    use_eprop_(use_eprop), use_incoming_(use_incoming),
                    ignore_in_(ignore_in), edges_(edges),
                    vertex_(vertex), connection_(connection){}
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
                std::unique_ptr<dan::DANInterface::Stub>& connection_;
        };

        /**
         * @brief create new graph distribute strategy using boost serialization
         * @param std::string gname: name of graph 
         * @param std::vector vprop: reference to vertex_prop_ in graph
         * @param std::vector vertex_fid: reference to vertex_fid_ in graph
         * @param Graph::Partition partition: reference to a list of partitions defined in a caller func
         * @param Table edge_prop: reference to edge_prop_ table in a graph
         * @param bool use_eprop: similar to use_eprop_ property in a graph
         * @param bool use_incoming: whether to store data in delta_in_, similar to use_incoming_ property in a graph
         * @param bool std::unique_ptr<dan::DANInterface::Stub> connection: grpc connection to target node
         * @param std::string path: path to output folder
         */
        SerializeGRPCGraphDistribute(std::string gname, std::vector<Table*>& vprop,
                            std::vector<int32_t>& vertex_fid, Graph::Partition& partition, 
                            Table* edge_prop, bool use_eprop, bool use_incoming, bool ignore_in, 
                            std::unique_ptr<dan::DANInterface::Stub>& connection, unsigned int part_idx = 0);
                                
        ~SerializeGRPCGraphDistribute();
        void distributeVertices();
        void distributeEdges();
        void waitVDistribute();
        void waitEDistribute();


        static std::pair<std::string, int> getClientInfo(std::unique_ptr<dan::DANInterface::Stub>& client);
        static void serializeValue(boost::archive::binary_oarchive& oa, EvalValue& val);
        static void serializeRecord(boost::archive::binary_oarchive& oa, EvalVec& props);
        static void serializeTable(boost::archive::binary_oarchive& oa, std::vector<EvalVec>& props);

        static void distributeVertexBatch(std::string& gname, std::vector<EvalVec>& vprops, 
                                                    std::vector<int64_t>& vpids, 
                                                    std::unique_ptr<dan::DANInterface::Stub>& connection);
        
        static void distributeEdgeBatch(std::string& gname, EvalVec& src, EvalVec& dest,
                            IntVec& src_vpid, IntVec& dest_vpid,
                            std::vector<EvalVec>& eprops, bool use_incoming, bool only_in, 
                            std::unique_ptr<dan::DANInterface::Stub>& connection);

    protected:
        std::unique_ptr<dan::DANInterface::Stub>& connection_;
        DistVertex* vertex_worker_;
        DistEdge* edge_worker_;
        unsigned int part_idx_;
        
};
};

#endif