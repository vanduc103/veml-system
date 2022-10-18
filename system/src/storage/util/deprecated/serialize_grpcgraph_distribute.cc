#include "common/def_const.h"
#include "storage/graph/graph.h"
#include "storage/util/serialize_grpcgraph_distribute.h"

// other include
#include <boost/asio.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/tcp.hpp>

#include <grpcpp/grpcpp.h>
#include <grpc/support/log.h>
#include <grpcpp/resource_quota.h>

// gRPC generated file
#include "interface/interface.grpc.pb.h"

using namespace boost::asio;
using namespace boost::asio::ip;

namespace storage{

void SerializeGRPCGraphDistribute::serializeValue(boost::archive::binary_oarchive& oa, EvalValue& val){
    try{
        switch (val.getEvalType()){
            case EvalType::ET_BIGINT: {
                oa << ET_BIGINT;
                oa << val.getBigInt().get();
                break;
            }
            case EvalType::ET_DOUBLE: {
                oa << ET_DOUBLE;
                oa << val.getDouble().get();
                break;
            }
            case EvalType::ET_STRING: {
                oa << ET_STRING;
                oa << val.getString().get();
                break;
            }
            case EvalType::ET_BOOL: {
                oa << ET_BOOL;
                oa << val.getBool().get();
                break;
            }
            case EvalType::ET_DOUBLEVEC: {
                DoubleVec& dl = val.getDoubleVec();
                oa << ET_DOUBLEVEC;
                oa << dl;
                break;
            }
            case EvalType::ET_DOUBLELIST: {
                DoubleList& dl = val.getDoubleList();
                DoubleVec dlv;
                dl.copyTo(dlv);
                oa << ET_DOUBLEVEC;
                oa << dlv;
                break;
            }
            case EvalType::ET_INTVEC: {
                IntVec& il = val.getIntVec();
                oa << ET_INTVEC;
                oa << il;
                break;
            }
            case EvalType::ET_INTLIST: {
                IntList& il = val.getIntList();
                IntVec ilv;
                il.copyTo(ilv);
                oa << ET_INTVEC;
                oa << ilv;
                break;
            }
            default: {
                std::string err("invalid value type @serializeValue, ");
                RTI_EXCEPTION(err);
            }
        }
    }catch(std::exception& e){
        std::cout << "error on converting" << e.what() << std::endl;
    }
}

void SerializeGRPCGraphDistribute::serializeRecord(boost::archive::binary_oarchive& oa, EvalVec& props){
    oa << props.size();
    for(size_t i = 0; i < props.size(); i++){
        EvalValue& val = props[i];
        SerializeGRPCGraphDistribute::serializeValue(oa, val);
    }
}

void SerializeGRPCGraphDistribute::serializeTable(boost::archive::binary_oarchive& oa, std::vector<EvalVec>& props){
    oa << props.size();
    for(size_t i = 0; i < props.size(); i++){
        EvalVec& val = props[i];
        SerializeGRPCGraphDistribute::serializeRecord(oa, val);
    }
}

void SerializeGRPCGraphDistribute::distributeVertexBatch(std::string& gname, std::vector<EvalVec>& vprops, 
                                                    std::vector<int64_t>& vpids, 
                                                    std::unique_ptr<dan::DANInterface::Stub>& connection){
    // 2. open socket server in target node
    try{
        std::stringstream os;
        boost::archive::binary_oarchive oa(os);
        // 3. serialize data // oa << something
        oa << gname;
        SerializeGRPCGraphDistribute::serializeTable(oa, vprops);
        oa << vpids;
        grpc::ClientContext ctx;
        dan::Code response;
        std::string msg = os.str();
        dan::Serialization request;
        request.set_v(msg);
        //connection->serialize_vertex_batch(&ctx, request, &response);
    }catch(std::exception& e){
        std::string err(e.what());
        std::cout << "error when requesting insert vertex " << err << std::endl;
    }
    // 4. stream data to target node
}

void SerializeGRPCGraphDistribute::distributeEdgeBatch(std::string& gname, EvalVec& src, EvalVec& dest,
                            IntVec& src_vpid, IntVec& dest_vpid,
                            std::vector<EvalVec>& eprops, bool use_incoming, bool only_in, 
                            std::unique_ptr<dan::DANInterface::Stub>& connection){    
    try{
        std::ostringstream os;
        boost::archive::binary_oarchive oa(os);
        // 3. serialize data // oa << something
        oa << gname;
        SerializeGRPCGraphDistribute::serializeRecord(oa, src);
        SerializeGRPCGraphDistribute::serializeRecord(oa, dest);
        oa << src_vpid;
        oa << dest_vpid;
        SerializeGRPCGraphDistribute::serializeTable(oa, eprops);
        oa << use_incoming;
        oa << only_in;
        grpc::ClientContext ctx;
        dan::Code response;
        std::string msg = os.str();
        dan::Serialization request;
        request.set_v(msg);
        //connection->serialize_edge_batch(&ctx, request, &response);
    }catch(std::exception& e){
        std::string err(e.what());
        std::cout << "error when requesting insert edges " << err << std::endl;
    }
}

void* SerializeGRPCGraphDistribute::DistVertex::run(){
    std::vector<EvalVec> vprops;
    IntVec vpids(vertex_.size());
    size_t i = 0;
    for (auto itr = vertex_.begin(); itr != vertex_.end(); ++itr) {
        int32_t vpid = *itr >> 32;
        int32_t vprop_vid = *itr & 0xFFFFFFFF;
        EvalVec vertex = vprop_[vpid]->getRecord(vprop_vid);
        vertex.push_back(Bool(false));
        vprops.push_back(vertex);
        vpids[i] = vpid;
        ++i;
    }
    SerializeGRPCGraphDistribute::distributeVertexBatch(name_, vprops, vpids, connection_);
    return nullptr;
}

void* SerializeGRPCGraphDistribute::DistEdge::run(){    
    auto eitr = edges_.begin();
    std::vector<EvalVec> eprops, rvprops;
    EvalVec srcs, dsts;
    IntVec s_vpids, d_vpids, rvpids;
    for ( ; eitr != edges_.end(); ++eitr) {
        // set source vertex
        int64_t src_id = eitr->first;
        int32_t s_vpid = src_id >> 32;
        int32_t s_vprop_vid = src_id & 0xFFFFFFFF;

        EvalValue src = vprop_[s_vpid]->getValue(
                            s_vprop_vid, vfid_[s_vpid]);
        // get adjacent list of source vertex
        size_t size = eitr->second.first.size();
        for (unsigned j = 0; j < size; j++) {
            // set dest vertex
            int64_t dest_id = eitr->second.first[j];
            int32_t d_vpid = dest_id >> 32;
            int32_t d_vprop_vid = dest_id & 0xFFFFFFFF;
            EvalValue dest;

            if (vertex_.count(dest_id) <= 0) {
                // dest vertex is assigned to another node
                vertex_.insert(dest_id);
                EvalVec dest_v = vprop_[d_vpid]->getRecord(d_vprop_vid);
                dest_v.push_back(Bool(true));
                rvprops.push_back(dest_v);
                rvpids.push_back(d_vpid);
                dest = dest_v[vfid_[d_vpid]];
            } else {
                // dest vertex is assigned to same node w/ src
                dest = vprop_[d_vpid]->getValue(d_vprop_vid, vfid_[d_vpid]);
            }

            if (!ignore_in_) {
                // dst -> src
                s_vpids.push_back(d_vpid);
                d_vpids.push_back(s_vpid);
                srcs.push_back(dest);
                dsts.push_back(src);
            } else{
                // src -> dst
                s_vpids.push_back(s_vpid);
                d_vpids.push_back(d_vpid);
                srcs.push_back(src);
                dsts.push_back(dest);
            }
            
            if (use_eprop_) {
                int64_t eprop_id = eitr->second.second[j];
                EvalVec eprop = eprop_->getRecord(eprop_id);               
                eprops.push_back(eprop);
            }
        }
    }
    if(rvprops.size() > 0){
        SerializeGRPCGraphDistribute::distributeVertexBatch(name_, rvprops, rvpids, connection_);
    }
    SerializeGRPCGraphDistribute::distributeEdgeBatch(name_, srcs, dsts, s_vpids, d_vpids, eprops, use_incoming_, false, connection_);
  
    
    return nullptr;
}

SerializeGRPCGraphDistribute::SerializeGRPCGraphDistribute(std::string gname, std::vector<Table*>& vprop,
                            std::vector<int32_t>& vertex_fid, Graph::Partition& partition, 
                            Table* edge_prop, bool use_eprop, bool use_incoming, bool ignore_in, 
                            std::unique_ptr<dan::DANInterface::Stub>& connection, unsigned int part_idx) 
                            : connection_(connection), part_idx_(part_idx){
                                /*
    vertex_worker_ = new SerializeGRPCGraphDistribute::DistVertex(gname, vprop, partition.vertex, connection);
    edge_worker_ = new SerializeGRPCGraphDistribute::DistEdge(gname, vprop, vertex_fid, edge_prop, use_eprop, use_incoming,
                                                ignore_in, partition.edges, partition.vertex, connection);                            
                                                */
}

SerializeGRPCGraphDistribute::~SerializeGRPCGraphDistribute(){
    delete vertex_worker_;
    delete edge_worker_;
}

void SerializeGRPCGraphDistribute::distributeVertices(){
    vertex_worker_->start();
}

void SerializeGRPCGraphDistribute::distributeEdges(){ 
    edge_worker_->start();
}

void SerializeGRPCGraphDistribute::waitVDistribute(){
    vertex_worker_->join();
}

void SerializeGRPCGraphDistribute::waitEDistribute(){
    edge_worker_->join();
}

};
