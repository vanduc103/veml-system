// Copyright 2021 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

// Project include
#include "common/def_const.h"
#include "storage/graph/graph.h"
#include "storage/util/serialize_graph_partition.h"

// C & C++ include
#include <boost/archive/binary_oarchive.hpp>
#include <boost/serialization/vector.hpp>

#include <grpcpp/grpcpp.h>
#include <grpc/support/log.h>
#include <grpcpp/resource_quota.h>

// gRPC generated file
#include "interface/interface.grpc.pb.h"

namespace storage{

void SerializeGraphPartition::distributeGraph(std::string& gname, std::string& partition_gname, std::unique_ptr<dan::DANInterface::Stub>& connection){
    /*
    Graph* graph = Metadata::getGraphFromName(partition_gname);
    if(graph == nullptr){
        std::string err("Not existing graph name" + partition_gname);
        RTI_EXCEPTION(err);    
    }
    std::vector<std::string> outputs;
    outputs.push_back("");
    // // oss[0] is for graph metadata, oss[1] is for edge prop, oss[2->n] is for vertex prop
    graph->serializePTables(outputs);
    // std::cout << "after serialize prop" << std::endl;
    std::ostringstream os;
    boost::archive::binary_oarchive oa(os); 
    // 1. serialize data
    oa << true; // is_graph flag
    oa << gname; // original graph name in master
    oa << partition_gname; // graph name for the partition in master
    graph->serialize(oa);
    outputs[0] = os.str();
    grpc::ClientContext ctx;
    dan::Code response;
    dan::SerializationBatch request;
    *request.mutable_v() = {outputs.begin(), outputs.end()};
    connection->serialize_graph_partition(&ctx, request, &response);
    */
}

void* SerializeGraphPartition::DistVertex::run(){
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
    Graph* graph = Metadata::getGraphFromName(name_);
    
    graph->insertVertexBatch(vprops, vpids);
    return nullptr;
}

void* SerializeGraphPartition::DistEdge::run(){    
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
    Graph* graph = Metadata::getGraphFromName(partition_gname_);
    if(rvprops.size() > 0){
        graph->insertVertexBatch(rvprops, rvpids);
    }
    graph->insertEdgeBatch(srcs, dsts, eprops, s_vpids, d_vpids, !use_incoming_, false);  
    SerializeGraphPartition::distributeGraph(name_, partition_gname_, connection_);
    return nullptr;
}

SerializeGraphPartition::SerializeGraphPartition(std::string gname,
        std::vector<Table*>& vprop, std::vector<int32_t>& vertex_fid,
        Graph::Partition& partition, Table* edge_prop,
        bool use_eprop, bool use_incoming, bool ignore_in,
        std::unique_ptr<dan::DANInterface::Stub>& connection,
        unsigned int part_idx) : connection_(connection), part_idx_(part_idx) {
    /*
    // find graph partition
    std::string partition_gname = gname + "$$" + std::to_string(part_idx);
    Graph* graph = Metadata::getGraphFromName(partition_gname);
    if(!graph){
        Graph* orig_graph = Metadata::getGraphFromName(gname);
        orig_graph->createPartitionGraph(partition_gname);
    }

    // generate worker
    vertex_worker_ = new DistVertex(partition_gname,
                            vprop, partition.vertex, connection);
    edge_worker_ = new DistEdge(gname, partition_gname,
                            vprop, vertex_fid, edge_prop, use_eprop,
                            use_incoming, ignore_in,
                            partition.edges, partition.vertex, connection);
    */
}

SerializeGraphPartition::~SerializeGraphPartition(){
    delete vertex_worker_;
    delete edge_worker_;
}

void SerializeGraphPartition::distributeVertices(){
    vertex_worker_->start();
}

void SerializeGraphPartition::distributeEdges(){ 
    edge_worker_->start();
}

void SerializeGraphPartition::waitVDistribute(){
    vertex_worker_->join();
}

void SerializeGraphPartition::waitEDistribute(){
    edge_worker_->join();
}

}  // namespace storage
