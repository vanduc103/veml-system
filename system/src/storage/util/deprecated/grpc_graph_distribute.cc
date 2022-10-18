#include "storage/graph/graph.h"
#include "storage/util/grpc_graph_distribute.h"

#include <grpcpp/grpcpp.h>
#include <grpc/support/log.h>
#include <grpcpp/resource_quota.h>

namespace storage{

void* GRPCGraphDistribute::DistVertex::run(){
        dan::Table* vprops = new dan::Table();
        dan::IntVec* vpids = new dan::IntVec();
        std::cout << "distribute vertex" << std::endl;
        for (auto itr = vertex_.begin(); itr != vertex_.end(); ++itr) {
            int32_t vpid = *itr >> 32;
            int32_t vprop_vid = *itr & 0xFFFFFFFF;
            EvalVec vertex = vprop_[vpid]->getRecord(vprop_vid);
            vertex.push_back(Bool(false));
            auto row = vprops->add_v();
            generatePBRecord(row, vertex);
            vpids->add_v(vpid);
            // Graph::distributeVertex(name_, vertex, vpid, false, client_);
        }
        GRPCGraphDistribute::distributeVertexBatch(name_, vprops, vpids, 0, client_);
        return nullptr;
}

void* GRPCGraphDistribute::DistEdge::run(){
    using grpc::ClientContext;

    // prepare request(insert edge)
    auto eitr = edges_.begin();
    dan::Table* eprops = new dan::Table();
    dan::Record* srcs = new dan::Record();
    dan::Record* dsts = new dan::Record();
    dan::IntVec* src_vpid = new dan::IntVec();
    dan::IntVec* dst_vpid = new dan::IntVec();

    // use for distributing replicated vertices
    dan::Table* rvprops = new dan::Table();
    dan::IntVec* rvpids = new dan::IntVec();
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
                rvpids->add_v(d_vpid);
                EvalVec dest_v = vprop_[d_vpid]->getRecord(d_vprop_vid);
                dest_v.push_back(Bool(true));
                auto* v = rvprops->add_v();
                generatePBRecord(v, dest_v);
                dest = dest_v[vfid_[d_vpid]];
            } else {
                // dest vertex is assigned to same node w/ src
                dest = vprop_[d_vpid]->getValue(d_vprop_vid, vfid_[d_vpid]);
            }

            if (!ignore_in_) {
                dan::Eval* s = dsts->add_v();
                generatePBEval(s, src);
                dst_vpid->add_v(s_vpid);

                dan::Eval* d = srcs->add_v();
                generatePBEval(d, dest);
                src_vpid->add_v(d_vpid);                       
            } else{
                dan::Eval* s = srcs->add_v();
                generatePBEval(s, src);
                src_vpid->add_v(s_vpid);

                dan::Eval* d = dsts->add_v();
                generatePBEval(d, dest);
                dst_vpid->add_v(d_vpid);                    
            }
            
            if (use_eprop_) {
                int64_t eprop_id = eitr->second.second[j];
                EvalVec eprop = eprop_->getRecord(eprop_id);
                dan::Record* eprop_record = eprops->add_v();
                generatePBRecord(eprop_record, eprop);                        
            }
        }
    }
    // request distribution of edges
    if(rvprops->v_size() > 0){
        GRPCGraphDistribute::distributeVertexBatch(name_, rvprops, rvpids, 0, client_);
    }
    // set only_in to false 
    GRPCGraphDistribute::distributeEdgeBatch(name_, srcs, dsts, src_vpid, dst_vpid, eprops, use_incoming_, false, client_);
    
    return nullptr;
}

GRPCGraphDistribute::GRPCGraphDistribute(std::string gname, std::vector<Table*>& vprop,
                                        std::vector<int32_t>& vertex_fid, Graph::Partition& partition, 
                                        Table* edge_prop, bool use_eprop, bool use_incoming, bool ignore_in, 
                                        std::unique_ptr<dan::DANInterface::Stub>& connection, unsigned int part_idx)
                                        : connection_(connection), part_idx_(part_idx){
                                            /*
    vertex_worker_ = new GRPCGraphDistribute::DistVertex (gname, vprop, partition.vertex, connection);
    edge_worker_ = new GRPCGraphDistribute::DistEdge(gname, vprop, vertex_fid, edge_prop, use_eprop, use_incoming,
                                                ignore_in, partition.edges, partition.vertex,
                                                connection);
                                                */
}

GRPCGraphDistribute::~GRPCGraphDistribute(){
    delete vertex_worker_;
    delete edge_worker_;
}

void GRPCGraphDistribute::distributeVertexBatch(std::string& gname,
                    dan::Table* vprop, dan::IntVec* vpids, int32_t vpid,
                    std::unique_ptr<dan::DANInterface::Stub>& client){
    using grpc::ClientContext;
    ClientContext ctx;
    dan::Code res;
    dan::InsertVertexBatch request;
    request.set_gname(gname);
    request.set_allocated_vprop(vprop);
    request.set_allocated_vpids(vpids);
    request.set_vpid(vpid);
    grpc::Status status = client->insert_vertex_batch(&ctx, request, &res);
    if(!status.ok()){
        std::cout << "[ERROR] at distributVertexBatch: " << status.error_code() << std::endl;
    }
}
void GRPCGraphDistribute::distributeEdgeBatch(std::string& gname, dan::Record* src, dan::Record* dest,
                    dan::IntVec* src_vpid, dan::IntVec* dest_vpid,
                    dan::Table* eprops, bool use_incoming, bool only_in, 
                    std::unique_ptr<dan::DANInterface::Stub>& client){
    using grpc::ClientContext;
    ClientContext ctx;
    dan::Code response;
    dan::InsertEdgeBatch request;
    request.set_gname(gname);
    request.set_ignore_in(!use_incoming);
    request.set_only_in(only_in);
    request.set_allocated_eprop(eprops);
    request.set_allocated_src(src);
    request.set_allocated_dest(dest);
    request.set_allocated_src_vpid(src_vpid);
    request.set_allocated_dest_vpid(dest_vpid);
    grpc::Status status = client->insert_edge_batch(&ctx, request, &response);
    if(!status.ok()){
        std::cout << "[ERROR] at distributVertexBatch: " << status.error_code() << std::endl;
    }
}

void GRPCGraphDistribute::distributeVertices(){
    vertex_worker_->start();
}

void GRPCGraphDistribute::distributeEdges(){ 
    edge_worker_->start();
}

void GRPCGraphDistribute::waitVDistribute(){
    vertex_worker_->join();
}

void GRPCGraphDistribute::waitEDistribute(){
    edge_worker_->join();
}

};
