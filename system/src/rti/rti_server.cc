// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

// Project include
#include "common/def_const.h"
#include "storage/relation/table.h"
#include "storage/graph/graph.h"
//#include "storage/graph/graph_sparse_training.h"
#include "persistency/archive_manager.h"
#include "storage/util/data_transfer.h"
#include "storage/util/util_func.h"

#include "util/timer.h"

// Other include
#include <grpcpp/grpcpp.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpc/support/log.h>
#include <google/protobuf/metadata.h>
#include <grpcpp/resource_quota.h>

// gRPC generated file
#include "interface/interface.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ClientContext;
using grpc::Status;

typedef google::protobuf::RepeatedPtrField<dan::Field> PBFieldVec;

class RTIServer final : public dan::DANInterface::Service {
  public:
    RTIServer(std::string addr) : addr_(addr) {}

    /**
     * dan cluster api
     */
    Status ping(ServerContext* context,
            const dan::Void* request, dan::Code* response) override;
    Status archive(ServerContext* context,
            const dan::Str* request, dan::Code* response) override;
    Status recover(ServerContext* context,
            const dan::Str* request, dan::Code* response) override;
    Status request_node_list(ServerContext* context,
            const dan::Void* request, dan::NodeInfoVec* response) override;
    Status memory_statistics(ServerContext* context,
            const dan::MemoryStat* request, dan::Code* response) override;

    /**
     * ddl api
     */
    Status create_table(ServerContext* context,
            const dan::CreateTable* request, dan::Code* response) override;
    Status drop_table(ServerContext* context,
            const dan::Str* request, dan::Code* response) override;
    Status drop_table_if_exists(ServerContext* context,
            const dan::Str* request, dan::Code* response) override;
    Status create_index(ServerContext* context,
            const dan::CreateIndex* request, dan::Code* response) override;
    Status table_partition(ServerContext* context,
            const dan::TablePartition* request, dan::Code* response) override;
    Status set_partition(ServerContext* context,
            const dan::Str* request, dan::Code* response) override;

    Status create_graph(ServerContext* context,
            const dan::CreateGraph* request, dan::Code* response) override;
    Status drop_graph(ServerContext* context,
            const dan::Str* request, dan::Code* response) override;
    Status drop_graph_if_exists(ServerContext* context,
            const dan::Str* request, dan::Code* response) override;
    Status define_vertex(ServerContext* context,
            const dan::DefineVertex* request, dan::Code* response) override;
    Status graph_partition(ServerContext* context,
            const dan::GraphPartition* request, dan::Code* response) override;
    Status graph_merge(ServerContext* context,
            const dan::MergeInfo* request, dan::Code* response) override;

    Status transfer_data(ServerContext* context,
            const dan::TransferBinary* request, dan::Code* response) override;

    /**
     * dml api
     */
    Status insert_record(ServerContext* context,
            const dan::InsertRecord* request, dan::Str* response) override;
    Status insert_record_batch(ServerContext* context,
            const dan::InsertRecordBatch* request, dan::Code* response) override;
    Status import_vertex(ServerContext* contest,
            const dan::ImportVertex* request, dan::Code* response) override;
    Status insert_vertex(ServerContext* context,
            const dan::InsertVertex* request, dan::Long* response) override;
    Status insert_vertex_batch(ServerContext* context,
            const dan::InsertVertexBatch* request, dan::Code* response) override;
    Status import_edge(ServerContext* contest,
            const dan::ImportEdge* request, dan::Code* response) override;
    Status insert_edge(ServerContext* context,
            const dan::InsertEdge* request, dan::Code* response) override;
    Status insert_edge_vp(ServerContext* context,
            const dan::InsertEdgeVP* request, dan::Code* response) override;
    Status insert_edge_batch(ServerContext* context,
            const dan::InsertEdgeBatch* request, dan::Code* response) override;

    Status get_table_info(ServerContext* context,
            const dan::Str* request, dan::TableInfo* response) override;
    Status get_num_records(ServerContext* context,
            const dan::Str* request, dan::Long* response) override;
    Status table_scan(ServerContext* context,
            const dan::TableScan* request, dan::Table* response) override;
    Status get_graph_info(ServerContext* context,
            const dan::Str* request, dan::GraphInfo* response) override;
    Status get_num_vertex(ServerContext* context,
            const dan::Str* request, dan::Long* response) override;
    Status get_num_edges(ServerContext* context,
            const dan::Str* request, dan::Long* response) override;
    Status get_vertex(ServerContext* context,
            const dan::GetVertex* request, dan::Record* response) override;
    Status adjacent_list(ServerContext* context, 
            const dan::AdjacentList* request, dan::AdjacentListResult* response) override;
    Status vertex_exists(ServerContext* context,
            const dan::VertexExists* request, dan::Code* response) override;
    Status train_node_classifier(ServerContext* context,
            const dan::NodeClassifierInfo* request, dan::Code* response) override;
    
  private:
    /**
     * util functions
     */
    static Status set_cfield_info(CreateFieldVec& cfinfos,
                            const PBFieldVec& schema);
    
    static bool releaseTable(Table* table);
    static bool releaseGraph(Graph* graph);

    std::string addr_;
};

/**
 * dan cluster api
 */
Status RTIServer::ping(ServerContext* context,
        const dan::Void* request, dan::Code* response) {
    response->set_msg(dan::Code_Status_OK);
    return Status::OK;
}

Status RTIServer::archive(ServerContext* context,
        const dan::Str* request, dan::Code* response) {
    if(!persistency::ArchiveManager::archive(request->v())) {
        response->set_msg(dan::Code_Status_ARCHIVE_ERROR);
        return Status(grpc::INTERNAL,
                "[FAILED] failed to archive system to path, "
                + request->v());
    }
    response->set_msg(dan::Code_Status_OK);
    return Status::OK;
}

Status RTIServer::recover(ServerContext* context,
        const dan::Str* request, dan::Code* response) {
    if(!persistency::ArchiveManager::recover(request->v())) {
        response->set_msg(dan::Code_Status_RECOVERY_ERROR);
        return Status(grpc::INTERNAL,
                "[FAILED] failed to recover system from path, "
                + request->v());
    }
    response->set_msg(dan::Code_Status_OK);
    return Status::OK;
}

Status RTIServer::request_node_list(ServerContext* context,
        const dan::Void* request, dan::NodeInfoVec* response) {
    // find table
    Table* table = Metadata::getTableFromName("$SYS$NODES");
    if (table == nullptr) {
        return Status(grpc::INVALID_ARGUMENT,
                "not existing table name: $SYS$NODES");
    }

    // scan table
    auto itr = table->begin();
    for ( ; itr; ++itr) {
        EvalVec records = itr.getRecord();
        dan::NodeInfo* node = response->add_v();
        node->set_id(records[0].getBigInt().get());
        node->set_host(records[1].getString().get());
        node->set_port(records[2].getBigInt().get());
        node->set_head(records[3].getBool().get());
    }

    // response
    return Status::OK;
}

Status RTIServer::memory_statistics(ServerContext* context,
            const dan::MemoryStat* request, dan::Code* response) {
    // collect memory stat
    Metadata::memoryStat(request->path(),
            request->verbose(), request->level(), addr_);

    StrVec nodes;
    storage::replace_pb_to_std(request->nodes(), nodes);

    // collect memory stat in worker nodes
    for (auto node : nodes) {
        grpc::ClientContext ctx;
        dan::MemoryStat req;
        dan::Code res;

        req.set_path(request->path());
        req.set_verbose(request->verbose());
        req.set_level(request->level()+1);

        auto stub = dan::DANInterface::NewStub(
                grpc::CreateChannel(node,
                    grpc::InsecureChannelCredentials()));
        stub->memory_statistics(&ctx, req, &res);
    }

    // response
    response->set_msg(dan::Code_Status_OK);
    return Status::OK;
}

/**
 * ddl api
 */
Status RTIServer::create_table(ServerContext* context,
        const dan::CreateTable* request, dan::Code* response) {
    // check requested table name
    Table* table = Metadata::getTableFromName(request->tname());
    if (table) {
        response->set_msg(dan::Code_Status_DUPLICATED_TABLE_NAME);
        return Status(grpc::ALREADY_EXISTS,
                "duplicated table name, " + request->tname());
    }

    // check requested schema
    CreateFieldVec cfinfos;
    Status err = RTIServer::set_cfield_info(cfinfos, request->schema());
    if (err.error_code() != grpc::OK) {
        response->set_msg(dan::Code_Status_CREATE_TABLE_ERROR);
        return err;
    }

    // check requested table type
    Table::Type type;
    if (request->ttype().compare("ROW") == 0) {
        type = Table::ROW;
    } else if (request->ttype().compare("COLUMN") == 0) {
        type = Table::COLUMN;
    } else {
        response->set_msg(dan::Code_Status_INVALID_TABLE_TYPE);
        return Status(grpc::INVALID_ARGUMENT,
                "invalid table type: " + request->ttype());
    }

    // create table
    table = Table::create(type, request->tname(),
            cfinfos, request->inc_fid(), request->inc_start());

    // response
    response->set_msg(dan::Code_Status_OK);
    return Status::OK;
}
    
Status RTIServer::drop_table(ServerContext* context,
        const dan::Str* request, dan::Code* response) {
    // find table
    Table* table = Metadata::getTableFromName(request->v());
    if (table == nullptr) {
        response->set_msg(dan::Code_Status_TABLE_NOT_FOUND);
        return Status(grpc::INVALID_ARGUMENT,
                "not existing table name: " + request->v());
    }

    // release table
    if (!RTIServer::releaseTable(table)) {
        response->set_msg(dan::Code_Status_DROP_TABLE_ERROR);
        return Status(grpc::INTERNAL,
                "[FAILED] drop table, " + request->v());
    }

    // response
    response->set_msg(dan::Code_Status_OK);
    return Status::OK;
}

Status RTIServer::drop_table_if_exists(ServerContext* context,
        const dan::Str* request, dan::Code* response) {
    // find table
    Table* table = Metadata::getTableFromName(request->v());
    if (table != nullptr) {
        // release table
        if (!RTIServer::releaseTable(table)) {
            response->set_msg(dan::Code_Status_DROP_TABLE_ERROR);
            return Status(grpc::INTERNAL,
                    "[FAILED] drop table, " + request->v());
        }
    }

    // response
    response->set_msg(dan::Code_Status_OK);
    return Status::OK;
}

Status RTIServer::create_index(ServerContext* context,
        const dan::CreateIndex* request, dan::Code* response) {
    // find table
    Table* table = Metadata::getTableFromName(request->tname());
    if (table == nullptr) {
        response->set_msg(dan::Code_Status_TABLE_NOT_FOUND);
        return Status(grpc::INVALID_ARGUMENT,
                "not existing table name: " + request->tname());
    }

    // create index
    table->createIndex(request->fid(), request->unique());

    // response
    response->set_msg(dan::Code_Status_OK);
    return Status::OK;
}

Status RTIServer::table_partition(ServerContext* context,
        const dan::TablePartition* request, dan::Code* response) {
    // find table
    Table* table = Metadata::getTableFromName(request->tname());
    if (table == nullptr) {
        response->set_msg(dan::Code_Status_TABLE_NOT_FOUND);
        return Status(grpc::INVALID_ARGUMENT,
                "not existing table name: " + request->tname());
    }

    // partition table
    StrVec nodes = storage::replace_pb_to_std(request->nodes());
    if (!table->partition(request->fid(), nodes, 8)) {
        response->set_msg(dan::Code_Status_TABLE_PARTITION_ERROR);
        return Status(grpc::UNKNOWN,
                "table partitioning failed " + request->tname());
    }

    // response
    response->set_msg(dan::Code_Status_OK);
    return Status::OK;
}

Status RTIServer::set_partition(ServerContext* context,
        const dan::Str* request, dan::Code* response) {
    // find table
    Table* table = Metadata::getTableFromName(request->v());
    if (table == nullptr) {
        response->set_msg(dan::Code_Status_TABLE_NOT_FOUND);
        return Status(grpc::INVALID_ARGUMENT,
                "not existing table name: " + request->v());
    }

    // create index
    table->setPartition();

    // response
    response->set_msg(dan::Code_Status_OK);
    return Status::OK;
}

Status RTIServer::create_graph(ServerContext* context,
        const dan::CreateGraph* request, dan::Code* response) {
    // check requested graph name
    Graph* graph = Metadata::getGraphFromName(request->gname());
    if (graph) {
        response->set_msg(dan::Code_Status_DUPLICATED_GRAPH_NAME);
        return Status(grpc::ALREADY_EXISTS,
                "duplicated grpah name, " + request->gname());
    }

    // check requested property schema
    CreateFieldVec vfinfos, efinfos;
    Status err = RTIServer::set_cfield_info(vfinfos, request->vschema());
    if (err.error_code() != grpc::OK) {
        response->set_msg(dan::Code_Status_CREATE_GRAPH_ERROR);
        return err;
    }
    err = RTIServer::set_cfield_info(efinfos, request->eschema());
    if (err.error_code() != grpc::OK) {
        response->set_msg(dan::Code_Status_CREATE_GRAPH_ERROR);
        return err;
    }

    // check requested type for property table
    Table::Type vptype = Table::ROW;
    Table::Type eptype = Table::ROW;
    if (request->vptype().compare("ROW") == 0) {
        vptype = Table::ROW;
    } else if (request->vptype().compare("COLUMN") == 0) {
        vptype = Table::COLUMN;
    } else if (request->vptype().compare("") != 0) {
        response->set_msg(dan::Code_Status_INVALID_TABLE_TYPE);
        return Status(grpc::INVALID_ARGUMENT,
            "invalid table type for graph property: "+ request->vptype());
    }

    if (request->eptype().compare("ROW") == 0) {
        eptype = Table::ROW;
    } else if (request->eptype().compare("COLUMN") == 0) {
        eptype = Table::COLUMN;
    } else if (request->eptype().compare("") != 0) {
        response->set_msg(dan::Code_Status_INVALID_TABLE_TYPE);
        return Status(grpc::INVALID_ARGUMENT,
            "invalid table type for graph property: "+ request->eptype());
    }

    // create graph
    graph = Graph::create(request->gname(),
            request->vpname(), vfinfos, request->vpfid(),
            vptype, efinfos, eptype,
            request->incoming(), request->merge(),
            request->is_partition(), request->part_type(),
            storage::replace_pb_to_std(request->nodes()));

    // response
    response->set_msg(dan::Code_Status_OK);
    return Status::OK;
}
    
Status RTIServer::drop_graph(ServerContext* context,
        const dan::Str* request, dan::Code* response) {
    // find graph 
    Graph* graph = Metadata::getGraphFromName(request->v());
    if (graph == nullptr) {
        response->set_msg(dan::Code_Status_GRAPH_NOT_FOUND);
        return Status(grpc::INVALID_ARGUMENT,
                "not existing graph name: " + request->v());
    }

    // release graph
    if (!RTIServer::releaseGraph(graph)) {
        response->set_msg(dan::Code_Status_DROP_GRAPH_ERROR);
        return Status(grpc::INTERNAL,
                "[FAILED] drop graph, " + request->v());
    }

    // response
    response->set_msg(dan::Code_Status_OK);
    return Status::OK;
}

Status RTIServer::drop_graph_if_exists(ServerContext* context,
        const dan::Str* request, dan::Code* response) {
    // find graph
    Graph* graph = Metadata::getGraphFromName(request->v());
    if (graph != nullptr) {
        // release graph
        if (!RTIServer::releaseGraph(graph)) {
            response->set_msg(dan::Code_Status_DROP_GRAPH_ERROR);
            return Status(grpc::INTERNAL,
                    "[FAILED] drop graph, " + request->v());
        }
    }

    // response
    response->set_msg(dan::Code_Status_OK);
    return Status::OK;
}

Status RTIServer::define_vertex(ServerContext* context,
        const dan::DefineVertex* request, dan::Code* response) {
    // find graph 
    Graph* graph = Metadata::getGraphFromName(request->gname());
    if (graph == nullptr) {
        response->set_msg(dan::Code_Status_GRAPH_NOT_FOUND);
        return Status(grpc::INVALID_ARGUMENT,
                "not existing graph name: " + request->gname());
    }

    // check requested property schema
    CreateFieldVec vfinfos;
    Status err = RTIServer::set_cfield_info(vfinfos, request->vschema());
    if (err.error_code() != grpc::OK) {
        response->set_msg(dan::Code_Status_CREATE_GRAPH_ERROR);
        return err;
    }

    // check requested type for property table
    Table::Type vptype = Table::ROW;
    if (request->vptype().compare("ROW") == 0) {
        vptype = Table::ROW;
    } else if (request->vptype().compare("COLUMN") == 0) {
        vptype = Table::COLUMN;
    } else if (request->vptype().compare("") != 0) {
        response->set_msg(dan::Code_Status_INVALID_TABLE_TYPE);
        return Status(grpc::INVALID_ARGUMENT,
            "invalid table type for graph property: "+ request->vptype());
    }

    // define vertex
    graph->defineVertex(request->vpname(),
            vfinfos, request->vpfid(), vptype);

    // response
    response->set_msg(dan::Code_Status_OK);
    return Status::OK;
}

Status RTIServer::graph_partition(ServerContext* context,
        const dan::GraphPartition* request, dan::Code* response) {
    // find graph
    Graph* graph = Metadata::getGraphFromName(request->gname());
    if (graph == nullptr) {
        response->set_msg(dan::Code_Status_GRAPH_NOT_FOUND);
        return Status(grpc::INVALID_ARGUMENT,
                "not existing graph name: " + request->gname());
    }

    // partition graph
    StrVec nodes = storage::replace_pb_to_std(request->nodes());
    if (!graph->partition(request->ptype(), nodes, request->dop())) {
        response->set_msg(dan::Code_Status_GRAPH_PARTITION_ERROR);
        return Status(grpc::UNKNOWN,
                "graph partitioning failed " + request->gname());
    }

    // response
    response->set_msg(dan::Code_Status_OK);
    return Status::OK;
}

Status RTIServer::graph_merge(ServerContext* context,
        const dan::MergeInfo* request, dan::Code* response) {
    // find graph
    Graph* graph = Metadata::getGraphFromName(request->gname());
    if (graph == nullptr) {
        response->set_msg(dan::Code_Status_GRAPH_NOT_FOUND);
        return Status(grpc::INVALID_ARGUMENT,
                "not existing graph name: " + request->gname());
    }
    size_t least = request->least();
    least = least > 0 ? least : 1;
    size_t num_split = request->num_split();
    if(num_split > 0) {
        graph->mergeManually(least, num_split);
    }else{
        // merge graph manually
        graph->mergeManually(least);
    }
    // response
    response->set_msg(dan::Code_Status_OK);
    return Status::OK;
}

Status RTIServer::transfer_data(ServerContext* context,
        const dan::TransferBinary* request, dan::Code* response) {
    // handle transfer
    TransferHandler handler(request->type(), request->name(),
                        request->pa_id(), request->sgmt_id(),
                        request->binary().v());
    handler.run();

    // response
    response->set_msg(dan::Code_Status_OK);
    return Status::OK;
}

/**
 * dml api
 */
Status RTIServer::insert_record(ServerContext* context,
        const dan::InsertRecord* request, dan::Str* response) {
    // find table
    Table* table = Metadata::getTableFromName(request->tname());
    if (table == nullptr) {
        return Status(grpc::INVALID_ARGUMENT,
                "not existing table name: " + request->tname());
    }

    // translate values
    EvalVec record = storage::generateEvalRecord(request->values());

    // insert to table
    AddressPair slot = table->insertRecord(record);
    if (slot.lptr_ == LOGICAL_NULL_PTR && !table->isPartitioned()) {
        return Status(grpc::INTERNAL,
                "[FAILED] insert to table, " + request->tname());
    }

    // response
    return Status::OK;
}

Status RTIServer::insert_record_batch(ServerContext* context,
        const dan::InsertRecordBatch* request, dan::Code* response) {
    // find table
    Table* table = Metadata::getTableFromName(request->tname());
    if (table == nullptr) {
        return Status(grpc::INVALID_ARGUMENT,
                "not existing table name: " + request->tname());
    }

    auto& batch = request->batch();
    if (table->isPartitioned()) {
        // divide batch
        class PartReq : public RTIThread {
          public:
            PartReq(std::string tname, std::string& addr)
                : stub_(dan::DANInterface::NewStub(
                            grpc::CreateChannel(addr,
                                grpc::InsecureChannelCredentials()))) {
                request_.set_tname(tname);
            }

            void enroll(const dan::Record& record) {
                auto batch = request_.mutable_batch();
                *(batch->add_v()) = record;
            }

            void* run() {
                ClientContext ctx;
                dan::Code response;
                stub_->insert_record_batch(&ctx, request_, &response);
                return nullptr;
            }

          private:
            dan::InsertRecordBatch request_;
            std::unique_ptr<dan::DANInterface::Stub> stub_;
        };

        // generate request for each partition
        StrVec nodes = table->getPartNodes();
        std::vector<PartReq> workers;
        for (auto node: nodes) {
            workers.push_back(PartReq(table->getTableName(), node));
        }

        int32_t part_fid = table->getPartFid();
        for (int i = 0; i < batch.v_size(); i++) {
            // get partition field value
            auto& record = batch.v(i);
            EvalValue val = storage::generateEvalValue(record.v(part_fid));
            int32_t part_id = table->getPartId(val);
            if (part_id < 0 || part_id >= (int)nodes.size()) {
                return Status(grpc::INTERNAL,
                        "[FAILED] insert to table, " + request->tname());
            }
            workers[part_id].enroll(record);
        }

        for (auto& w : workers) {
            w.start();
        }

        for (auto& w : workers) {
            w.join();
        }

    } else {
        for (int i = 0; i < batch.v_size(); i++) {
            // translate values
            EvalVec record = storage::generateEvalRecord(batch.v(i));

            // insert to table
            AddressPair slot = table->insertRecord(record);
            if (slot.lptr_ == LOGICAL_NULL_PTR) {
                response->set_msg(dan::Code_Status_INSERT_RECORD_ERROR);
                return Status(grpc::INTERNAL,
                        "[FAILED] insert to table, " + request->tname());
            }
        }
    }

    // response
    response->set_msg(dan::Code_Status_OK);
    return Status::OK;
}

Status RTIServer::import_vertex(ServerContext* contest,
        const dan::ImportVertex* request, dan::Code* response) {
    Graph* graph = Metadata::getGraphFromName(request->gname());
    if (graph == nullptr) {
        return Status(grpc::INVALID_ARGUMENT,
                "not existing graph name: " + request->gname());
    }

    // import vertex
    graph->importVertex(request->vpname(),
            request->csv(), request->delim(), request->header());

    // response
    response->set_msg(dan::Code_Status_OK);
    return Status::OK;
}

Status RTIServer::insert_vertex(ServerContext* context,
        const dan::InsertVertex* request, dan::Long* response) {
    Graph* graph = Metadata::getGraphFromName(request->gname());
    if (graph == nullptr) {
        return Status(grpc::INVALID_ARGUMENT,
                "not existing graph name: " + request->gname());
    }

    // translate values
    EvalVec vprop = storage::generateEvalRecord(request->vprop());
    int vpid = request->vpid();

    int ret = graph->insertVertex(vprop, vpid);

    // response
    response->set_v(ret);
    return Status::OK;
}

Status RTIServer::import_edge(ServerContext* contest,
        const dan::ImportEdge* request, dan::Code* response) {
    Graph* graph = Metadata::getGraphFromName(request->gname());
    if (graph == nullptr) {
        return Status(grpc::INVALID_ARGUMENT,
                "not existing graph name: " + request->gname());
    }

    // import edge
    graph->importEdge(request->csv(),
            request->src_vpid(), request->dst_vpid(),
            request->delim(), request->header(),
            request->batch(), request->dop());

    // response
    response->set_msg(dan::Code_Status_OK);
    return Status::OK;
}

Status RTIServer::insert_vertex_batch(ServerContext* context, const dan::InsertVertexBatch* request, dan::Code* response){
    util::Timer timer;
    timer.start();
    Graph* graph = Metadata::getGraphFromName(request->gname());
    if (graph == nullptr) {
        return Status(grpc::INVALID_ARGUMENT,
                "not existing graph name: " + request->gname());
    }
    using std::chrono::high_resolution_clock;
    using std::chrono::duration_cast;
    using std::chrono::milliseconds;

    std::vector<EvalVec> tables;
    IntVec vpids;
    storage::generateEvalTable(tables, request->vprop());
    auto rvpids = request->vpids();
    storage::replace_pb_to_std(rvpids, vpids);

    graph->insertVertexBatch(tables, vpids, request->vpid(), 8);
    timer.stop();
    return Status::OK;
}

Status RTIServer::insert_edge(ServerContext* context,
        const dan::InsertEdge* request, dan::Code* response) {
    Graph* graph = Metadata::getGraphFromName(request->gname());
    try{
        if (graph == nullptr) {
            return Status(grpc::INVALID_ARGUMENT,
                    "not existing graph name: " + request->gname());
        }

        // translate values
        EvalValue src = storage::generateEvalValue(request->src());
        EvalValue dest = storage::generateEvalValue(request->dest());
        int src_vpid = request->src_vpid();
        int dest_vpid = request->dest_vpid();
        EvalVec eprop = storage::generateEvalRecord(request->eprop());
        bool ignore_in = request->ignore_in();
        bool only_in = request->only_in();

        graph->insertEdge(src, dest, eprop,
                src_vpid, dest_vpid, ignore_in, only_in);

        // response
        response->set_msg(dan::Code_Status_OK);
        
    }catch(RTIException& e){
        response->set_msg(dan::Code_Status_OBJECT_NOT_FOUND);
    }
    return Status::OK;
}

Status RTIServer::insert_edge_vp(ServerContext* context,
        const dan::InsertEdgeVP* request, dan::Code* response) {
    Graph* graph = Metadata::getGraphFromName(request->gname());
    if (graph == nullptr) {
        return Status(grpc::INVALID_ARGUMENT,
                "not existing graph name: " + request->gname());
    }

    // translate values
    EvalVec src = storage::generateEvalRecord(request->src());
    EvalVec dest = storage::generateEvalRecord(request->dest());
    int src_vpid = request->src_vpid();
    int dest_vpid = request->dest_vpid();
    EvalVec eprop = storage::generateEvalRecord(request->eprop());

    graph->insertEdge(src, dest, eprop, src_vpid, dest_vpid);

    // response
    response->set_msg(dan::Code_Status_OK);
    return Status::OK;
}

Status RTIServer::insert_edge_batch(ServerContext* context,
        const dan::InsertEdgeBatch* request, dan::Code* response) {
    try{
        util::Timer timer;
        timer.start();
        Graph* graph = Metadata::getGraphFromName(request->gname());
        if (graph == nullptr) {
            return Status(grpc::INVALID_ARGUMENT,
                    "not existing graph name: " + request->gname());
        }
        // translate values
        EvalVec src = storage::generateEvalRecord(request->src());
        EvalVec dest = storage::generateEvalRecord(request->dest());
        IntVec src_vpid = storage::replace_pb_to_std(request->src_vpid());
        IntVec dest_vpid = storage::replace_pb_to_std(request->dest_vpid());
        bool ignore_in = request->ignore_in();
        bool only_in = request->only_in();
        bool use_eprop = request->eprop().v_size() != 0;

        std::vector<EvalVec> eprop;
        if (use_eprop)
            storage::generateEvalTable(eprop, request->eprop());

        graph->insertEdgeBatch(src, dest, eprop,
                src_vpid, dest_vpid, ignore_in, only_in, 16);
        // response
        response->set_msg(dan::Code_Status_OK);
        timer.stop();
    }catch(RTIException& e){
        response->set_msg(dan::Code_Status_OBJECT_NOT_FOUND);
    }
    return Status::OK;
}

Status RTIServer::get_table_info(ServerContext* context,
        const dan::Str* request, dan::TableInfo* response) {
    // find table
    Table* table = Metadata::getTableFromName(request->v());
    if (table == nullptr) {
        return Status(grpc::INVALID_ARGUMENT,
                "not existing table name: " + request->v());
    }

    StrVec finfos = table->getFieldInfoStr();
    StrVec part_nodes = table->getPartNodes();
    int32_t part_fid = table->getPartFid();

    // response
    storage::replace_std_to_pb(finfos, response->mutable_fields());
    storage::replace_std_to_pb(part_nodes, response->mutable_nodes());
    response->set_part_fid(part_fid);
    return Status::OK;
}

Status RTIServer::get_num_records(ServerContext* context,
        const dan::Str* request, dan::Long* response) {
    // find table
    Table* table = Metadata::getTableFromName(request->v());
    if (table == nullptr) {
        return Status(grpc::INVALID_ARGUMENT,
                "not existing table name: " + request->v());
    }

    // response
    if (table->isPartitioned()) {
        StrVec parts = table->getPartNodes();
        int total = 0;
        for (auto p : parts) {
            grpc::ClientContext ctx;
            dan::Str req;
            dan::Long res;

            req.set_v(request->v());
            auto stub = dan::DANInterface::NewStub(
                    grpc::CreateChannel(p,
                        grpc::InsecureChannelCredentials()));

            stub->get_num_records(&ctx, req, &res);
            total += res.v();
        }
        response->set_v(total);
    } else {
        response->set_v(table->getNumRecords());
    }

    return Status::OK;
}

Status RTIServer::table_scan(ServerContext* context,
        const dan::TableScan* request, dan::Table* response) {
    // find table
    Table* table = Metadata::getTableFromName(request->tname());
    if (table == nullptr) {
        return Status(grpc::INVALID_ARGUMENT,
                "not existing table name: " + request->tname());
    }

    if (table->isPartitioned()) {
        // collect data from worker nodes
        auto collect = [] (std::string addr, std::string tname,
                int32_t limit, dan::Table& response) {
            auto node = std::unique_ptr<dan::DANInterface::Stub>(
                    dan::DANInterface::NewStub(
                        grpc::CreateChannel(addr,
                            grpc::InsecureChannelCredentials())));

            dan::TableScan request;
            request.set_tname(tname);
            request.set_limit(limit);
            ClientContext ctx;

            node->table_scan(&ctx, request, &response);
        };

        StrVec nodes = table->getPartNodes();
        std::vector<dan::Table> results(nodes.size(), dan::Table());
        for (unsigned i = 0; i < nodes.size(); i++) {
            collect(nodes[i], table->getTableName(),
                    request->limit(), results[i]);
            response->MergeFrom(results[i]);
        }
    } else {
        // scan data in head node
        auto itr = table->begin();
        for ( ; itr; ++itr) {
            dan::Record* ret = response->add_v();
            generatePBRecord(ret, itr.getRecord());
        }
    }

    // response
    return Status::OK;
}

Status RTIServer::get_graph_info(ServerContext* context,
        const dan::Str* request, dan::GraphInfo* response) {
    // find table
    Graph* graph = Metadata::getGraphFromName(request->v());
    if (graph == nullptr) {
        return Status(grpc::INVALID_ARGUMENT,
                "not existing graph name: " + request->v());
    }

    StrVec vpname = graph->getVPName();
    std::vector<StrVec> vpfields = graph->getVPFields();
    StrVec epfields = graph->getEPFields();
    bool use_incoming = graph->useIncoming();

    // response
    response->set_gname(graph->getName());
    storage::replace_std_to_pb(vpname, response->mutable_vpname());
    for (auto fields : vpfields) {
        storage::replace_std_to_pb(fields, response->add_vpfields());
    }
    storage::replace_std_to_pb(epfields, response->mutable_epfields());
    response->set_incoming(use_incoming);

    return Status::OK;
}

Status RTIServer::get_num_vertex(ServerContext* context,
        const dan::Str* request, dan::Long* response) {
    // find graph 
    Graph* graph = Metadata::getGraphFromName(request->v());
    if (graph == nullptr) {
        return Status(grpc::INVALID_ARGUMENT,
                "not existing graph name: " + request->v());
    }

    // response
    response->set_v(graph->getNumVertex());
    return Status::OK;
}

Status RTIServer::get_num_edges(ServerContext* context,
        const dan::Str* request, dan::Long* response) {
    // find graph 
    Graph* graph = Metadata::getGraphFromName(request->v());
    if (graph == nullptr) {
        return Status(grpc::INVALID_ARGUMENT,
                "not existing graph name: " + request->v());
    }

    // response
    response->set_v(graph->getNumEdges());
    return Status::OK;
}

Status RTIServer::vertex_exists(ServerContext* context,
            const dan::VertexExists* request, dan::Code* response) {
    // find graph 
    Graph* graph = Metadata::getGraphFromName(request->gname());
    if (graph == nullptr) {
        return Status(grpc::INVALID_ARGUMENT,
                "not existing graph name: " + request->gname());
    }

    // get vertex
    EvalValue vkey = storage::generateEvalValue(request->vkey());
    bool res = graph->vertexExists(vkey,request->vpid());
    if(res){
        // response
        response->set_msg(dan::Code_Status_OK);
    }else{
        response->set_msg(dan::Code_Status_OBJECT_NOT_FOUND);
    }
    return Status::OK;
    
}

Status RTIServer::get_vertex(ServerContext* context,
            const dan::GetVertex* request, dan::Record* response) {
    // find graph 
    Graph* graph = Metadata::getGraphFromName(request->gname());
    if (graph == nullptr) {
        return Status(grpc::INVALID_ARGUMENT,
                "not existing graph name: " + request->gname());
    }

    // get vertex
    IntVec fids = storage::replace_pb_to_std(request->fids());
    EvalValue vkey = storage::generateEvalValue(request->vkey());
    EvalVec vertex = graph->getVertex(vkey,
                        request->vpid(), request->vid(), fids);
    generatePBRecord(response, vertex);

    // response
    return Status::OK;
}

Status RTIServer::adjacent_list(ServerContext* context,
        const dan::AdjacentList* request, dan::AdjacentListResult* response) {
    // find graph 
    Graph* graph = Metadata::getGraphFromName(request->gname());
    if (graph == nullptr) {
        return Status(grpc::INVALID_ARGUMENT,
                "not existing graph name: " + request->gname());
    }

    // get adjacent list
    EvalValue src = storage::generateEvalValue(request->src());
    IntVec fids = storage::replace_pb_to_std(request->fids());
    std::vector<EvalVec> dest;
    EvalVec eprop;
    graph->adjacentList(src, dest, fids,
            eprop, request->incoming(), request->vpid());

    // response
    storage::generatePBTable(response->mutable_dests(), dest);
    storage::generatePBRecord(response->mutable_eprops(), eprop);

    // response
    return Status::OK;
}

Status RTIServer::train_node_classifier(ServerContext* context,
        const dan::NodeClassifierInfo* request, dan::Code* response){
    
    Graph* graph = Metadata::getGraphFromName(request->gname());
    IntVec fids = storage::replace_pb_to_std(request->fids());
    int32_t input_size = request->input_size();
    int32_t hidden_size = request->hidden_size();
    int32_t num_class = request->num_class();
    float dropout = request->dropout();
    float learning_rate = request->learning_rate();
    int32_t data_update_interval = request->data_update_interval();
    const dan::ModelInfo& model_info = request->model_info();
    float t = 0.0;
    util::Timer timer;
    if(request->merge_manually()) {
        timer.start();
        graph->mergeManually(1);
        timer.stop();
        t += timer.getElapsedMilli();
    }
    std::string& log_file = const_cast<std::string&> (request->log_file());
    /**switch(model_info.m_case()) {
        case dan::ModelInfo::kAppnp: {
            const dan::APPNP& appnp_config = model_info.appnp();
            APPNPNodeClassifier classifier(graph, fids, input_size, hidden_size, num_class, 
                                        appnp_config.k(), appnp_config.alpha(), log_file, dropout, 
                                        graph->useIncoming(), learning_rate, data_update_interval);
            timer.start();
            classifier.init(-1, true);
            timer.stop();
            t += timer.getElapsedMilli();
            std::cout << "Start training APPNP \n";
            classifier.train(request->num_epochs());
            break;
        }
        case dan::ModelInfo::kGat: {
            const dan::GAT& gat_config = model_info.gat();
            GATNodeClassifier classifier(graph, fids, input_size, hidden_size, num_class, 
                                        gat_config.num_head(), graph->useIncoming(), 
                                        log_file, learning_rate, gat_config.agg(), data_update_interval);
            timer.start();
            classifier.init(gat_config.importance_spl(), true);
            timer.stop();
            t += timer.getElapsedMilli();
            std::cout << "Start training GAT \n";
            classifier.train(request->num_epochs());
            break;
        }
        default: {
            GCNSparseNodeClassifier classifier(graph, fids, input_size, hidden_size, num_class, 
                                            graph->useIncoming(), log_file, learning_rate, dropout,
                                            data_update_interval);
            timer.start();
            classifier.init(-1, true);
            timer.stop();
            t += timer.getElapsedMilli();
            std::cout << "Start training GCN \n";
            classifier.train(request->num_epochs());
            break;
        }
    }*/
    std::cout << "First data init time: " << t << " ms" << std::endl;
    return Status::OK;
}

/**
 * util functions
 */
Status RTIServer::set_cfield_info(
        CreateFieldVec& cfinfos, const PBFieldVec& schema) {
    for (const auto fdef : schema) {
        if (!fdef.ftype().compare("BIGINT")) {
            cfinfos.push_back(CreateFieldInfo(fdef.fname(), ValueType::VT_BIGINT));
        } else if (!fdef.ftype().compare("INT")) {
            cfinfos.push_back(CreateFieldInfo(fdef.fname(), ValueType::VT_INT));
        } else if (!fdef.ftype().compare("DECIMAL")) {
            cfinfos.push_back(CreateFieldInfo(fdef.fname(), ValueType::VT_DECIMAL));
        } else if (!fdef.ftype().compare("DOUBLE")) {
            cfinfos.push_back(CreateFieldInfo(fdef.fname(), ValueType::VT_DOUBLE));
        } else if (!fdef.ftype().compare("FLOAT")) {
            cfinfos.push_back(CreateFieldInfo(fdef.fname(), ValueType::VT_FLOAT));
        } else if (!fdef.ftype().compare("BOOL")) {
            cfinfos.push_back(CreateFieldInfo(fdef.fname(), ValueType::VT_BOOL));
        } else if (!fdef.ftype().compare("VARCHAR")) {
            cfinfos.push_back(CreateFieldInfo(fdef.fname(), ValueType::VT_VARCHAR));
        } else if (!fdef.ftype().compare("DATE")) {
            cfinfos.push_back(CreateFieldInfo(fdef.fname(), ValueType::VT_DATE));
        } else if (!fdef.ftype().compare("TIME")) {
            cfinfos.push_back(CreateFieldInfo(fdef.fname(), ValueType::VT_TIME));
        } else if (!fdef.ftype().compare("TIMESTAMP")) {
            cfinfos.push_back(CreateFieldInfo(fdef.fname(), ValueType::VT_TIMESTAMP));
        } else if (!fdef.ftype().compare("INTLIST")) {
            cfinfos.push_back(CreateFieldInfo(fdef.fname(), ValueType::VT_INTLIST));
        } else if (!fdef.ftype().compare("LONGLIST")) {
            cfinfos.push_back(CreateFieldInfo(fdef.fname(), ValueType::VT_LONGLIST));
        } else if (!fdef.ftype().compare("FLOATLIST")) {
            cfinfos.push_back(CreateFieldInfo(fdef.fname(), ValueType::VT_FLOATLIST));
        } else if (!fdef.ftype().compare("DOUBLELIST")) {
            cfinfos.push_back(CreateFieldInfo(fdef.fname(), ValueType::VT_DOUBLELIST));
        } else {
            return Status(grpc::INVALID_ARGUMENT, "not supported value type: " + fdef.ftype());
        }
    }

    return Status::OK;
}

bool RTIServer::releaseTable(Table* table) {
    if (table->isPartitioned()) {
        StrVec addrs = table->getPartNodes();
        for (unsigned i = 0; i < addrs.size(); i++) {
            auto node = std::unique_ptr<dan::DANInterface::Stub>(
                    dan::DANInterface::NewStub(
                        grpc::CreateChannel(addrs[i],
                            grpc::InsecureChannelCredentials())));
            dan::Str req;
            req.set_v(table->getTableName());
            dan::Code res;
            ClientContext ctx;

            node->drop_table(&ctx, req, &res);
            if (res.msg() != dan::Code_Status_OK) return false;
        }
    }
    Metadata::releaseTable(table->getTableName());
    return true;
}

bool RTIServer::releaseGraph(Graph* graph) {
    if (graph->isPartitioned()) {
        StrVec addrs = graph->getPartNodes();
        for (unsigned i = 0; i < addrs.size(); i++) {
            auto node = std::unique_ptr<dan::DANInterface::Stub>(
                    dan::DANInterface::NewStub(
                        grpc::CreateChannel(addrs[i],
                            grpc::InsecureChannelCredentials())));
            dan::Str req;
            req.set_v(graph->getName());
            dan::Code res;
            ClientContext ctx;

            node->drop_graph(&ctx, req, &res);

            if (res.msg() != dan::Code_Status_OK) return false;
        }
    }
    Metadata::releaseGraph(graph->getName());
    return true;
}

static void run_rti_server(std::string host, int port) {
    std::string address = host + ":" + std::to_string(port);
    int64_t max_message_size = 1024 * 1024 * 1024;
    RTIServer service(address);

    ServerBuilder builder;
	grpc::ResourceQuota quota;
	quota.SetMaxThreads(64);
	builder.SetResourceQuota(quota);
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    builder.SetMaxMessageSize(max_message_size);
    builder.SetMaxReceiveMessageSize(max_message_size);
    builder.SetMaxSendMessageSize(max_message_size);

    std::unique_ptr<Server> server(builder.BuildAndStart());
    Metadata::getMetadata();
    std::cout << "[INFO] RTI server listening on " << address << std::endl;
    server->Wait();
}

int main(int argc, char **argv) {
    // load the ip address from shell
    char addr[64];
    try {
        FILE *fp;
        fp = popen("ip addr | grep \"inet \" | grep brd | awk \'{print $2}\' | awk -F/ \'{print $1}\'", "r");
        char* ret = nullptr;
        ret = fgets(addr, sizeof(addr) -1 , fp);
        if (ret == nullptr) {
            std::string err("failed to local address of server, ");
            RTI_EXCEPTION(err);
        }
        pclose(fp);
        addr[strlen(addr)-1] = 0;
    } catch(...) {
        strcpy(addr, "localhost");
    }

    std::string host(addr);
    int port = 0;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--port") == 0) {
            port = atoi(argv[++i]);
        }
    }

    if (port == 0) {
        std::string err("should set port of rti server, ");
        RTI_EXCEPTION(err);
    }

    std::thread server([](std::string host_, int port_)
                    { run_rti_server(host_, port_); }, host, port);
    server.join();
    
    return 0;
}
