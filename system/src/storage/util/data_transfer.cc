// Copyright 2021 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

// Related include
#include "storage/util/data_transfer.h"

// Project include
#include "storage/relation/table.h"
#include "storage/graph/graph.h"
#include "util/timer.h"

// Other include
#include <grpcpp/grpcpp.h>

// gRPC generated file
#include "interface/interface.grpc.pb.h"

namespace storage {

void TransferRequester::add(
        std::string addr, Table* table, std::string tname) {
    // register table to transfer requester
    table_.push_back(new TransferTable(addr, table, tname));

    std::vector<uint32_t> pa_ids;
    table->getPagedArrayIds(pa_ids);
    for (auto pa_id : pa_ids) {
        // register paged_array to transfer requester
        this->add(addr, pa_id);
    }
}

void TransferRequester::add(
        std::string addr, Graph* graph, std::string gname) {
    // register graph to transfer requester
    graph_.push_back(new TransferGraph(addr, graph, gname));

    std::vector<uint32_t> pa_ids;
    graph->getPagedArrayIds(pa_ids);
    for (auto pa_id : pa_ids) {
        // register paged_array to transfer requester
        this->add(addr, pa_id);
    }
}

void TransferRequester::add(std::string addr, uint32_t pa_id) {
    paged_array_.push_back(new TransferPA(addr, pa_id));

    PABase* pa = Metadata::getPagedArray(pa_id);
    auto sgmt_ids = pa->getAllSgmtIds();

    for (auto sgmt_id : sgmt_ids) {
        // register segment to transfer requester
        segment_.push_back(new TransferSGMT(addr, pa_id, sgmt_id));
    }
}

void* TransferRequester::TransferWorker::run() {
    for (int i = start_; i < end_; i++) {
        TransferObject* object = objects_[i];
        dan::TransferBinary binary;

        // set type
        binary.set_type((int)object->type_);

        switch (object->type_) {
            case TransferObject::TABLE:
            {
                TransferTable* ttable
                    = reinterpret_cast<TransferTable*>(object);
                // set name
                binary.set_name(ttable->tname_);
                // serialize table
                std::ostringstream ss;
                boost::archive::binary_oarchive oa(ss);
                ttable->table_->serialize(oa);
                binary.mutable_binary()->set_v(ss.str());
                break;
            }
            case TransferObject::GRAPH:
            {
                TransferGraph* tgraph 
                    = reinterpret_cast<TransferGraph*>(object);
                // set name
                binary.set_name(tgraph->gname_);
                // serialize graph
                std::ostringstream ss;
                boost::archive::binary_oarchive oa(ss);
                tgraph->graph_->serialize(oa);
                binary.mutable_binary()->set_v(ss.str());
                break;
            }
            case TransferObject::PA:
            {
                TransferPA* tpa
                    = reinterpret_cast<TransferPA*>(object);
                PABase* pa = Metadata::getPagedArray(tpa->pa_id_);
                // set pa_id
                binary.set_pa_id(tpa->pa_id_);
                // serialize paged_array
                std::ostringstream ss;
                boost::archive::binary_oarchive oa(ss);
                pa->serialize(oa);
                binary.mutable_binary()->set_v(ss.str());
                break;
            }
            case TransferObject::SGMT:
            {
                TransferSGMT* tsgmt
                    = reinterpret_cast<TransferSGMT*>(object);
                PABase* pa = Metadata::getPagedArray(tsgmt->pa_id_);
                // set pa_id, sgmt_id
                binary.set_pa_id(tsgmt->pa_id_);
                binary.set_sgmt_id(tsgmt->sgmt_id_);
                // serialize segment
                std::ostringstream ss;
                boost::archive::binary_oarchive oa(ss);
                pa->serialize(tsgmt->sgmt_id_, oa);
                binary.mutable_binary()->set_v(ss.str());
                break;
            }
        }

        // transfer data
        auto stub = std::unique_ptr<dan::DANInterface::Stub>(
                        dan::DANInterface::NewStub(
                            grpc::CreateChannel(object->addr_,
                                grpc::InsecureChannelCredentials())));
        grpc::ClientContext ctx;
        dan::Code response;
        stub->transfer_data(&ctx, binary, &response);
    }

    return nullptr;
}

void TransferRequester::run() {
    auto transfer = [](std::vector<TransferObject*>& objects, int dop) {
        int num_task = objects.size();
        int division = num_task / dop;
        int mod = num_task % dop;

        std::vector<TransferWorker> workers;
        int start, end = 0;
        for (int i = 0; i < dop; i++) {
            start = end;
            end = start + division;
            if (i < mod) end++;
            workers.emplace_back(start, end, objects);
        }

        for (auto& w: workers) w.start();
        for (auto& w: workers) w.join();
    };

    // transfer table
    transfer(table_, dop_);

    // transfer graph
    transfer(graph_, dop_);

    // transfer paged_array
    transfer(paged_array_, dop_);

    // transfer segment
    transfer(segment_, dop_);

    // request rebuild of tables
    class Rebuild : public RTIThread {
      public:
        Rebuild(int start, int end, std::vector<TransferObject*>& object)
            : start_(start), end_(end), object_(object) {}

        void* run() {
            for (int i = start_; i < end_; i++) {
                TransferTable* ttable
                    = reinterpret_cast<TransferTable*>(object_[i]);

                auto stub = std::unique_ptr<dan::DANInterface::Stub>(
                                dan::DANInterface::NewStub(
                                    grpc::CreateChannel(ttable->addr_,
                                        grpc::InsecureChannelCredentials())));
                dan::Str request;
                request.set_v(ttable->tname_);
                grpc::ClientContext ctx;
                dan::Code response;
                stub->set_partition(&ctx, request, &response);
            }
            return nullptr;
        }

      private:
        int start_;
        int end_;
        std::vector<TransferObject*>& object_;
    };

    std::vector<Rebuild> workers;
    int num_table = table_.size();
    int division = num_table / dop_;
    int mod = num_table % dop_;
    int start, end = 0;
    for (int i = 0; i < dop_; i++) {
        start = end;
        end += division;
        if (i < mod) end++;

        workers.emplace_back(start, end, table_);
    }

    for (auto& w : workers) w.start();
    for (auto& w : workers) w.join();
}

void TransferHandler::run() {
    std::stringstream ss;
    ss << binary_;
    boost::archive::binary_iarchive ia(ss);

    switch (type_) {
        case TransferRequester::TransferObject::TABLE:
        {
            Table* t = Table::deserialize(ia);
            std::vector<uint32_t> pa_ids;
            t->getPagedArrayIds(pa_ids);
            Metadata::resetTableName(t->getTableName(), name_);
            break;
        }
        case TransferRequester::TransferObject::GRAPH:
        {
            Graph* g = Graph::deserialize(ia);
            Metadata::resetGraphName(g->getName(), name_);
            break;
        }
        case TransferRequester::TransferObject::PA:
        {
            PABase* pa = Metadata::getPagedArray(pa_id_);
            pa->deserialize(ia);
            break;
        }
        case TransferRequester::TransferObject::SGMT:
        {
            PABase* pa = Metadata::getPagedArray(pa_id_);
            pa->deserialize(sgmt_id_, ia);
            break;
        }
    }
}

}  // namespace storage
