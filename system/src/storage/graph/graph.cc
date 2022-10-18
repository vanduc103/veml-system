// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

// Related include
#include "storage/graph/graph.h"
#include "storage/graph/subgraph.h"

// Project include
#include "common/def_const.h"
#include "storage/graph/subgraph.h"
#include "storage/graph/graph_delta.h"
#include "storage/relation/row_table.h"
#include "storage/util/util_func.h"
#include "storage/util/data_transfer.h"
// C & C++ system include
#include <fstream>
#include <random>
// Other include
#include <boost/serialization/vector.hpp>
#include <grpcpp/grpcpp.h>
#include <grpc/support/log.h>
#include <grpcpp/resource_quota.h>

// gRPC generated file
#include "interface/interface.grpc.pb.h"

namespace storage {

Graph::iterator::iterator(Graph* graph, bool use_eprop, bool incoming)
        : graph_(graph), itr_idx_(0), is_ended_(false),
        use_eprop_(use_eprop), incoming_(incoming) {
    if (!incoming_) {
        itr_.push_back(new GraphDelta::iterator(
                            graph_->delta_, graph_->use_eprop_));
        itr_.push_back(new GraphMain::iterator(graph_->main_));
    } else {
        itr_.push_back(new GraphDelta::iterator(
                            graph_->delta_in_, graph_->use_eprop_));
        itr_.push_back(new GraphMain::iterator(graph_->main_in_));
    }

    if (itr_[0]->isEnded()) {
        itr_idx_++;
        is_ended_ = itr_[1]->isEnded();
    }
}

Graph::iterator::iterator(Graph* graph, bool use_eprop, bool incoming,
            GraphIterator* delta_itr, GraphIterator* main_itr)
        : graph_(graph), itr_idx_(0), is_ended_(false),
        use_eprop_(use_eprop), incoming_(incoming) {
    itr_.push_back(delta_itr);
    itr_.push_back(main_itr);

    if (itr_[0]->isEnded()) {
        itr_idx_++;
        is_ended_ = itr_[1]->isEnded();
    }
}

int64_t Graph::iterator::getSrcIdx() {
    return itr_[itr_idx_]->getSrc();
}

int64_t Graph::iterator::getDestIdx() {
    return itr_[itr_idx_]->getDest();
}

int64_t Graph::iterator::getEpropIdx() {
    return itr_[itr_idx_]->getEprop();
}

LongVec Graph::iterator::getDestList() {
    return itr_[itr_idx_]->getDests();
}

LongVec Graph::iterator::getEpropList() {
    return itr_[itr_idx_]->getEprops();
}

EvalValue Graph::iterator::getSrcKey() {
    int64_t vid = itr_[itr_idx_]->getSrc();
    return graph_->getVertex(vid);
}

EvalValue Graph::iterator::getDestKey() {
    int64_t vid = itr_[itr_idx_]->getDest();
    return graph_->getVertex(vid);
}

EvalVec Graph::iterator::getSrc(IntVec fids) {
    int64_t vid = itr_[itr_idx_]->getSrc();
    return graph_->getVertex(vid, fids);
}

EvalVec Graph::iterator::getDest(IntVec fids) {
    int64_t vid = itr_[itr_idx_]->getDest();
    return graph_->getVertex(vid, fids);
}

EvalVec Graph::iterator::getEprop() {
    int64_t eprop_idx = itr_[itr_idx_]->getEprop();
    return graph_->edge_prop_->getRecord(eprop_idx);
}

Graph::iterator& Graph::iterator::operator++() {
    itr_[itr_idx_]->next();
    if (itr_[itr_idx_]->isEnded()) {
        if (itr_idx_ == 0) {
            itr_idx_++;
        }
        is_ended_ = itr_[itr_idx_]->isEnded();
    }

    return *this;
}

void Graph::iterator::nextList(bool skip_empty) {
    itr_[itr_idx_]->nextList(skip_empty);
    if (itr_[itr_idx_]->isEnded()) {
        if (itr_idx_ == 0) {
            itr_idx_++;
        }
        is_ended_ = itr_[itr_idx_]->isEnded();
    }
}

Graph* Graph::create(const std::string& name,
        std::string vpname, CreateFieldVec vpfields,
        int32_t vpfid, Table::Type vptype, 
        CreateFieldVec epfields, Table::Type eptype, 
        bool use_incoming, bool merge_bot_on,
        bool is_partition, int32_t part_type, StrVec nodes) {
    // set vpname
    if (vpname.find("$VP$") == std::string::npos) {
        vpname = name + "$VP$" + vpname;
    }

    // create new graph
    Graph* graph = new Graph(name,
                    vpname, vpfields, vpfid, vptype,
                    epfields, eptype,
                    use_incoming, merge_bot_on, is_partition,
                    part_type, nodes);
    // enroll to metadata
    graph->id_ = Metadata::addNewGraph(graph);
    graph->setLastMergeTimestamp(); // init last_merge_timestamp_
    return graph;
}

Graph::~Graph() {
    // deallocate delta
    if (delta_) {
        delete delta_;
    }

    // deallocate incoming delta if used
    if (delta_in_) {
        delete delta_in_;
    }

    // deallocate vertex property table
    for (auto vprop : vertex_prop_) {
        Metadata::releaseTable(vprop->getTableName());
    }

    // deallocate edge property table
    if (use_eprop_) Metadata::releaseTable(edge_prop_->getTableName());
}

void Graph::truncate() {
    WriteLock write_lock(mx_merge_);

    delete delta_;
    delta_ = new GraphDelta(name_, delta_id_++, use_eprop_);

    if (use_incoming_) {
        delete delta_in_;
        delta_in_ = new GraphDelta(name_, delta_id_++, use_eprop_);
    }

    for (auto vp: vertex_prop_){
        vp->truncate();
    }

    if (use_eprop_) {
        edge_prop_->truncate();
    }

    main_.truncate();
    main_in_.truncate();
}

bool Graph::defineVertex(std::string vpname,
        CreateFieldVec& vpfields, int32_t vpfid, Table::Type vptype) {
    // set vpname
    if (vpname.find("$VP$") == std::string::npos) {
        vpname = name_ + "$VP$" + vpname;
    }

    // create new vertex porperty table
    Table* vprop = Table::create(vptype, vpname, vpfields);
    if (!vprop) return false;
    vprop->createIndex(vpfid, true);

    // enroll to graph
    vertex_prop_.push_back(vprop);
    vertex_fid_.push_back(vpfid);
    main_.addNewVPPartition();
    main_in_.addNewVPPartition();

    return true;
}

bool Graph::vertexPropExists(std::string vpname){
    for(Table* prop : vertex_prop_){
        if(prop->getTableName() == vpname){
            return true;
        }
    }
    return false;
}

int32_t Graph::findVertexPropId(std::string vpname){
    int32_t i = -1;
    for(Table* prop : vertex_prop_){
        ++i;
        if(prop->getTableName() == vpname){
            return i;
        }
    }
    return -1;

}

bool Graph::importVertex(std::string vpname, std::string csv, std::string delim, bool header) {
    // find vertex property table
    Table* vprop = getVPTable(vpname);
    if (vprop == nullptr) {
        std::string err("not existing name for vertex property, ");
        return false;
    }

    if (part_nodes_.size() > 0) {
        // in case of graph is partitioned
    } else {
        // in case of graph is not partitioned
        return vprop->importCSV(csv, delim, 10000, 8, header, false);
    }

    return true;
}

int64_t Graph::insertVertex(EvalVec& values, int32_t vpid) {
    // check vpid is valid
    if (!this->validVPId(vpid)) return -1;

    ReadLock read_lock(mx_insert_);
    int32_t vprop_vid = -1;
    int32_t vfid = vertex_fid_[vpid];

    while (vprop_vid == -1) {
        vprop_vid = getVertexId(values[vfid], vpid);
        if (vprop_vid == -1) {
            // not existing vertex, insert new vertex
            vprop_vid = (int32_t)vertex_prop_[vpid]->insertRecord(values).lptr_;
        }
    }

    int64_t vid = ((int64_t)vpid << 32) + vprop_vid;
    return vid;
}

bool Graph::insertVertexBatch(std::vector<EvalVec>& values,
        IntVec& vpids, int32_t vpid, int32_t dop) {
    class InsertWorker : public RTIThread {
      public:
        InsertWorker(Graph* graph, std::vector<EvalVec>& values,
                IntVec& vpids, int32_t vpid, int start, int end)
            : graph_(graph), values_(values), vpids_(vpids),
            vpid_(vpid), start_(start), end_(end) {}

        void* run() {
            if (!vpids_.empty()) {
                for (int i = start_; i < end_; i++) {
                    graph_->insertVertex(values_[i], vpids_[i]);
                }
            } else {
                for (int i = start_; i < end_; i++) {
                    graph_->insertVertex(values_[i], vpid_);
                }
            }
            return nullptr;
        }

      private:
        Graph* graph_;
        std::vector<EvalVec>& values_;
        IntVec vpids_;
        int32_t vpid_;
        int start_;
        int end_;
    };

    size_t size = values.size();
    std::vector<InsertWorker> workers;
    int division = size / dop;
    int mod = size % dop;
    int start, end = 0;
    for (int i = 0; i < dop; i++) {
        start = end;
        end = start + division;
        if (i < mod) end++;
        workers.push_back(InsertWorker(this, values, vpids, vpid, start, end));
    }

    for (auto& w : workers) {
        w.start();
    }

    for (auto& w : workers) {
        w.join();
    }
    
    return true;
}

bool Graph::importEdge(std::string csv, int32_t src_vpid,
        int32_t dst_vpid, std::string delim, bool header,
        int batch, int dop) {
    // check vpid is valid
    if (!this->validVPId(src_vpid)) return false; 
    if (!this->validVPId(dst_vpid)) return false;

    class InsertWorker : public RTIThread {
      public:
        InsertWorker(Graph* graph,
                int32_t src_vpid, int32_t dst_vpid,
                int start, int end, StrVec& batch,
                std::string delim, bool eprop) : graph_(graph),
            src_vpid_(src_vpid), dst_vpid_(dst_vpid),
            start_(start), end_(end), batch_(batch),
            delim_(delim), eprop_(eprop) {}

        void* run() {
            FieldInfoVec finfo;
            finfo.push_back(graph_->getVPFinfo(src_vpid_));
            finfo.push_back(graph_->getVPFinfo(dst_vpid_));

            if (eprop_) {
                FieldInfoVec epfinfo = graph_->getEPFinfo();
                finfo.insert(finfo.end(), epfinfo.begin(), epfinfo.end());
            }

            for (int i = start_; i < end_; i++) {
                EvalVec values, eprop;
                Table::parseAndTranslate(batch_[i], finfo, values, delim_);
                if (eprop_) {
                    eprop.insert(eprop.begin(), values.begin()+2, values.end());
                }
                graph_->insertEdge(values[0], values[1],
                            eprop, src_vpid_, dst_vpid_);
            }
            return nullptr;
        }

      private:
        Graph* graph_;
        int32_t src_vpid_;
        int32_t dst_vpid_;
        int start_;
        int end_;
        StrVec& batch_;
        std::string delim_;
        bool eprop_;
    };

    std::ifstream data(csv);
    if (data.good() == false) {
        std::string err("invalid csv path: " + csv + "\n");
        RTI_EXCEPTION(err);
    }
    std::string buf;
    StrVec batch_buf;

    // skip header
    if (header) getline(data, buf);

    while (getline(data, buf)) {
        batch_buf.push_back(buf);
        if ((int)batch_buf.size() == batch) {
            int size = batch / dop;
            int remains = batch % dop;

            // generate insert workers
            std::vector<InsertWorker> workers;
            int start = 0;
            for (int i = 0; i < dop; i++) {
                int end = start + size;
                if (i < remains) end++;

                workers.push_back(InsertWorker(this, src_vpid, dst_vpid,
                            start, end, batch_buf, delim, use_eprop_));
                start = end;
            }

            // run insert workers
            for (auto& w: workers) {
                w.start();
            }

            for (auto& w: workers) {
                w.join();
            }

            // clear batch
            batch_buf.clear();
        }
    }

    // process remains
    if (batch_buf.size() > 0) {
        int size = batch_buf.size() / dop;
        int remains = batch_buf.size() % dop;

        // generate insert workers
        std::vector<InsertWorker> workers;
        int start = 0;
        for (int i = 0; i < dop; i++) {
            int end = start + size;
            if (i < remains) end++;

            workers.push_back(InsertWorker(this, src_vpid, dst_vpid,
                        start, end, batch_buf, delim, use_eprop_));
            start = end;
        }

        // run insert workers
        for (auto& w: workers) {
            w.start();
        }

        for (auto& w: workers) {
            w.join();
        }

        // clear batch
        batch_buf.clear();
    }

    return true;
}

bool Graph::insertEdge(int64_t src_id, int64_t dest_id,
        EvalVec& eprop, bool ignore_in, bool only_in) {
    if (src_id < 0) {
        std::string err("invalid source id @Graph::insertEdge");
        RTI_EXCEPTION(err);
        return false;
    }

    int64_t eprop_idx = -1;
    if (use_eprop_)
        eprop_idx = edge_prop_->insertRecord(eprop).lptr_;

    return this->insertEdge(src_id, dest_id, eprop_idx, ignore_in, only_in);
}

bool Graph::insertEdge(EvalValue src, EvalValue dest, EvalVec& eprop,
        int32_t src_vpid, int32_t dest_vpid, bool ignore_in, bool only_in) {
    // check vpid is valid
    if (!this->validVPId(src_vpid)) return false; 
    if (!this->validVPId(dest_vpid)) return false;

    // get vertex id
    int64_t src_id = getVertexId(src, src_vpid);
    int64_t dest_id = getVertexId(dest, dest_vpid);

    int32_t d_vpid = dest_id >> 32;
    if (d_vpid < 0 || d_vpid >= (int)vertex_prop_.size()) {
        std::cout << src_id << " " << dest_id << std::endl;
        std::string err("invalid vpid\n");
        RTI_EXCEPTION(err);
    }

    if (src_id < 0 || dest_id < 0) {
        std::string err("not existing vertex property @Graph::insertEdge\n");
        RTI_EXCEPTION(err);
        return false;
    }

    // insert edge property if used
    int64_t eprop_idx = -1;
    if (use_eprop_)
        eprop_idx = edge_prop_->insertRecord(eprop).lptr_;
    
    // insert edge
    return this->insertEdge(src_id, dest_id, eprop_idx, ignore_in, only_in);
}

bool Graph::insertEdge(EvalVec& src, EvalVec& dest,
        EvalVec& eprop, int32_t src_vpid, int32_t dest_vpid) {
    // check vpid is valid
    if (!this->validVPId(src_vpid)) return false;
    if (!this->validVPId(dest_vpid)) return false;

    int64_t src_id = insertVertex(src, src_vpid);
    int64_t dest_id = insertVertex(dest, dest_vpid);

    int64_t eprop_idx = -1;
    if (use_eprop_)
        eprop_idx = edge_prop_->insertRecord(eprop).lptr_;

    return insertEdge(src_id, dest_id, eprop_idx, false, false);
}

bool Graph::insertEdgeBatch(EvalVec& src, EvalVec& dest,
        std::vector<EvalVec>& eprop,
        IntVec& src_vpid, IntVec& dest_vpid,
        bool ignore_in, bool only_in, int dop) {
    // check argument
    size_t size = src.size();
    if ((size != dest.size())
            || (use_eprop_ && size != eprop.size())
            || (!use_eprop_ && eprop.size() != 0)) {
        std::string err("invalid size of parameter @Graph::insertEdge, ");
        RTI_EXCEPTION(err);
        return false;
    }
    bool hetero = src_vpid.size() == 0? false : true;

    class InsertWorker : public RTIThread {
      public:
        InsertWorker(Graph* graph, bool use_eprop, bool hetero,
                bool ignore_in, bool only_in, int start, int end,
                EvalVec& src, EvalVec& dest,
                std::vector<EvalVec>& eprop,
                IntVec& src_vpid, IntVec& dest_vpid)
            : graph_(graph), use_eprop_(use_eprop), hetero_(hetero),
            ignore_in_(ignore_in), only_in_(only_in),
            start_(start), end_(end), src_(src), dest_(dest), eprop_(eprop),
            src_vpid_(src_vpid), dest_vpid_(dest_vpid) {}

        void* run() {
            int64_t src_vpid = 0;
            int64_t dest_vpid = 0;
            for (int i = start_; i < end_; i++) {
                if (hetero_) {
                    src_vpid = src_vpid_[i];
                    dest_vpid = dest_vpid_[i];
                }

                if (use_eprop_) {
                    graph_->insertEdge(src_[i], dest_[i], eprop_[i],
                            src_vpid, dest_vpid, ignore_in_, only_in_);
                } else {
                    EvalVec eprop;
                    graph_->insertEdge(src_[i], dest_[i], eprop,
                            src_vpid, dest_vpid, ignore_in_, only_in_);
                }
            }
            return nullptr;
        }

      private:
        Graph* graph_;
        bool use_eprop_;
        bool hetero_;
        bool ignore_in_;
        bool only_in_;
        int start_;
        int end_;
        EvalVec& src_;
        EvalVec& dest_;
        std::vector<EvalVec>& eprop_;
        IntVec& src_vpid_;
        IntVec& dest_vpid_;
    };

    std::vector<InsertWorker> workers;
    int division = size / dop;
    int mod = size % dop;
    int start, end = 0;
    for (int i = 0; i < dop; i++) {
        start = end;
        end = start + division;
        if (i < mod) end++;

        workers.push_back(InsertWorker(
                    this, use_eprop_, hetero, ignore_in, only_in,
                    start, end, src, dest, eprop, src_vpid, dest_vpid));
    }

    for (auto& w : workers) {
        w.start();
    }

    for (auto& w : workers) {
        w.join();
    }
    
    return true;
}

size_t Graph::getNumVertex() {
    size_t cnt = 0;

    // in case that graph is not partitioned
    if (part_nodes_.size() == 0) {
        for (auto vp : vertex_prop_) {
            cnt += vp->getNumRecords();
        }
        return cnt;
    }

    if (!is_partition_) {
        // in case that graph in head agent
        dan::Str request;
        request.set_v(name_);

        for (auto node : part_nodes_) {
            grpc::ClientContext ctx;
            dan::Long response;
            auto stub = dan::DANInterface::NewStub(
                            grpc::CreateChannel(node,
                            grpc::InsecureChannelCredentials()));

            stub->get_num_vertex(&ctx, request, &response);
            cnt += response.v();
        }
        return cnt;

    } else {
        // in case that graph in worker agent
        for (auto vp : vertex_prop_) {
            auto itr = vp->begin();
            unsigned fid = vp->getNumFields() - 1;
            for ( ; itr; ++itr) {
                EvalValue is_replica = itr.getValue(fid);
                if (!is_replica.getBool().get()) {
                    cnt++;
                }
            }
        }
        return cnt;
    }
}

EvalVec Graph::getVertex(EvalValue& vkey,
            int32_t vpid, int64_t vid, IntVec& fids) {
    if (vpid >= 0) {
        // get vertex with vertex primary key
        vid = this->getVertexId(vkey, vpid);
    } else {
        vpid = vid >> 32;
    }

    // check vpid is valid
    if (!validVPId(vpid)) return {};

    if (fids.size() == 0) fillFids(fids, vpid);

    return this->getVertex(vid, fids);
}

void Graph::getVertexList(EvalVec& vkey, IntVec& vpid,
        std::vector<EvalVec>& vprop, IntVec fids) {
    for (int32_t i = 0; i < (int)vertex_prop_.size(); i++) {
        IntVec vprop_fids = fids;
        if (vprop_fids.size() == 0) fillFids(vprop_fids, i);

        auto itr = vertex_prop_[i]->begin();
        for ( ; itr; ++itr) {
            vkey.push_back(itr.getValue(vertex_fid_[i]));
            vpid.push_back(i);
            vprop.push_back(itr.getRecord(vprop_fids));
        }
    }
}

bool Graph::vertexExists(EvalValue& vkey, int32_t vpid){
    int64_t vid = -1;
    if (vpid >= 0) {
        // get vertex with vertex primary key
        vid = this->getVertexId(vkey, vpid);
    }
    if(vid != -1)
        return true;
    return false;
}

bool Graph::validVertexId(int64_t vid) {
    int32_t vpid = vid >> 32;
    int32_t vprop_vid = vid & 0xFFFFFFFF;

    if (!validVPId(vpid)) return false;

    return vertex_prop_[vpid]->isValid(vprop_vid);
}

size_t Graph::getNumEdges() {
    // in case that graph has not vertex property
    if (delta_ == nullptr) return 0;

    // in case that graph is not partitioned
    if (part_nodes_.size() == 0) {
        return delta_->size() + main_.size();
    }

    if (!is_partition_) {
        // in case that graph in head agent
        dan::Str request;
        request.set_v(name_);

        size_t cnt = 0;
        for (auto node : part_nodes_) {
            grpc::ClientContext ctx;
            dan::Long response;
            auto stub = dan::DANInterface::NewStub(
                            grpc::CreateChannel(node,
                            grpc::InsecureChannelCredentials()));

            stub->get_num_edges(&ctx, request, &response);
            cnt += response.v();
        }
        return cnt;

    } else {
        return delta_->size() + main_.size();
    }
}

Graph::iterator Graph::begin(bool incoming) {
    return iterator(this, use_eprop_, incoming);
}

std::vector<Graph::iterator> Graph::beginMulti(int dop, bool incoming) {
    std::vector<Graph::iterator> itrs;

    std::vector<GraphDelta::iterator*> delta_itrs;
    std::vector<GraphMain::iterator*> main_itrs;
    if (!incoming) {
        delta_itrs = delta_->beginMulti(dop);
        main_itrs = main_.beginMulti(dop);
    } else {
        delta_itrs = delta_in_->beginMulti(dop);
        main_itrs = main_in_.beginMulti(dop);
    }

    for (unsigned i = 0; i < delta_itrs.size(); i++) {
        itrs.emplace_back(this, use_eprop_, incoming,
                        delta_itrs[i], main_itrs[i]);
    }

    return itrs;
}

void Graph::adjacentList(EvalValue src,
        EvalVec& dest, bool incoming, int32_t vpid) {
    // get vertex-id of src value
    if (!this->validVPId(vpid)) return;
    int64_t src_id = getVertexId(src, vpid);
    if (src_id < 0) return;

    // get list of neighboring vertices
    LongVec dest_ids;
    this->adjacentList(src_id, dest_ids, incoming);

    // collect primary key of neighboring vertices
    dest.reserve(dest.size() + dest_ids.size());
    for (auto d : dest_ids) {
        dest.push_back(getVertex(d));
    }
}

void Graph::adjacentList(EvalValue src, std::vector<EvalVec>& dest,
        IntVec& fids, EvalVec& eprop, bool incoming, int32_t vpid) {
    // get vertex-id of src value
    if (!this->validVPId(vpid)) return;
    int64_t src_id = getVertexId(src, vpid);
    if (src_id < 0) return;

    // if fids is empty, get all property fields of vertex
    if (fids.size() == 0) fillFids(fids, vpid);

    // get list of neighboring vertices
    LongVec dest_ids, eprop_ids;
    this->adjacentList(src_id, dest_ids, eprop_ids, incoming);

    // collect property of neighboring vertices
    dest.reserve(dest.size() + dest_ids.size());
    for (auto d : dest_ids) {
        dest.push_back(getVertex(d, fids));
    }

    // collect property of edges
    eprop.reserve(eprop.size() + eprop_ids.size());
    for (auto e : eprop_ids) {
        eprop.push_back(edge_prop_->getValue(e, 0));
    }
}

void Graph::getEdgeList(EvalVec& src, EvalVec& dest) {
    for (auto itr = this->begin() ; itr; ++itr) {
        src.push_back(itr.getSrcKey());
        dest.push_back(itr.getDestKey());
    }
}

/**
* an implementation of bfs. Given src node, return the traversal orders
* if give dest = -1, mean traverse over all nodes, else stop when meet dest
*/
void Graph::bfs(int64_t src, int64_t dest){

}

/**
* an implementation of dfs
* task_id can be -1 => init new task
* parent_id = -1 means parent = source node else = component id or ...
*/
void Graph::dfs(int64_t src, int64_t dest){
    
}

/**
 * counting the number of triangle in the graph
 * return a number with a possibility of duplication
*/
uint64_t Graph::triangleCounting(int64_t from_vertex, int64_t to_vertex){
    return 0;
}

/**
 * counting the number of triangle in the graph
 * return a number with a possibility of duplication
*/
uint64_t Graph::triangleCounting(int64_t from_vertex, int64_t to_vertex, bool is_directed){
    return 0;
}

void Graph::randomWalkR(uint32_t k, int64_t seed_id, std::unordered_set<int64_t>& path, double rp){
    std::default_random_engine generator;
    std::uniform_int_distribution<int> uni_dist(0, 0xFFFFFFFF);
    std::bernoulli_distribution berl_dist(rp);
    
    uint32_t total = 1;
    size_t nV = getNumVertex();
    path.insert(seed_id);
    int64_t next_id = seed_id;
    while(total < k && total < nV){
        LongVec adj;
        adjacentList(next_id, adj, false);
        if(adj.size() != 0){
            uint64_t rad = (uint64_t) uni_dist(generator);
            uint64_t sel = rad % adj.size();
            next_id = adj[sel];
            path.insert(next_id);
            if(berl_dist(generator)){
                // restart
                next_id = seed_id;
            }
        }else{
            next_id = seed_id;
        }
        ++total;
    }
}

/**
 * Return a path from random walk with restart given a seed node
 * https://medium.com/@chaitanya_bhatia/random-walk-with-restart-and-its-applications-f53d7c98cb9
 * r = cWr+ (1-c)e, e[i] = 1 if i is the starting node else 0, r[i] denotes the probability of being at node i
*/
void Graph::randomWalkR(uint32_t k, EvalValue& seed_node, int32_t vpid, std::unordered_set<int64_t>& path, double rp){
    int64_t seed_id = getVertexId(seed_node, vpid);
    if(seed_id < 0) return;
    randomWalkR(k, seed_id, path, rp);
}

/**
 * return a subgraph using multiple randomWalk steps, use for hetgraph
*/
SubGraph* Graph::randomWalkSample(uint32_t k, std::vector<IntVec>& vfids, IntVec& efids, double rp, uint32_t first_k, uint32_t dop){
    //1. start from a random node
    std::default_random_engine generator;
    std::uniform_int_distribution<int> uni_dist(0, 0xFFFFFFFF);
    int32_t start_vpid = vertex_prop_.size() == 0 ? 0 : ((uint64_t) uni_dist(generator)) % vertex_prop_.size();
    // randomly start from a position in vprop table
    uint64_t rad = (uint64_t) uni_dist(generator);
    uint64_t l_seed_id = rad % vertex_prop_[start_vpid]->getNumRecords();
    int64_t seed_id = ((int64_t) start_vpid << 32) + l_seed_id;
    std::unordered_set<int64_t> seed_nodes;
    size_t count = 0;
    //2. sample the first walk   
    while(count < first_k){
        randomWalkR(k, seed_id, seed_nodes, rp);
        if(seed_nodes.size() >= 3) break;
        count++;
    }
    //3. sample multiple walks    
    std::vector<int64_t> seed_nodes_w (seed_nodes.begin(), seed_nodes.end());
    class SamplerWorker : public RTIThread{
        public:
            SamplerWorker(Graph* graph, std::vector<int64_t>& input_nodes, uint32_t start, uint32_t end, uint32_t k, double rp)
                : graph_(graph), input_nodes_(input_nodes), start_(start), end_(end), k_(k), rp_(rp){}
            void* run(){
                for(size_t i = start_; i < end_; i++){
                    graph_->randomWalkR(k_, input_nodes_[i], output_nodes_, rp_);
                }
                return nullptr;
            }
        
        Graph* graph_;
        std::vector<int64_t>& input_nodes_;
        uint32_t start_;
        uint32_t end_;
        uint32_t k_;
        double rp_;
        std::unordered_set<int64_t> output_nodes_;
    };
    if(seed_nodes.size() < 2){
        for(auto v : seed_nodes){
            randomWalkR(k, v, seed_nodes, rp);
        }
    }else{
        std::vector<SamplerWorker> workers;
        // use for worker to sample
        std::vector<int64_t> input_nodes(seed_nodes.begin(), seed_nodes.end());
        uint32_t size = input_nodes.size();
        uint32_t per_node = size / dop;
        uint32_t mod = size % dop;
        uint32_t start, end = 0;
        for(size_t i = 0; i < dop; i++){
            start = end;
            end = start + per_node;
            if(i < mod) end++;
            SamplerWorker worker(this, input_nodes, start, end, k, rp);
            workers.push_back(worker);
        }
        for(auto& worker: workers){
            worker.start();
        }
        for(auto& worker: workers){
            worker.join();
        }
        for(auto& worker: workers){
            seed_nodes.merge(worker.output_nodes_);
        }
    }
    //4. sampleNodeSubgraph
    std::map<int64_t, int64_t> newv2oldv;
    std::unordered_map<int32_t, int64_t> num_nodes;
    std::unordered_map<int64_t, LongVec> src, dest;
    EvalVec edata;
    sampleNodeSubgraph(seed_nodes, efids, newv2oldv, src, dest, edata, num_nodes);
    StrVec vprop_map;
    std::unordered_map<int32_t, std::string> vprop_ids;
    std::unordered_map<std::string, EvalVec> ndata;
    for(size_t i = 0; i < vfids.size(); i++){
        std::string tbl_name = vertex_prop_[i]->getTableName();
        tbl_name.replace(0, name_.size() + 1, "");
        EvalVec v_props;
        ndata[tbl_name] = v_props;
        vprop_map.push_back(tbl_name);
        vprop_ids[i] = tbl_name;
    }
    
    for(auto itr = newv2oldv.begin(); itr != newv2oldv.end(); ++itr){
        int32_t vpid = itr->second >> 32;
        EvalVec vdata = getVertex(itr->second, vfids[vpid]);
        std::string& tbl_name = vprop_ids[vpid];
        auto& v_props = ndata[tbl_name];
        v_props.insert(v_props.end(), vdata.begin(), vdata.end());
    }
    // clean empty vertex
    SubGraph* sg = new SubGraph(newv2oldv, src, dest, ndata, edata, num_nodes, vprop_map);
    return sg;
}

/**
 */
void Graph::sampleNodeSubgraph(std::unordered_set<int64_t> &seeding_nodes, IntVec& efids, 
                        std::map<int64_t, int64_t>& newv2oldv, 
                        std::unordered_map<int64_t, LongVec>& src, std::unordered_map<int64_t, LongVec>& dest,
                        EvalVec& edata, std::unordered_map<int32_t, int64_t>& num_nodes){
    std::unordered_map<int64_t, int64_t> oldv2newv; // old_logical_idx, new_id
    int64_t i = -1;
    // should be parallel
    for(auto itr : seeding_nodes){
        LongVec adj;
        if(!efids.size()){
            adjacentList(itr, adj, false);
            // loop over out-going adj of u; if v is in seeding_nodes
            if(adj.empty())
                continue;

            for(auto v : adj){
                if(seeding_nodes.count(v) > 0){
                    int32_t vpid_from = itr >> 32;
                    int32_t vpid_to = v >> 32;

                    if(oldv2newv.count(itr) == 0){
                        oldv2newv[itr] = ++i;
                        newv2oldv[i] = itr;
                        if(num_nodes.count(vpid_from) == 0){
                            num_nodes[vpid_from] = 1;
                        }else{
                            num_nodes[vpid_from] += 1;
                        }
                    }
                    if(oldv2newv.count(v) == 0){
                        oldv2newv[v] = ++i;
                        newv2oldv[i] = v;
                        if(num_nodes.count(vpid_to) == 0){
                            num_nodes[vpid_to] = 1;
                        }else{
                            num_nodes[vpid_to] += 1;
                        }
                    }
                    
                    int64_t map_id = ((int64_t)vpid_from << 32)  + vpid_to;
                    if(src.count(map_id) == 0){
                        LongVec sl;
                        src[map_id]  = sl;
                    }
                    if(dest.count(map_id) == 0){
                        LongVec dl;
                        dest[map_id]  = dl;
                    }
                    auto& s = src[map_id];
                    auto& d = dest[map_id];
                    s.push_back(oldv2newv[itr]);
                    d.push_back(oldv2newv[v]);
                }
            }
        }else{
            // implement later
        }
    }
}

/**
 * multi threading sampleNodeSubgraph => new vertex ids, num_nodes counting, edata
*/
void Graph::sampleNodeSubgraph(std::unordered_set<int64_t> &seeding_nodes, IntVec& efids, 
                        std::map<int64_t, int64_t>& newv2oldv, 
                        std::unordered_map<int64_t, LongVec>& src, std::unordered_map<int64_t, LongVec>& dest,
                        EvalVec& edata, std::unordered_map<int32_t, int64_t>& num_nodes, uint32_t dop){
    // 1. collect edge list
    class CollectWorker : public RTIThread{
        public:
            CollectWorker(Graph* graph, std::unordered_set<int64_t>& seeding_nodes, 
                        LongVec& input_nodes, IntVec& efids, uint32_t start, uint32_t end)
                : graph_(graph), seeding_nodes_(seeding_nodes), input_nodes_(input_nodes), efids_(efids), 
                start_(start), end_(end){}
            void* run(){
                for(size_t i = start_; i < end_; i++){
                    LongVec adj;
                    if(efids_.empty()){
                        int64_t u = input_nodes_[i];
                        graph_->adjacentList(u, adj, false);
                        // loop over out-going adj of u; if v is in seeding_nodes
                        if(adj.empty()) continue;

                        for(auto v : adj){
                            if(seeding_nodes_.count(v) > 0){
                                src_.push_back(u);
                                dst_.push_back(v);
                            }
                        }
                    }else{
                        // implement later
                    }
                }
                return nullptr;
            }
            Graph* graph_;
            std::unordered_set<int64_t>& seeding_nodes_;
            LongVec& input_nodes_;
            IntVec& efids_;
            uint32_t start_;
            uint32_t end_;
            LongVec src_, dst_;
    };
    uint32_t size = seeding_nodes.size();
    if(dop <= 1) dop = 1;
    uint32_t d = size / dop;
    uint32_t m = size % dop;
    uint32_t start, end = 0;
    std::vector<CollectWorker> cworkers;
    LongVec input_nodes (seeding_nodes.begin(), seeding_nodes.end());
    for(size_t i = 0; i < dop; i++){
        start = end;
        end = start + d;
        if(i < m) end++;
        cworkers.emplace_back(this, seeding_nodes, input_nodes, efids, start, end);
    }
    for(auto& w : cworkers) w.start();
    for(auto& w : cworkers) w.join();
    // 2. join edges
    std::unordered_map<int64_t, int64_t> oldv2newv;
    size_t idx = -1;
    for(auto& w : cworkers){
        uint32_t esz = w.src_.size();
        for(size_t i = 0; i < esz; i++){
            int64_t u = w.src_[i];
            int32_t vpid_from = u >> 32;
            if(oldv2newv.count(u) == 0){
                oldv2newv[u] = ++idx;
                newv2oldv[idx] = u;
                if(num_nodes.count(vpid_from) == 0){
                    num_nodes[vpid_from] = 1;
                }else{
                    num_nodes[vpid_from] += 1;
                }
            }

            int64_t v = w.dst_[i];
            int32_t vpid_to = v >> 32;
            if(oldv2newv.count(v) == 0){
                oldv2newv[v] = ++idx;
                newv2oldv[idx] = v;
                if(num_nodes.count(vpid_to) == 0){
                    num_nodes[vpid_to] = 1;
                }else{
                    num_nodes[vpid_to] += 1;
                }
            }
            int64_t map_id = ((int64_t)vpid_from << 32)  + vpid_to;
            if(src.count(map_id) == 0){
                LongVec sl;
                src[map_id]  = sl;
            }
            if(dest.count(map_id) == 0){
                LongVec dl;
                dest[map_id]  = dl;
            }
            auto& s = src[map_id];
            auto& d = dest[map_id];
            s.push_back(oldv2newv[u]);
            d.push_back(oldv2newv[v]);
        }
    }
}

/**
 * Return a subgraph wherein all dest nodes of edges are in the seeding nodes.
 *      Only work w/ homograph
 * Subgraph: (new_vids, src, dst, vertex_props, edge_props, num_vertex_in_graph)
 * len(new_vids) == num_vertex_in_graph
 */
SubGraph* Graph::sampleNodeSubgraph(LongVec &seeding_nodes,
                            IntVec& vfids,
                            IntVec& efids,
                            uint32_t dop){
    // 1. collect rti id of vertices
    class GatherWorker : public RTIThread{
      public:
        GatherWorker(Graph* graph, LongVec &seeding_nodes,
                uint64_t start, uint64_t end) 
            : graph_(graph), seeding_nodes_(seeding_nodes),
            start_(start), end_(end){};

        void* run(){ 
            for(size_t i = start_; i < end_; i++){
                EvalValue v = BigInt(seeding_nodes_[i]);
                int64_t vid = graph_->getVertexId(v, 0);
                if(vid >= 0)
                    raw2id_.insert(vid);
            }
            return nullptr;
        }
      
        Graph* graph_;
        LongVec &seeding_nodes_;
        uint64_t start_;
        uint64_t end_;
        std::unordered_set<int64_t> raw2id_;
    };

    if(dop == 0) dop = 1;
    std::vector<GatherWorker> gworkers;
    uint32_t size = seeding_nodes.size();
    uint32_t div = size / dop;
    uint32_t mod = size % dop;
    uint32_t start, end = 0;
    for(size_t i = 0; i < dop; i++){
        start = end;
        end = start + div;
        if(i < mod) end++;
        gworkers.emplace_back(this, seeding_nodes, start, end);
    }
    for(auto& w : gworkers){
        w.start();
    }
    for(auto& w : gworkers){
        w.join();
    }
    //2. merge set
    std::unordered_set<int64_t> raw2id; 
    if(dop == 1){
        raw2id.reserve(gworkers[0].raw2id_.size());
        std::copy(gworkers[0].raw2id_.begin(), gworkers[0].raw2id_.end(),
                std::inserter(raw2id, raw2id.begin()));
    }else{
        for(auto& w: gworkers){
            raw2id.merge(w.raw2id_);
        }
    }
    std::map<int64_t, int64_t> newv2oldv; // new_vid, old_logical_idx
    EvalVec edata;
    std::unordered_map<int64_t, LongVec> src, dest;
    std::unordered_map<int32_t, int64_t> num_nodes;
    if(dop <= 1){
        sampleNodeSubgraph(raw2id, efids, newv2oldv,
                src, dest, edata, num_nodes);
    }else{
        sampleNodeSubgraph(raw2id, efids, newv2oldv,
                src, dest, edata, num_nodes, dop);
    }
    EvalVec v_props;
    // copy data of node
    if(!vfids.empty()){
        v_props.reserve(vfids.size() * newv2oldv.size());
        for(auto itr = newv2oldv.begin(); itr != newv2oldv.end(); ++itr){
            EvalVec ndata = getVertex(itr->second, vfids);
            std::copy(ndata.begin(), ndata.end(), std::back_inserter(v_props));
        }
    } 
    std::unordered_map<std::string, EvalVec> ndata;
    std::string tbl_name = vertex_prop_[0]->getTableName();
    tbl_name.replace(0, name_.size() + 1, "");
    ndata[tbl_name] = v_props;
    StrVec vprop_map = {tbl_name};
    // clean empty vertex
    SubGraph* sg = new SubGraph(newv2oldv, src, dest, ndata, edata, num_nodes, vprop_map);
    return sg;
}

/**
 * Return a subgraph wherein all dest nodes of edges are in the seeding nodes. Work w/ heterograph
 * Subgraph: (new_vids, src, dst, vertex_props, edge_props, num_vertex_in_graph)
 * len(new_vids) == num_vertex_in_graph
 */
SubGraph* Graph::sampleHeteroSubGraph(LongVec &seeding_nodes,
                            std::vector<IntVec>& vfids,
                            IntVec& efids,
                            uint32_t dop){
    class GatherWorker : public RTIThread{
        public:
            GatherWorker(Graph* graph, LongVec &seeding_nodes, uint64_t start, uint64_t end) 
                : graph_(graph), seeding_nodes_(seeding_nodes), start_(start), end_(end){};
            void* run(){ 
                for(size_t i = start_; i < end_; i++){
                    EvalValue v = BigInt(seeding_nodes_[i]);
                    int64_t vid = graph_->getVertexId(v, 0);
                    if(vid >= 0)
                        raw2id_.insert(vid);
                }
                return nullptr;
            }
        
            Graph* graph_;
            LongVec &seeding_nodes_;
            uint64_t start_;
            uint64_t end_;
            std::unordered_set<int64_t> raw2id_;

    };
    if(dop == 0)
        dop = 1;
    std::vector<GatherWorker> gworkers;
    uint32_t size = seeding_nodes.size();
    uint32_t div = size / dop;
    uint32_t mod = size % dop;
    uint32_t start, end = 0;
    for(size_t i = 0; i < dop; i++){
        start = end;
        end = start + div;
        if(i < mod) end++;
        gworkers.emplace_back(this, seeding_nodes, start, end);
    }
    for(auto& w : gworkers){
        w.start();
    }
    for(auto& w : gworkers){
        w.join();
    }
    //2. merge set
    std::unordered_set<int64_t> raw2id; 
    if(dop == 1){
        raw2id.reserve(gworkers[0].raw2id_.size());
        std::copy(gworkers[0].raw2id_.begin(), gworkers[0].raw2id_.end(), std::inserter(raw2id, raw2id.begin()));
    }else{
        for(auto& w: gworkers){
            raw2id.merge(w.raw2id_);
        }
    }
    std::map<int64_t, int64_t> newv2oldv; // new_vid, old_logical_idx
    EvalVec edata;
    std::unordered_map<int64_t, LongVec> src, dest;
    std::unordered_map<int32_t, int64_t> num_nodes;
    sampleNodeSubgraph(raw2id, efids, newv2oldv, src, dest, edata, num_nodes);
    StrVec vprop_map;
    std::unordered_map<int32_t, std::string> vprop_ids;
    std::unordered_map<std::string, EvalVec> ndata;
    for(size_t i = 0; i < vfids.size(); i++){
        std::string tbl_name = vertex_prop_[i]->getTableName();
        tbl_name.replace(0, name_.size() + 1, "");
        EvalVec v_props;
        ndata[tbl_name] = v_props;
        vprop_map.push_back(tbl_name);
        vprop_ids[i] = tbl_name;
    }
    
    for(auto itr = newv2oldv.begin(); itr != newv2oldv.end(); ++itr){
        int32_t vpid = itr->second >> 32;
        EvalVec vdata = getVertex(itr->second, vfids[vpid]);
        std::string& tbl_name = vprop_ids[vpid];
        auto& v_props = ndata[tbl_name];
        v_props.insert(v_props.end(), vdata.begin(), vdata.end());
    }
    // clean empty vertex
    SubGraph* sg = new SubGraph(newv2oldv, src, dest, ndata, edata, num_nodes, vprop_map);
    
    return sg;
}

/**
 * Given nodes, randomly sample its neighbors; using for GraphSage like algorithms
 * Return a subgraph: (new_vids, src, dst, vertex_props, edge_props, num_vertex_in_graph)
 * len(new_vids) != num_vertex_in_graph
*/
SubGraph* Graph::sample_neighbors(LongVec &nodes,
                            IntVec& vfids,
                            IntVec& efids,
                            int32_t fanout,
                            bool incoming){
    /*
    // store mapping of old id => new id: use for making new ids
    std::unordered_map<int64_t, int64_t> oldv2newv; // old_logical_idx, new_vid
    // store mapping of new id to native vid: use for store trained embedding,..
    std::map<int64_t, int64_t> newv2oldv; // new_vid, old_logical_idx
    std::vector<int64_t> src;
    std::vector<int64_t> dest;
    for(auto u : nodes){
        EvalValue v = BigInt(u);
        u = getVertexId(v);
        // regularly, vid should >= 0; mean vertex found
        if(u < 0)
            continue;
        IntVec adj;
        if (!efids.size()){
            adjacentList(u, adj, incoming);
            int32_t adj_l = adj.size();
            if(oldv2newv.count(u) == 0){
                oldv2newv[u] = oldv2newv.size();
                newv2oldv[oldv2newv[u]] = u;
            }
            if(adj_l > 0){
                // create COO arrays of src & dst
                std::random_shuffle(adj.begin(), adj.end());
                adj_l = (adj_l < fanout || fanout == -1) ? adj_l : fanout;
                src.insert(src.end(), adj_l, oldv2newv[u]); 
                for(int t = 0; t < adj_l; ++t){
                    if(oldv2newv.count(adj[t]) == 0){
                        oldv2newv[adj[t]] = oldv2newv.size();
                    }
                    dest.push_back(oldv2newv[adj[t]]);
                }
            }else{
                // add self-loop if vertex has no adj
                src.push_back(oldv2newv[u]); 
                dest.push_back(oldv2newv[u]);
            }
        }else{
            // implement later
        }
    }
    EvalVec v_props;
    // copy data of node
    if(vfids.size()){
        for(auto itr = oldv2newv.begin(); itr != oldv2newv.end(); ++itr){
            EvalVec ndata = getVertex(itr->first, vfids);
            v_props.insert(v_props.end(), ndata.begin(), ndata.end());
        }
    }  
    EvalVec edata;
    SubGraph* sg;
    // std::cout << "num of input nodes " << newv2oldv.size() << " " << nodes.size() << std::endl;
    // std::cout << "num of subgraph nodes " << oldv2newv.size() << std::endl;
    if(incoming)
        sg = new SubGraph(newv2oldv, dest, src, v_props, edata, oldv2newv.size());
    else
        sg = new SubGraph(newv2oldv, src, dest, v_props, edata, oldv2newv.size());
    return sg;
    */
    return nullptr;
}

/**
 * multi-layer architecture
 */
void Graph::startMergeBot() {
    if (!merge_bot_on_.load()) {
        merge_bot_on_.store(true);
        merge_bot_.start();
    }
}

void Graph::stopMergeBot() {
    if (merge_bot_on_.load()) {
        merge_bot_on_.store(false);
        merge_bot_.join();
    }
}

/**
 * serialize/deserialize graph
 */
void Graph::serialize(boost::archive::binary_oarchive& oa) const {
    oa << name_;
    oa << id_;

    size_t num_vprop = vertex_prop_.size();
    oa << num_vprop;
    for (unsigned i = 0; i < num_vprop; i++) {
        uint32_t vprop_id = vertex_prop_[i]->getTableId();
        oa << vprop_id;
        oa << vertex_fid_[i];
    }

    oa << use_eprop_;
    if (use_eprop_) {
        uint32_t eprop_id = edge_prop_->getTableId();
        oa << eprop_id;
    }

    oa << use_incoming_;
    oa << delta_id_;
    bool merge_bot_on = merge_bot_on_.load();
    oa << merge_bot_on;
    
    oa << part_type_;
    oa << part_nodes_;
    oa << part_embed_;
    oa << part_filter_;
    oa << is_partition_;

    delta_->serialize(oa);
    if (use_incoming_) delta_in_->serialize(oa);
    main_.serialize(oa);
    main_in_.serialize(oa);
}

Graph* Graph::deserialize(boost::archive::binary_iarchive& ia) {
    std::string name;
    uint32_t id;
    size_t num_vprop;
    std::vector<Table*> vertex_prop;
    std::vector<int32_t> vertex_fid;

    uint32_t edge_prop_id;
    Table* edge_prop;
    bool use_eprop, use_incoming;

    GraphDelta *delta, *delta_in;
    uint32_t delta_id;
    bool merge_bot_on;
    PartitionType part_type;
    StrVec part_nodes;
    uint32_t part_embed;
    uint64_t part_filter;
    bool is_partition;

    ia >> name >> id;

    // deserialize vertex property tables
    ia >> num_vprop;
    for (unsigned i = 0; i < num_vprop; i++) {
        uint32_t vp_tid;
        int32_t vp_fid;
        ia >> vp_tid >> vp_fid;
        Table* t = Metadata::getTable(vp_tid);
        if (t == nullptr) return nullptr;

        vertex_prop.push_back(t);
        vertex_fid.push_back(vp_fid);
    }

    // deserialize edge property table
    ia >> use_eprop;
    if (use_eprop) {
        ia >> edge_prop_id;
        edge_prop = Metadata::getTable(edge_prop_id);
        if (edge_prop == nullptr) return nullptr;
    } else {
        edge_prop = nullptr;
    }

    ia >> use_incoming >> delta_id >> merge_bot_on;
    ia >> part_type >> part_nodes
        >> part_embed >> part_filter >> is_partition;

    // deserialize delta
    std::string dname;
    uint32_t did;
    bool deprop;
    uint32_t dhash_divisor;
    std::vector<uint32_t> hash_tid;
    ia >> dname >> did >> deprop >> dhash_divisor;
    for (unsigned i = 0; i < dhash_divisor; i++) {
        uint32_t tid;
        ia >> tid;
        hash_tid.push_back(tid);
    }
    delta = new GraphDelta(dname, did, deprop,
                dhash_divisor, hash_tid);

    if (use_incoming) {
        std::string dname;
        uint32_t did;
        bool deprop;
        uint32_t dhash_divisor;
        std::vector<uint32_t> hash_tid;
        ia >> dname >> did >> deprop >> dhash_divisor;
        for (unsigned i = 0; i < dhash_divisor; i++) {
            uint32_t tid;
            ia >> tid;
            hash_tid.push_back(tid);
        }

        delta_in = new GraphDelta(dname, did, deprop,
                    dhash_divisor, hash_tid);
    } else {
        delta_in = nullptr;
    }

    // restore graph
    Graph* g = new Graph(name, id, vertex_prop, vertex_fid,
                    edge_prop, use_eprop, use_incoming,
                    delta, delta_in, delta_id, merge_bot_on,
                    part_type, part_nodes, part_embed,
                    part_filter, is_partition);

    if (g != nullptr) {
        g->main_.deserialize(ia);
        g->main_in_.deserialize(ia);
        Metadata::addNewGraph(g, id);
    }

    return g;
}

/**
 * graph partition
 */
bool Graph::partition(std::string type, StrVec& nodes, int32_t dop) {
    if (nodes.size() == 0 || dop <= 0) {
        return false;
    }

    // 1. generate local partition
    part_nodes_ = nodes;
    std::vector<Graph*> partitions
        = generatePartition(type, nodes.size(), dop);

    // 2. transfer tables in graph
    TransferRequester requester(dop);
    for (unsigned part_id = 0; part_id < partitions.size(); part_id++) {
        Graph* part = partitions[part_id];
        // collect vertex property tables
        for (unsigned vpid = 0; vpid < part->vertex_prop_.size(); vpid++) {
            requester.add(nodes[part_id], part->vertex_prop_[vpid],
                    this->vertex_prop_[vpid]->getTableName());
        }

        // collect edge property table
        if (use_eprop_) {
            requester.add(nodes[part_id], part->edge_prop_,
                    this->edge_prop_->getTableName());
        }

        // collect tables in delta
        GraphDelta* delta = part->delta_;
        uint32_t delta_id = delta->id_;
        for (unsigned hid = 0; hid < delta->hash_table_.size(); hid++) {
            requester.add(nodes[part_id],
                    delta->hash_table_[hid].bucket_,
                    this->name_ + "$DELTA$" + std::to_string(delta_id)
                    + "$" + std::to_string(hid));
        }

        if (use_incoming_) {
            GraphDelta* delta = part->delta_in_;
            uint32_t delta_id = delta->id_;
            for (unsigned hid = 0; hid < delta->hash_table_.size(); hid++) {
                requester.add(nodes[part_id],
                        delta->hash_table_[hid].bucket_,
                        this->name_ + "$DELTA$" + std::to_string(delta_id)
                        + "$" + std::to_string(hid));
            }
        }
    }

    // 3. transfer graph partitions
    for (unsigned part_id = 0; part_id < partitions.size(); part_id++) {
        requester.add(nodes[part_id], partitions[part_id], this->name_);
    }

    requester.run();

    // 4. truncate data
    this->truncate();
    for (auto part : partitions) {
        Metadata::releaseGraph(part->getName());
    }

    return true;
}

std::vector<Graph*> Graph::generatePartition(
                        std::string type, int num, int dop) {
    // set partition type
    if (type == "EDGE_CUT_HASH") {
        part_type_ = EDGE_CUT_HASH;
    } else if (type == "EDGE_CUT_HASH_PROP") {
        part_type_ = EDGE_CUT_HASH_PROP;
    } else if (type == "VERTEX_CUT_RANGE_SRC") {
        part_type_ = VERTEX_CUT_RANGE_SRC;
    } else if (type == "VERTEX_CUT_RANGE_DST") {
        part_type_ = VERTEX_CUT_RANGE_DST;
    } else if (type == "VERTEX_CUT_HASH_SRC") {
        part_type_ = VERTEX_CUT_HASH_SRC;
    } else if (type == "VERTEX_CUT_HASH_DST") {
        part_type_ = VERTEX_CUT_HASH_DST;
    }

    // partition option
    part_embed_ = 32 - __builtin_clz(num);
    part_filter_ = 1;
    for (uint32_t i = 1; i < part_embed_; i++) {
        part_filter_ = part_filter_ << 1;
        part_filter_ += 1;
    }

    // create partition graph
    CreateFieldVec eprop;
    Table::Type eptype = Table::ROW;
    if (use_eprop_) {
        eprop = Table::createFieldInfo(edge_prop_->getFieldInfo());
        eptype = edge_prop_->getTableType();
    }

    std::vector<Graph*> partitions;
    for (int i = 0; i < num; i++) {
        std::string name = name_ + "$PART$" + std::to_string(i);
        Graph* g = Graph::create(name, "", {}, -1, Table::ROW,
                        eprop, eptype, use_incoming_, merge_bot_on_.load(),
                        true);
        g->part_embed_ = part_embed_;
        g->part_filter_ = part_filter_;
        g->part_nodes_ = part_nodes_;
        for (unsigned j = 0; j < vertex_prop_.size(); j++) {
            CreateFieldVec finfo = Table::createFieldInfo(
                                        vertex_prop_[j]->getFieldInfo());
            finfo.push_back(CreateFieldInfo(
                                "IS_REPLICA", ValueType::VT_BOOL));
            std::string vpname = vertex_prop_[j]->getTableName()
                                    + "$PART$" + std::to_string(i);
            g->defineVertex(vpname, finfo, vertex_fid_[j],
                            vertex_prop_[j]->getTableType());
        }
        partitions.push_back(g);
    }

    // generate partitions
    switch (part_type_) {
        case EDGE_CUT_HASH:
        case EDGE_CUT_HASH_PROP:
            edgeCutHash(partitions, dop);
            break;
        case VERTEX_CUT_RANGE_SRC:
            vertexCut(partitions, true, true, dop);
            break;
        case VERTEX_CUT_RANGE_DST:
            vertexCut(partitions, false, true, dop);
            break;
        case VERTEX_CUT_HASH_SRC:
            vertexCut(partitions, true, false, dop);
            break;
        case VERTEX_CUT_HASH_DST:
            vertexCut(partitions, false, false, dop);
            break;
        default: {
            std::string err("invalid partitioning type @Graph::partition, ");
            RTI_EXCEPTION(err);
        }
    }

    return partitions;
}

StrVec Graph::getVPName() const {
    StrVec ret;
    for (auto vp : vertex_prop_) {
        ret.push_back(vp->getTableName());
    }
    return ret;
}

std::vector<StrVec> Graph::getVPFields() const {
    std::vector<StrVec> ret;
    for (auto vp : vertex_prop_) {
        ret.push_back(vp->getFieldInfoStr());
    }
    return ret;
}

StrVec Graph::getEPFields() const {
    StrVec ret;
    if (use_eprop_) {
        return edge_prop_->getFieldInfoStr();
    }
    return ret;
}

StrVec Graph::findNode(EvalValue& src, bool incoming) {
    if (part_type_ == EDGE_CUT_HASH
        || part_type_ == VERTEX_CUT_RANGE_SRC 
        || part_type_ == VERTEX_CUT_RANGE_DST
        || (part_type_ == VERTEX_CUT_HASH_SRC && incoming)
        || (part_type_ == VERTEX_CUT_HASH_DST && !incoming)) {
        return part_nodes_;
    }

    // if part_type is VERTEX_CUT_HASH_SRC/DST, src is src/dst of edge;
    // can't find if src/dst doesn't have out-going, in-coming edges
    int32_t part_id = part_func_(src);
    
    if (part_id < 0 || part_id >= (int)part_nodes_.size()) {
        std::string err("invalid partition id @Graph::findNode, ");
        RTI_EXCEPTION(err);
    }

    return {part_nodes_[part_id]};
}

void Graph::getPagedArrayIds(std::vector<uint32_t>& pa_ids) const {
    main_.getPagedArrayIds(pa_ids);
    main_in_.getPagedArrayIds(pa_ids);
}

size_t Graph::getMemoryUsage(std::ostream& os, bool verbose, int level) {
    size_t delta_usage = delta_->getMemoryUsage(os, false, level+1);
    size_t delta_usage_in = use_incoming_?
                delta_in_->getMemoryUsage(os, false, level+1) : 0;
    size_t main_usage = main_.getMemoryUsage(os, false, level+1);
    size_t main_usage_in = use_incoming_?
                main_.getMemoryUsage(os, false, level+1) : 0;

    size_t vp_total = 0;
    std::vector<std::pair<std::string, size_t> > vp_usage;
    for (auto t : vertex_prop_) {
        size_t vp_mem = t->getMemoryUsage(os, true, level);
        vp_total += vp_mem;
        vp_usage.emplace_back(t->getTableName(), vp_mem);
    }

    size_t ep_mem = 0;
    if (use_eprop_) {
        ep_mem = edge_prop_->getMemoryUsage(os, false);
    }

    size_t total = sizeof(Graph) + delta_usage + delta_usage_in
                    + main_usage + main_usage_in + vp_total + ep_mem;

    if (verbose) {
        std::string indent = "";
        for (int i = 0; i < level; i++) indent += '\t';

        os << indent << "Graph Name: " << name_ << " => "
            << memory_unit_str(total) << '\n';

        os << indent << '\t' << "Delta(out) size: "
            << memory_unit_str(delta_usage) << '\n';
        os << indent << '\t' << "Delta(in) size: "
            << memory_unit_str(delta_usage_in) << '\n';
        os << indent << '\t' << "Main(out) size: "
            << memory_unit_str(main_usage) << '\n';
        os << indent << '\t' << "Main(in) size: "
            << memory_unit_str(main_usage_in) << '\n';

        for (auto itr : vp_usage) {
            os << indent << '\t' << "Vertex Property(" << itr.first<< "): "
                << memory_unit_str(itr.second) << '\n';
        }

        if (use_eprop_) {
            os << indent << '\t' << "Edge Property: "
                << memory_unit_str(ep_mem) << '\n';
        }
    }

    return total;
}

void Graph::mergeManually(size_t least) {
    mergeDelta(least, NUM_EDGES_FOR_SPLIT);
}

void Graph::mergeManually(size_t least, size_t num_split) {
    mergeDelta(least, num_split);
}

void Graph::mainOrder() {
    main_.orderCheck();
    main_in_.orderCheck();
}

/*
 * graph constructor
 */
Graph::Graph(const std::string& name,
        std::string vpname, CreateFieldVec vpfields,
        int32_t vpfid, Table::Type vptype, 
        CreateFieldVec epfields, Table::Type eptype, 
        bool use_incoming, bool merge_bot_on,
        bool is_partition, int32_t part_type, StrVec nodes)
    : name_(name), edge_prop_(nullptr), use_incoming_(use_incoming),
    delta_in_(nullptr), delta_id_(0), merge_bot_(MergeBot(this)),
    is_merging_(false), merge_bot_on_(false),
    part_type_((PartitionType)part_type),
    part_nodes_(nodes), is_partition_(is_partition) {
    
    // create vertex property table
    if (vpfields.size() != 0) {
        Table* vprop = Table::create(vptype, vpname, vpfields);
        vprop->createIndex(vpfid, true);
        vertex_prop_.push_back(vprop);
        vertex_fid_.push_back(vpfid);
        main_.addNewVPPartition();
        main_in_.addNewVPPartition();
    }

    // create edge property table
    use_eprop_ = (epfields.size() != 0);
    if (use_eprop_) {
        edge_prop_ = Table::create(eptype,
                        name_ + "$EP$", epfields);
    }

    // create delta
    delta_ = new GraphDelta(name_, delta_id_++, use_eprop_);
    if (use_incoming_)
        delta_in_ = new GraphDelta(name_, delta_id_++, use_eprop_);

    if (is_partition_) {
        part_embed_ = 32 - __builtin_clz(part_nodes_.size());
        part_filter_ = 1;
        for (uint32_t i = 1; i < part_embed_; i++) {
            part_filter_ = part_filter_ << 1;
            part_filter_ += 1;
        }
    }

    if (merge_bot_on) startMergeBot();
}

Graph::Graph(const std::string& name, uint32_t id,
        std::vector<Table*>& vertex_prop, std::vector<int32_t> vertex_fid,
        Table* edge_prop, bool use_eprop, bool use_incoming,
        GraphDelta* delta, GraphDelta* delta_in, uint32_t delta_id,
        bool merge_bot_on, PartitionType part_type, StrVec& part_nodes,
        uint32_t part_embed, uint64_t part_filter, bool is_partition)
    : name_(name), id_(id), vertex_prop_(vertex_prop), vertex_fid_(vertex_fid),
    edge_prop_(edge_prop), use_eprop_(use_eprop), use_incoming_(use_incoming),
    delta_(delta), delta_in_(delta_in), delta_id_(delta_id),
    merge_bot_(MergeBot(this)), is_merging_(false),
    merge_bot_on_(merge_bot_on),
    part_type_(part_type), part_nodes_(part_nodes),
    part_embed_(part_embed), part_filter_(part_filter),
    is_partition_(is_partition) {}


bool Graph::insertEdge(int64_t src_id, int64_t dest_id,
        int64_t eprop_idx, bool ignore_in, bool only_in) {
    ReadLock read_lock(mx_insert_);
    bool ret = true;
    
    if (use_incoming_ && !ignore_in) {
        if (only_in)
            ret = delta_in_->insertEdge(src_id, dest_id, eprop_idx);
        else
            ret = delta_in_->insertEdge(dest_id, src_id, eprop_idx);
    }

    if (!only_in)
        ret &= delta_->insertEdge(src_id, dest_id, eprop_idx);

    return ret;
}

int64_t Graph::getVertexId(EvalValue& value, int32_t vpid) {
    // check vpid is valid
    if (!this->validVPId(vpid)) return -1;

    LogicalPtr vprop_vid = LOGICAL_NULL_PTR;
    int32_t vfid = vertex_fid_[vpid];
    if (!vertex_prop_[vpid]->indexSearch(value, vfid, vprop_vid)) {
        return -1;
    }

    return ((int64_t)vpid << 32) + vprop_vid;
}

EvalValue Graph::getVertex(int64_t vertex_id) {
    if (vertex_id >= 0) { // get vertex from this node
        // devide vertex-id to vp-id and local-vid
        int32_t vpid = vertex_id >> 32;
        int32_t vprop_vid = vertex_id & 0xFFFFFFFF;

        // check vpid is valid
        if (!validVPId(vpid)) return EvalValue();

        // return vertex
        return vertex_prop_[vpid]->getValue(vprop_vid, vertex_fid_[vpid]);

    } else { // get vertex from another worker node
        using grpc::Channel;
        using grpc::ClientAsyncResponseReader;
        using grpc::ClientContext;

        vertex_id *= -1;
        uint32_t part_id = (vertex_id & part_filter_) - 1;
        int64_t dest_id = vertex_id >> part_embed_;
        int32_t vpid = dest_id >> 32;

        // create connection for the partition
        auto node = std::unique_ptr<dan::DANInterface::Stub>(
                        dan::DANInterface::NewStub(
                            grpc::CreateChannel(part_nodes_[part_id],
                                grpc::InsecureChannelCredentials()))); 
        ClientContext ctx;
        dan::GetVertex request;
        dan::Record response;

        // set graph name, dest-id in request
        request.set_gname(name_);
        request.set_vid(dest_id);
        request.set_vpid(-1);
        auto fids = request.mutable_fids();
        fids->add_v(vpid);

        node->get_vertex(&ctx, request, &response);
        return generateEvalRecord(response)[0];
    }
}

EvalVec Graph::getVertex(int64_t vertex_id, IntVec& fids) {
    if (vertex_id >= 0) { // get vertex from this node
        // devide vertex-id to vp-id and local-vid
        int32_t vpid = vertex_id >> 32;
        int32_t vprop_vid = vertex_id & 0xFFFFFFFF;

        // check vpid is valid
        if (!validVPId(vpid)) return {};

        // if fids is empty, get all property fields of vertex
        if (fids.size() == 0) fillFids(fids, vpid);

        // return vertex property
        return vertex_prop_[vpid]->getRecord(vprop_vid, fids);

    } else { // get vertex from another worker node
        vertex_id *= -1;
        uint32_t part_id = (vertex_id & part_filter_) - 1;
        int64_t dest_id = vertex_id >> part_embed_;

        // create connection for the partition
        auto node = std::unique_ptr<dan::DANInterface::Stub>(
                        dan::DANInterface::NewStub(
                            grpc::CreateChannel(part_nodes_[part_id],
                                grpc::InsecureChannelCredentials()))); 
        using grpc::Channel;
        using grpc::ClientAsyncResponseReader;
        using grpc::ClientContext;
        ClientContext ctx;
        dan::GetVertex request;
        dan::Record response;

        // set graph name, dest-id in request
        request.set_gname(name_);
        request.set_vid(dest_id);
        request.set_vpid(-1);
        replace_std_to_pb(fids, request.mutable_fids());

        node->get_vertex(&ctx, request, &response);
        return generateEvalRecord(response);
    }
}

void Graph::adjacentList(int64_t src, LongVec& dest, bool incoming) {
    if (incoming && delta_in_ == nullptr) {
        std::string err("invalid request for incoming edges, ");
        RTI_EXCEPTION(err);
    }

    ReadLock read_lock(mx_insert_);
    GraphDelta* delta = incoming? delta_in_ : delta_;
    GraphMain& main = incoming? main_in_ : main_;

    delta->adjacentList(src, dest);
    main.adjacentList(src, dest);
}

void Graph::adjacentList(int64_t src,
        LongVec& dest, LongVec& eprop, bool incoming) {
    if (incoming && delta_in_ == nullptr) {
        std::string err("invalid request for incoming edges, ");
        RTI_EXCEPTION(err);
    }

    ReadLock read_lock(mx_insert_);
    GraphDelta* delta = incoming? delta_in_ : delta_;
    GraphMain& main = incoming? main_in_ : main_;
    if (use_eprop_) {
        delta->adjacentList(src, dest, eprop);
        main.adjacentList(src, dest, eprop);
    } else {
        delta->adjacentList(src, dest);
        main.adjacentList(src, dest);
    }
}

void Graph::MergeWorker::collect(GraphPartition* old) {
    old->copyTo(old_src_, old_dest_, old_eprop_, start_, end_);
}

void Graph::MergeWorker::enroll(
        int64_t src, std::pair<LongList*, LongList*> edge) {
    edges_[src] = edge;
}

void* Graph::MergeWorker::run() {
    if (pos_ < 0) {
        new_ = new GraphPartition(use_eprop_, start_, end_, edges_);
    } else {
        new_ = new GraphPartition(use_eprop_, start_, end_,
                old_src_, old_dest_, old_eprop_, edges_);
    }
    return nullptr;
}

void* Graph::MergeBot::run() {
    while(graph_->merge_bot_on_.load()) {  // if not terminated
        bool check_merging = false;
        bool set_merging = true;

        // skip if merge is in progress
        if (graph_->is_merging_.compare_exchange_strong(check_merging, set_merging)) {
            graph_->mergeDelta();
            graph_->is_merging_.store(false);
        }
        RTIThread::sleep(GRAPH_MERGE_INTERVAL);
    }

    return nullptr;
}

void Graph::mergeDelta(size_t least, size_t num_split) {
    /**
     * 0. get exclusive access
     */
    WriteLock mergeLock(mx_merge_, try_to_lock);
    if (!mergeLock.owns_lock()) {
        return;
    }
    is_merging_.store(true);
    int numvp = vertex_prop_.size();

    /**
     * 1. collect edge-list to merge
     */
    class CollectWorker : public RTIThread {
      public:
        CollectWorker(int numvp, GraphDelta* delta,
                GraphMain& main, size_t least) : merge_info_(numvp), 
            min_(numvp, std::numeric_limits<int64_t>::max()),
            max_(numvp, std::numeric_limits<int64_t>::min()),
            delta_(delta), main_(main), least_(least) {}

        void* run() {
            delta_->collectMerge(min_, max_,
                    main_, merge_info_, least_);
            return nullptr;
        }

        GraphDelta::MergeInfo merge_info_;
        LongVec min_;
        LongVec max_;
        GraphDelta* delta_;
        GraphMain& main_;
        size_t least_;
    };

    std::vector<CollectWorker> cworkers;
    cworkers.emplace_back(numvp, delta_, main_, least);
    if (use_incoming_) {
        cworkers.emplace_back(numvp, delta_in_, main_in_, least);
    }
    for (auto& w: cworkers) w.start();
    for (auto& w: cworkers) w.join();

    /**
     * 2. generate merge worker for each partition
     */
    typedef std::vector<MergeWorker> MergeWorkerVec;
    std::vector<MergeWorkerVec> mworkers(numvp, MergeWorkerVec());
    std::vector<MergeWorkerVec> mworkers_in(numvp, MergeWorkerVec());

    // 2-1. generate merge worker of push_front partition
    for (int vpid = 0; vpid < numvp; vpid++) {
        if (main_.numPartition(vpid) == 0
                || cworkers[0].min_[vpid] < main_.getMin(vpid)) {
            int64_t min = cworkers[0].min_[vpid];
            int64_t max = main_.numPartition(vpid) == 0?
                            cworkers[0].max_[vpid]+1 : main_.getMin(vpid);
            Graph::gen_merge_worker_front_back(vpid, -1, min, max,
                    cworkers[0].merge_info_, mworkers[vpid], use_eprop_, num_split);
        }
        if (use_incoming_) {
            if (main_in_.numPartition(vpid) == 0
                    || cworkers[1].min_[vpid] < main_in_.getMin(vpid)) {
                int64_t min = cworkers[1].min_[vpid];
                int64_t max = main_in_.numPartition(vpid) == 0?
                            cworkers[1].max_[vpid]+1 : main_in_.getMin(vpid);
                Graph::gen_merge_worker_front_back(vpid, -1, min, max,
                    cworkers[1].merge_info_, mworkers_in[vpid], use_eprop_, num_split);
            }
        }
    }

    // 2-2. generate merge worker of replace partition
    for (int vpid = 0; vpid < numvp; vpid++) {
        Graph::gen_merge_worker_replace(vpid,
            cworkers[0].min_[vpid], cworkers[0].max_[vpid], main_,
            cworkers[0].merge_info_, mworkers[vpid], use_eprop_, num_split);
        if (use_incoming_) {
            Graph::gen_merge_worker_replace(vpid,
                cworkers[1].min_[vpid], cworkers[1].max_[vpid], main_in_,
                cworkers[1].merge_info_, mworkers_in[vpid], use_eprop_, num_split);
        }
    }

    // 2-3. generate merge worker of push_back partition
    for (int vpid = 0; vpid < numvp; vpid++) {
        if (cworkers[0].max_[vpid] >= main_.getMax(vpid)) {
            int64_t min = main_.getMax(vpid);
            int64_t max = cworkers[0].max_[vpid] + 1;
            Graph::gen_merge_worker_front_back(vpid, -2, min, max,
                    cworkers[0].merge_info_, mworkers[vpid], use_eprop_, num_split);
        }
        if (use_incoming_) {
            if (cworkers[1].max_[vpid] >= main_in_.getMax(vpid)) {
                int64_t min = main_in_.getMax(vpid);
                int64_t max = cworkers[1].max_[vpid] + 1;
                Graph::gen_merge_worker_front_back(vpid, -2, min, max,
                    cworkers[1].merge_info_, mworkers_in[vpid], use_eprop_, num_split);
            }
        }
    }

    /**
     * 3. do merge. create new partitions
     */
    WriteLock write_lock(mx_insert_);
    bool has_merge_data = false;
    for (auto& workers : mworkers) {
        for (auto& w : workers) w.start();
    }
    for (auto& workers : mworkers_in) {
        for (auto& w : workers) w.start();
    }

    for (auto& workers : mworkers) {
        for (auto& w : workers) {
            has_merge_data = true;
            w.join();
        }
    }
    for (auto& workers : mworkers_in) {
        for (auto& w : workers) {
            has_merge_data = true;
            w.join();
        }
    }
    if(has_merge_data) setLastMergeTimestamp();
    /**
     * 4. insert merge partitions to main
     */
    for (int vpid = 0; vpid < numvp; vpid++) {
        Graph::insert_merge_partition(vpid, main_, mworkers[vpid]);
        if (use_incoming_) {
            Graph::insert_merge_partition(vpid, main_in_, mworkers_in[vpid]);
        }
    }

    /**
     * 5. delete edges in deta
     */
    class DeleteWorker : public RTIThread {
      public:
        DeleteWorker(GraphDelta* delta, int32_t hashid, LongVec& srcids)
            : delta_(delta), hashid_(hashid), srcids_(srcids) {}

        void* run() {
            delta_->remove(hashid_, srcids_);
            return nullptr;
        }

      protected:
        GraphDelta* delta_;
        int32_t hashid_;
        LongVec& srcids_;
    };

    std::vector<DeleteWorker> dworkers;
    auto& deletes = cworkers[0].merge_info_.deletes_;
    for (auto ditr = deletes.begin(); ditr != deletes.end(); ++ditr) {
        dworkers.push_back(
                DeleteWorker(delta_, ditr->first, ditr->second));
    }
    if (use_incoming_) {
        auto& deletes = cworkers[1].merge_info_.deletes_;
        for (auto ditr = deletes.begin(); ditr != deletes.end(); ++ditr) {
            dworkers.push_back(
                    DeleteWorker(delta_in_, ditr->first, ditr->second));
        }
    }

    for (auto& w : dworkers) w.start();
    for (auto& w : dworkers) w.join();

    /**
     * 6. shrink delta
     */
    class ShrinkWorker : public RTIThread {
      public:
        ShrinkWorker(GraphDelta* delta, GraphDelta::iterator* itr)
            : delta_(delta), itr_(itr) {}
        
        void* run() {
            for ( ; (*itr_); itr_->nextList()) {
                EvalVec edge_list = itr_->getRecord();
                delta_->insertList(edge_list);
            }
            return nullptr;
        }
      private:
        GraphDelta* delta_;
        GraphDelta::iterator* itr_;
    };

    GraphDelta* new_delta = new GraphDelta(name_, delta_id_++, use_eprop_);
    std::vector<GraphDelta::iterator*> itrs = delta_->beginMulti(1);
    std::vector<ShrinkWorker> sworkers;
    for (unsigned i = 0; i < itrs.size(); i++) {
        sworkers.emplace_back(new_delta, itrs[i]);
    }
    for (auto& w : sworkers) w.start();
    for (auto& w : sworkers) w.join();

    delete delta_;
    delta_ = new_delta;

    if (use_incoming_) {
        GraphDelta* new_delta = new GraphDelta(name_, delta_id_++, use_eprop_);
        std::vector<GraphDelta::iterator*> itrs = delta_in_->beginMulti(1);
        std::vector<ShrinkWorker> sworkers;
        for (unsigned i = 0; i < itrs.size(); i++) {
            sworkers.emplace_back(new_delta, itrs[i]);
        }
        for (auto& w : sworkers) w.start();
        for (auto& w : sworkers) w.join();

        delete delta_in_;
        delta_in_ = new_delta;
    }
}

void Graph::gen_merge_worker_front_back(int vpid,
            int pos, int64_t min, int64_t max,
            GraphDelta::MergeInfo& merge_info,
            std::vector<MergeWorker>& workers, 
            bool use_eprop, size_t num_split) {
    auto itr = merge_info.map_[vpid].end();
    itr = merge_info.map_[vpid].find(pos);
    if (itr != merge_info.map_[vpid].end()) {
        GraphDelta::MergeInfo::Partition& partition = itr->second;
        size_t size = 0;
        int64_t i = min;

        workers.push_back(MergeWorker(vpid, pos, i, use_eprop));
        MergeWorker* worker = &workers.back();

        for ( ; i < max; i++) {
            auto itr = partition.edges_.find(i);
            if (itr == partition.edges_.end()) continue;

            if (size + itr->second.first->size() > num_split) {
                // size exceeds, move to next worker
                worker->setEnd(i);
                workers.push_back(MergeWorker(vpid, pos, i, use_eprop));
                worker = &workers.back();
                size = 0;
            }

            // enroll edge-list to merge worker
            worker->enroll(i, itr->second);
            size += itr->second.first->size();
        }
        worker->setEnd(i);
    }
}

void Graph::gen_merge_worker_replace(int vpid,
        int64_t min, int64_t max, GraphMain& main,
        GraphDelta::MergeInfo& merge_info,
        std::vector<MergeWorker>& workers, 
        bool use_eprop, size_t num_split) {
    auto itr = merge_info.map_[vpid].begin();
    for ( ; itr != merge_info.map_[vpid].end(); itr++) {
        int32_t id = itr->first;
        GraphDelta::MergeInfo::Partition& new_part = itr->second;
        if (id < 0) continue;

        GraphPartition* old_part = main.getPartition(vpid, id);
        int64_t part_min = old_part->getMin();
        int64_t part_max = old_part->getMax();
        if (part_min > max || part_max < min) continue;
        
        part_min = std::min(part_min, new_part.min_);
        part_max = std::max(part_max, new_part.max_);

        workers.push_back(MergeWorker(vpid, id, part_min, use_eprop));
        MergeWorker* worker = &workers.back();
        size_t size = 0;
        int64_t i = part_min;
        for ( ; i < part_max; i++) {
            size_t to_add = 0;

            // get size of new edge list
            auto itr = new_part.edges_.find(i);
            if (itr != new_part.edges_.end()) {
                to_add += itr->second.first->size();
            }

            // get size of existing edge list
            to_add += old_part->size(i);

            if (size + to_add > num_split) {
                worker->setEnd(i);
                worker->collect(old_part);
                workers.push_back(MergeWorker(vpid, id, i, use_eprop));
                worker = &workers.back();
                size = 0;
            }

            if (itr != new_part.edges_.end()) {
                worker->enroll(i, itr->second);
            }
            size += to_add;
        }
        worker->setEnd(i);
        worker->collect(old_part);
    }
}

void Graph::insert_merge_partition(int vpid, GraphMain& main,
        std::vector<MergeWorker>& workers) {
    int32_t nid = 0;
    int32_t pid = 0;

    // insert front partition
    while (nid < (int)workers.size() && workers[nid].pos() == -1) {
        main.addPartition(vpid, pid++, workers[nid++].get());
    }

    // insert replace partition
    while (nid < (int)workers.size() && workers[nid].pos() >= 0) {
        int32_t prev_pid = workers[nid].pos();

        while (pid < (int)main.numPartition(vpid)
                && main.getPartition(vpid, pid)->getId() < prev_pid) {
            pid++;
        }

        if (main.getPartition(vpid, pid)->getId() == prev_pid) {
            main.removePartition(vpid, pid);
        }

        while (nid < (int)workers.size() && workers[nid].pos() == prev_pid) {
            main.addPartition(vpid, pid++, workers[nid++].get());
        }
    }

    // insert backend partition
    while (nid < (int)workers.size()) {
        main.addPartition(vpid, main.numPartition(vpid), workers[nid++].get());
    }

    for (unsigned i = 0; i < main.numPartition(vpid); i++) {
        main.getPartition(vpid, i)->setId(i);
    }
}

void Graph::edgeCutHash(std::vector<Graph*>& targets, int dop) {
    // 1. create partitioning function
    PartitionFunc* part_func = HashMod::create(targets.size());
    part_func_ = PartitionFunc::generate(part_func);

    // type for vid mapping
    // <global_vid, <part_id, local_vid>
    typedef std::unordered_map<int64_t, LongPair> VidMap;
    VidMap vid_map;

    // 2. partition vertex
    //   edgeCut method determines the partition according to the vertex value
    for (int32_t vpid = 0; vpid < (int)vertex_prop_.size(); vpid++) {
        Table* vprop = vertex_prop_[vpid];
        auto itr = vprop->begin();
        for ( ; itr; ++itr) {
            EvalVec vertex = itr.getRecord();
            int64_t global_vid = ((int64_t)vpid << 32) + itr.getLptr();
            uint32_t part_id = 0;
            if (part_type_ == EDGE_CUT_HASH) {
                // select partition with global_vid;
                part_id = part_func_(BigInt(global_vid));
            } else if (part_type_ == EDGE_CUT_HASH_PROP) {
                // select partition with vertex property
                part_id = part_func_(vertex[vertex_fid_[vpid]]);
            }

            vertex.push_back(Bool(false));
            int64_t local_vid = targets[part_id]->insertVertex(vertex, vpid);

            vid_map[global_vid] = LongPair(part_id, local_vid);
        }
    }

    // 3. partition edge
    auto distribute = [](std::vector<Graph*>& targets,
            Graph::iterator itr, Table* eprop, VidMap& vid_map,
            bool use_eprop, uint32_t part_embed, bool incoming) {
        for ( ; itr; itr.nextList()) {
            // get source vertex
            int64_t src_id = itr.getSrcIdx();
            int64_t s_local_vid = vid_map[src_id].second;
            int s_part_id = vid_map[src_id].first;

            // get adjacent list
            LongVec dest, eprop_id;
            dest = itr.getDestList();

            if (use_eprop) {
                eprop_id = itr.getEpropList();
            }

            // distribute edges
            for (unsigned i = 0; i < dest.size(); i++) {
                int64_t d_local_vid = vid_map[dest[i]].second;
                int d_part_id = vid_map[dest[i]].first;

                if (s_part_id != d_part_id) {
                    d_local_vid = -1 * (
                        (d_local_vid << part_embed) + d_part_id + 1);
                }

                EvalVec eprops;
                if (use_eprop) {
                    eprops = eprop->getRecord(eprop_id[i]);
                }

                if (!incoming) {
                    targets[s_part_id]->insertEdge(s_local_vid,
                                            d_local_vid, eprops, true, false);
                } else {
                    targets[s_part_id]->insertEdge(s_local_vid,
                                            d_local_vid, eprops, false, true);
                }
            }
        }
    };

    distribute(targets, this->begin(false), edge_prop_,
            vid_map, use_eprop_, part_embed_, false);
    if (use_incoming_)
        distribute(targets, this->begin(true), edge_prop_,
                vid_map, use_eprop_, part_embed_, true);
}

void Graph::vertexCut(std::vector<Graph*>& targets,
        bool ignore_in, bool range_partition, int32_t dop) {
    GraphDelta* delta = ignore_in? delta_ : delta_in_;
    int num_partition = targets.size();

    // 1. create partitioning func
    PartitionFunc* part_func;
    if (range_partition) {
        int num_edges_dop = (delta->size() + main_.size()) / dop;
        LongVec splits(num_partition, num_edges_dop/num_partition);
        for (int i = 0; i < num_partition; i++) {
            // add remnant edges => nodes[0->n-1] accordingly
            if (i < (num_edges_dop%num_partition)) {
                splits[i]++;
            }
            // add up idx of delta's vertices will be assigned nodes
            // for ex. idx = 0->100 to n1, 101 -> 200 to n2
            if (i > 0) {
                splits[i] += splits[i-1];
            }
        }
        splits.back() = std::numeric_limits<int64_t>::max();
        part_func = Range::create(splits);
    } else {
        part_func = HashMod::create(num_partition);
    }
    part_func_ = PartitionFunc::generate(part_func);

    // 2. collect edges and assign to partition
    class CollectWorker : public RTIThread {
      public:
        CollectWorker(std::vector<Graph*>& targets,
                int num_partition, Graph::iterator& itr,
                std::vector<Table*>& vprop, std::vector<int32_t>& vfid,
                PartEvalFunc& part_func, bool use_eprop, bool range_partition)
            : targets_(targets), itr_(itr),
            partitions_(num_partition, Partition()),
            vprop_(vprop), vfid_(vfid), part_func_(part_func),
            use_eprop_(use_eprop), range_partition_(range_partition) {}
        
        void* run() {
            int64_t idx = 0;
            for ( ; itr_; itr_.nextList()) {
                int64_t src_id = itr_.getSrcIdx();
                int32_t vpid = src_id >> 32;
                int32_t vid = src_id & 0xFFFFFFFF;

                EvalVec vertex = vprop_[vpid]->getRecord(vid);
                int32_t part_id = -1;
                if (range_partition_) {
                    BigInt v(idx);
                    part_id = part_func_(v);
                } else {
                    EvalValue v = vprop_[vpid]->getValue(vid, vfid_[vpid]);
                    part_id = part_func_(vertex[vfid_[vpid]]);
                }
                // insert src vertex to partition
                vertex.push_back(Bool(false));
                targets_[part_id]->insertVertex(vertex, vpid);
                partitions_[part_id].src_set.insert(src_id);

                // get edge collection
                // if id of src vertex is not exists, create new one
                auto& edge_set = partitions_[part_id].edges.emplace(src_id,
                                    std::make_pair<LongVec, LongVec>
                                        (LongVec(), LongVec())).first->second;

                // get dest-id and eprop-idx of edge
                LongVec dest_list, eprop_list;
                dest_list = itr_.getDestList();
                if (use_eprop_) {
                    eprop_list = itr_.getEpropList();
                }
                for (unsigned j = 0; j < dest_list.size(); j++) {
                    int64_t dest_id = dest_list[j];
                    edge_set.first.push_back(dest_id);
                    if (use_eprop_) {
                        edge_set.second.push_back(eprop_list[j]);
                    }

                    int32_t d_vpid = dest_id >> 32;
                    if (d_vpid < 0 || d_vpid >= (int)vprop_.size()) {
                        std::string err("invalid vpid");
                        RTI_EXCEPTION(err);
                    }
                    partitions_[part_id].dest_set.insert(dest_id);
                    ++idx;
                }
            }
            return nullptr;
        }

        std::vector<Graph*>& targets_;
        Graph::iterator& itr_;
        std::vector<Partition> partitions_;
        std::vector<Table*>& vprop_;
        std::vector<int32_t>& vfid_;
        PartEvalFunc& part_func_;
        bool use_eprop_;
        bool range_partition_;
    };

    // 2-1. collect edges from delta
    std::vector<iterator> itrs = this->beginMulti(dop);
    std::vector<CollectWorker> cworkers;
    for (int i = 0; i < dop; i++) {
        cworkers.emplace_back(targets, num_partition,
                itrs[i], vertex_prop_, vertex_fid_,
                part_func_, use_eprop_, range_partition);
    }
    for (auto& w: cworkers) w.start();
    for (auto& w: cworkers) w.join();

    // 2-2. merge collections
    std::set<int64_t> active_vertex;
    std::vector<LongSet> src_set(num_partition, LongSet());
    std::vector<LongSet> dest_set(num_partition, LongSet());
    for (auto& w : cworkers) {
        for (int i = 0; i < num_partition; i++) {
            active_vertex.merge(w.partitions_[i].src_set);
            src_set[i].merge(w.partitions_[i].src_set);
            dest_set[i].merge(w.partitions_[i].dest_set);
        }
    }

    // 3. assign vertex which does not have edge
    for (int32_t vpid=0; vpid < (int)vertex_prop_.size(); vpid++) {
        Table* vprop = vertex_prop_[vpid];
        int64_t idx = 0;
        for (auto itr = vprop->begin(); itr; ++itr) {
            int64_t vprop_vid = itr.getLptr();
            int64_t vid = ((int64_t)vpid << 32) + vprop_vid;

            if (active_vertex.count(vid) <= 0) {
                EvalVec vertex = itr.getRecord();
                int32_t part_id = -1;
                if (range_partition) {
                    BigInt v(idx++);
                    part_id = part_func_(v);
                } else {
                    part_id = part_func_(vertex[vertex_fid_[vpid]]);
                }

                // insert vertex to partition
                vertex.push_back(Bool(false));
                targets[part_id]->insertVertex(vertex, vpid);

                src_set[part_id].insert(vid);
            }
            ++idx;
        }
    }

    // 4. trim replica vertex for each partition
    for (int i = 0; i < num_partition; i++) {
        LongSet& dest = dest_set[i];
        LongSet& src = src_set[i];

        for (auto d : dest) {
            if (src.count(d) <= 0) {
                int32_t vpid = d >> 32;
                int32_t vid = d & 0xFFFFFFFF;

                if (vpid < 0 || vpid >= (int)vertex_prop_.size()) {
                    std::string err("invalid vpid");
                    RTI_EXCEPTION(err);
                }

                EvalVec v = vertex_prop_[vpid]->getRecord(vid);
                v.push_back(Bool(true));

                targets[i]->insertVertex(v, vpid);
            }
        }
    }

    // 5. distribute edges to partitions
    class DistWorker : public RTIThread {
      public:
        DistWorker(int id , std::vector<Graph*>& targets,
                std::vector<Partition>& partitions,
                std::vector<Table*>& vprop, std::vector<int32_t>& vfid,
                Table* eprop, bool use_eprop,
                bool use_incoming, bool ignore_in)
            : id_(id), targets_(targets), partitions_(partitions),
            vprop_(vprop), vfid_(vfid), eprop_(eprop), use_eprop_(use_eprop),
            use_incoming_(use_incoming), ignore_in_(ignore_in) {}

        void* run() {
            for (unsigned i = 0; i < targets_.size(); i++) {
                EdgeMap& edges = partitions_[i].edges;
                auto eitr = edges.begin();
                for ( ; eitr != edges.end(); ++eitr) {
                    // set source vertex
                    int64_t src_id = eitr->first;
                    int32_t s_vpid = src_id >> 32;
                    int32_t s_vid = src_id & 0xFFFFFFFF;

                    EvalValue src
                        = vprop_[s_vpid]->getValue(s_vid, vfid_[s_vpid]);

                    // get adjacent list of source vertex
                    size_t size = eitr->second.first.size();
                    for (unsigned j = 0; j < size; j++) {
                        // set dest vertex
                        int64_t dest_id = eitr->second.first[j];
                        int32_t d_vpid = dest_id >> 32;
                        int32_t d_vid = dest_id & 0xFFFFFFFF;

                        EvalValue dest
                            = vprop_[d_vpid]->getValue(d_vid, vfid_[d_vpid]);

                        EvalVec eprop;
                        if (use_eprop_) {
                            int64_t eprop_id = eitr->second.second[j];
                            eprop = eprop_->getRecord(eprop_id);
                        }

                        if (ignore_in_) {
                            targets_[i]->insertEdge(src, dest, eprop,
                                    s_vpid, d_vpid, !use_incoming_, false);
                        } else {
                            targets_[i]->insertEdge(dest, src, eprop,
                                    d_vpid, s_vpid, !use_incoming_, false);
                        }
                    }
                }
            }

            return nullptr;
        }

        size_t size() {
            size_t sum = 0;
            for (auto p : partitions_) {
                sum += p.edges.size();
            }
            return sum;
        }
          
      private:
        int id_;
        std::vector<Graph*>& targets_;
        std::vector<Partition>& partitions_;
        std::vector<Table*>& vprop_;
        std::vector<int32_t>& vfid_;
        Table* eprop_;
        bool use_eprop_;
        bool use_incoming_;
        bool ignore_in_;
    };

    std::vector<DistWorker> dworkers;
    for (int i = 0; i < dop; i++) {
        dworkers.emplace_back(i, targets, cworkers[i].partitions_,
                vertex_prop_, vertex_fid_, edge_prop_, use_eprop_,
                use_incoming_, ignore_in);
    }

    for (auto& w: dworkers) w.start();
    for (auto& w: dworkers) w.join();
}

void Graph::fillFids(IntVec& fids, int32_t vpid) {
    Table* vprop = vertex_prop_[vpid];
    int32_t num_fields = is_partition_?
                vprop->getNumFields()-1 : vprop->getNumFields();
    for (int i = 0; i < num_fields; i++) {
        fids.push_back(i);
    }
}

bool Graph::validVPId(int32_t vpid) {
    if (vpid >= (int)vertex_prop_.size() || vpid < 0) {
        std::stringstream err;
        err << "invalid vertex property id, ";
        err << "#vprop(" << vertex_prop_.size() << "), ";
        err << "requested(" << vpid << "), ";
        RTI_EXCEPTION(err.str());
        return false;
    }

    return true;
}

Table* Graph::getVPTable(std::string vpname) {
    for (auto t : vertex_prop_) {
        if (t->getTableName() == vpname) {
            return t;
        }
    }
    
    vpname = name_ + "$VP$" + vpname;
    for (auto t : vertex_prop_) {
        if (t->getTableName() == vpname) {
            return t;
        }
    }

    return nullptr;
}

FieldInfo* Graph::getVPFinfo(int32_t vpid) {
    return vertex_prop_[vpid]->getFieldInfo(vertex_fid_[vpid]);
}

FieldInfoVec Graph::getEPFinfo() {
    if (edge_prop_ == nullptr) return FieldInfoVec();
    return edge_prop_->getFieldInfo();
}

}  // namespace storage
