#include "storage/graph/graph_training_data.h"
#include "util/timer.h"

#include <torch/torch.h>

using namespace torch::indexing;

namespace storage {

void* EdgeCollect::run() {
    srcs.reserve(num_v_);
    if(rank_ == 0) srcs.push_back(0);
    out_degree.reserve(num_v_);
    for(; *itr_; (*itr_).nextList(false)) {
        int64_t src_id = (*itr_).getSrc();
        if(!graph_->validVertexId(src_id) || (*oldv2newv_).count(src_id) == 0) continue;
        
        int32_t src_idx = (*oldv2newv_)[src_id];
        LongVec dest = (*itr_).getDests();
        if(!dest.empty()) {
            int32_t dsize = dest.size();
            if(importance_spl_ > 0 && dsize > importance_spl_) {
                dsize = importance_spl_;
                random_unique(dest.begin(), dest.end(), importance_spl_);
            }
            if(edge_ops_) src_os.push_back(src_idx);
            std::set<int32_t> check_dest;
            check_dest.insert(src_idx);
            for(int32_t i = 0; i < dsize; i++) {
                int64_t d = dest[i];
                if((*oldv2newv_).count(d) == 0) continue;
                int32_t d_idx = (*oldv2newv_)[d];
                if(check_dest.count(d_idx) == 0) {
                    check_dest.insert(d_idx);
                    if(edge_ops_) src_os.push_back(src_idx);
                }
            }
            dsize = check_dest.size();
            float dsize_sqrt = 1/sqrt(dsize);
            for(int32_t i = 0; i < dsize; i++) out_degree.push_back(dsize_sqrt);
            dests.reserve(dests.size() + dsize);
            std::copy(check_dest.begin(), check_dest.end(), std::back_inserter(dests));
        } else {
            dests.push_back(src_idx);
            out_degree.push_back(1.0);
            if(edge_ops_) src_os.push_back(src_idx);
        }
        srcs.push_back(dests.size());
    }
    srcs.shrink_to_fit();            
    return nullptr;
}

int32_t TrainingDataManager::loadVertexContent(GraphMain& main, IntVec& fids) {
    if(!oldv2newv_->empty()) oldv2newv_->clear();
    uint32_t num_v = graph_->getNumVertex();
    
    TensorVec features;
    LongVec labels;
    features.reserve(num_v);
    labels.reserve(num_v);

    GraphMain::iterator itr = main.begin();
    int32_t t = 0;
    for(; itr; itr.nextList(false)) {
        int64_t src_id = itr.getSrc();
        if(graph_->validVertexId(src_id)) {
            EvalVec vertex = graph_->getVertex(src_id, fids);
            labels.push_back(vertex[0].getBigInt().get());
            FloatList& dbl = vertex[1].getFloatList().get();                        
            torch::Tensor tensor = dbl.toTensor();
            features.push_back(tensor);
            (*oldv2newv_)[src_id] = t++;
        }
    }
    features.shrink_to_fit();
    labels.shrink_to_fit();
    uint32_t dim = features[0].size(0);
    X_ = torch::cat(features).reshape({t, dim});
    X_.set_requires_grad(false);
    label_ = torch::tensor(labels, torch::kInt64);
    return t;
}

void TrainingDataManager::buildTransposedCSR(IntVec& srcs, IntVec& dests,
                                            torch::Tensor& st, torch::Tensor& dt, torch::Tensor& vt) {
    // build src
    int src_sz = srcs.size();
    int dest_sz = dests.size();
    IntVec tsrcs(src_sz, 0), tdests(dest_sz, 0);
    for(auto d : dests) {
        ++tsrcs[d+1];
    }
    int num_v = src_sz-1;
    IntVec c(num_v, 0);
    for(int i = 1; i < src_sz; i++) {
        tsrcs[i] += tsrcs[i-1];
        c[i-1] = tsrcs[i-1];
    }
    // built dest
    IntVec tv;
    tv.reserve(dest_sz);
    for(int i = 0; i < num_v; i++) {
        for(int j = srcs[i]; j < srcs[i+1]; j++) {
            int new_index = c[dests[j]]++;
            tdests[new_index] = i;
            tv.push_back(new_index);
        }
    }
    st = torch::tensor(tsrcs, torch::kInt32);
    dt = torch::tensor(tdests, torch::kInt32);
    vt = torch::tensor(tv, torch::kInt32);
}

void TrainingDataManager::buildTransposedCSR(IntVec& srcs, IntVec& dests, FloatVec& values, 
                                    torch::Tensor& st, torch::Tensor& dt, torch::Tensor& vt) {
    // build src
    int src_sz = srcs.size();
    int dest_sz = dests.size();
    IntVec tsrcs(src_sz, 0), tdests(dest_sz, 0);
    for(auto d : dests) {
        ++tsrcs[d+1];
    }
    int num_v = src_sz-1;
    IntVec c(num_v, 0);
    IntVec in_deg(num_v, 0);
    for(int i = 1; i < src_sz; i++) {
        in_deg[i-1] = tsrcs[i];
        tsrcs[i] += tsrcs[i-1];
        c[i-1] = tsrcs[i-1];
    }
    // built dest
    FloatVec tvalues(dest_sz, 0.0);
    for(int i = 0; i < num_v; i++) {
        for(int j = srcs[i]; j < srcs[i+1]; j++) {
            int new_index = c[dests[j]]++;
            tdests[new_index] = i;
            float norm_deg = values[j] * 1/sqrt(in_deg[i]); // sqrt(in_degree_v) * sqrt(out_degree_u)
            tvalues[new_index] = norm_deg; 
            values[j] = norm_deg;
        }
    }
    st = torch::tensor(tsrcs, torch::kInt32);
    dt = torch::tensor(tdests, torch::kInt32);
    vt = torch::tensor(tvalues, torch::kFloat32);
}

void TrainingDataManager::initializeGraphIndices(IntVec& srcs, IntVec& dests, FloatVec& out_degree, IntVec& src_os, bool edge_ops){
    S_ = torch::tensor(srcs, torch::kInt32);
    D_ = torch::tensor(dests, torch::kInt32);
    if(edge_ops) { 
        src_os.shrink_to_fit();
        So_ = torch::tensor(src_os, torch::kInt32);
    }
    // build A_T for backward
    if(!edge_ops) {
        buildTransposedCSR(srcs, dests, out_degree, St_, Dt_, Vt_);
    } else {
        buildTransposedCSR(srcs, dests, St_, Dt_, Vt_);
    }
    V_ = torch::tensor(out_degree, torch::kFloat32);
}

void TrainingDataManager::generateCSR(IntVec& fids, bool incoming, int importance_spl, bool edge_ops) {
    WriteLock csr_lock(mx_csr_);
    uint32_t num_v = graph_->getNumVertex(); 
    if(num_v == 0) {
        RTI_EXCEPTION("Graph is empty.\n");
        return;
    }
    int64_t last_merge_timestamp = graph_->getLastMergeTimestamp();
    if(last_update_timestamp_ > 0 && last_update_timestamp_ == last_merge_timestamp) return;
    last_update_timestamp_ = last_merge_timestamp;
    GraphMain& main = incoming ? graph_->main_in_ : graph_->main_;
    
    int64_t max = main.getMax(0);
    if(max < 0){
        RTI_EXCEPTION("Main storage is empty. Plz wait for merging.\n");
        return;
    }

    // 1. load vector properties    
    if(num_v != oldv2newv_->size()) num_v = loadVertexContent(main, fids); 
    // 2. build CSR
    uint32_t psbl_dlist_sz = graph_->getNumEdges() + num_v;
    if(D_.numel() != psbl_dlist_sz) {
        IntVec srcs, src_os, dests;
        srcs.reserve(num_v+1);
        srcs.push_back(0);
        dests.reserve(psbl_dlist_sz); // all dest edges
        FloatVec out_degree; // out degree matrix D
        out_degree.reserve(psbl_dlist_sz);
        if(edge_ops) src_os.reserve(psbl_dlist_sz);
        GraphMain::iterator itr = main.begin();
        for(; itr; itr.nextList(false)) {
            int64_t src_id = itr.getSrc();
            if(!graph_->validVertexId(src_id) || (*oldv2newv_).count(src_id) == 0) continue;
            
            int32_t src_idx = (*oldv2newv_)[src_id];
            LongVec dest = itr.getDests();
            if(!dest.empty()) {
                int32_t dsize = dest.size();
                if(importance_spl > 0 && dsize > importance_spl) {
                    dsize = importance_spl;
                    random_unique(dest.begin(), dest.end(), importance_spl);
                }
                if(edge_ops) src_os.push_back(src_idx);
                std::set<int32_t> check_dest;
                check_dest.insert(src_idx);
                for(int32_t i = 0; i < dsize; i++) {
                    int64_t d = dest[i];
                    if((*oldv2newv_).count(d) == 0) continue;
                    int32_t d_idx = (*oldv2newv_)[d];
                    if(check_dest.count(d_idx) == 0) {
                        check_dest.insert(d_idx);
                        if(edge_ops) src_os.push_back(src_idx);
                    }
                }
                dsize = check_dest.size();
                float dsize_sqrt = 1/sqrt(dsize);
                for(int32_t i = 0; i < dsize; i++) out_degree.push_back(dsize_sqrt);
                std::copy(check_dest.begin(), check_dest.end(), std::back_inserter(dests));
            } else {
                dests.push_back(src_idx);
                out_degree.push_back(1.0);
                if(edge_ops) src_os.push_back(src_idx);
            }
            srcs.push_back(dests.size());
        }
        dests.shrink_to_fit();
        out_degree.shrink_to_fit();
        srcs.shrink_to_fit();

        initializeGraphIndices(srcs, dests, out_degree, src_os, edge_ops);
    }
}

void TrainingDataManager::generateCSRMulti(IntVec& fids, bool incoming, int importance_spl, bool edge_ops) {
    WriteLock csr_lock(mx_csr_);
    uint32_t num_v = graph_->getNumVertex(); 
    if(num_v == 0) {
        RTI_EXCEPTION("Graph is empty.\n");
        return;
    }
    int64_t last_merge_timestamp = graph_->getLastMergeTimestamp();
    if(last_update_timestamp_ > 0 && last_update_timestamp_ == last_merge_timestamp) return;
    last_update_timestamp_ = last_merge_timestamp;
    GraphMain& main = incoming ? graph_->main_in_ : graph_->main_;
    
    int64_t max = main.getMax(0);
    if(max < 0){
        RTI_EXCEPTION("Main storage is empty. Plz wait for merging.\n");
        return;
    }

    // 1. load vector properties    
    if(num_v != oldv2newv_->size()) num_v = loadVertexContent(main, fids);

    // 2. build CSR

    uint32_t psbl_dlist_sz = graph_->getNumEdges() + num_v;

    if(D_.numel() != psbl_dlist_sz){
        std::vector<GraphMain::iterator*> itrs = main.beginMulti(main.numPartition());
        std::vector<EdgeCollect> workers;
        uint32_t rank = 0;
        for(auto itr : itrs){
            workers.emplace_back(graph_, itr, oldv2newv_, edge_ops, importance_spl, num_v / itrs.size(), rank++);
        }
        for(auto& w : workers) w.start();
        for(auto& w : workers) w.join();
        FloatVec& out_degree = workers[0].out_degree; // out degree matrix D
        IntVec& srcs = workers[0].srcs;
        IntVec& src_os = workers[0].src_os;
        IntVec& dests = workers[0].dests;
        
        srcs.reserve(num_v+1);
        
        dests.reserve(psbl_dlist_sz); 
        out_degree.reserve(psbl_dlist_sz);

        for(size_t i = 1; i < workers.size(); i++) {                     
            IntVec& s = workers[i].srcs;
            torch::Tensor sp = torch::from_blob(s.data(), {(int) s.size()}, torch::kInt32);
            sp += srcs.back();
            std::copy(s.begin(), s.end(), std::back_inserter(srcs));
            
            IntVec& d = workers[i].dests;
            std::copy(d.begin(), d.end(), std::back_inserter(dests));
            
            if(edge_ops){
                IntVec& sos = workers[i].src_os;
                std::copy(sos.begin(), sos.end(), std::back_inserter(src_os));
            }
            FloatVec& od = workers[i].out_degree; // out degree matrix D
            std::copy(od.begin(), od.end(), std::back_inserter(out_degree));
        }

        srcs.shrink_to_fit();
        dests.shrink_to_fit();
        out_degree.shrink_to_fit();

        initializeGraphIndices(srcs, dests, out_degree, src_os, edge_ops);
    }
}

torch::Tensor TrainingDataManager::getCSR() {
    uint32_t num_v = S_.size(0) - 1;
    torch::Tensor a = make_sparse_csr(S_, D_, V_, num_v, false);
    return a;
}

torch::Tensor TrainingDataManager::getCSR(torch::Tensor& v) {
    uint32_t num_v = S_.size(0) - 1;
    torch::Tensor a = make_sparse_csr(S_, D_, v, num_v, false);
    return a;
}

void TrainingDataManager::getTransposedCSR(torch::Tensor&s, torch::Tensor& d, torch::Tensor& v) {
    s = St_;
    d = Dt_;
    v = Vt_;
}

};