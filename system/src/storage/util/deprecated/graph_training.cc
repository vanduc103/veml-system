#include "storage/graph/graph_training.h"
#include "util/timer.h"

#include "random"
#include "math.h"
#include "sys/types.h"
#include "sys/sysinfo.h"

#include <torch/torch.h>

namespace storage{

// void GNNBase::getWeights(){
//     // https://discuss.pytorch.org/t/how-to-copy-network-parameters-in-libtorch-c-api/32221
//     // https://pytorch.org/tutorials/advanced/cpp_frontend.html
//     auto params = named_parameters();
//     for(auto& param : params){
//         // param.key();
//         // param.value();
//     }  
// };
GCNLayer::GCNLayer(uint32_t input_size, uint32_t output_size, bool incoming) : incoming_(incoming) {
    // fc_ = register_module("fc", torch::nn::Linear(input_size, output_size));
    fc_ = std::make_shared<Linear>(input_size, output_size);
}

GCNLayer::Linear::Linear(uint32_t input_size, uint32_t output_size){
    torch::Tensor w = torch::empty({output_size, input_size});
    w.set_requires_grad(true);
    torch::Tensor b = torch::empty({output_size});
    b.set_requires_grad(true);
    torch::nn::init::kaiming_uniform_(w, sqrt(5));
    float bound = 1 / sqrt(input_size);
    torch::nn::init::uniform_(b, -bound, bound);
    w_ = std::make_shared<torch::Tensor>(std::move(w));
    b_ = std::make_shared<torch::Tensor>(std::move(b));
}

torch::Tensor GCNLayer::Linear::forward(torch::Tensor& x){
    return torch::nn::functional::linear(x, *w_, *b_);
}

std::shared_ptr<TensorMap> GCNLayer::forward(Graph* graph, std::shared_ptr<TensorMap>& x, uint32_t k){
    Graph::iterator itr = graph->begin(incoming_);
    std::shared_ptr<TensorMap> outputs = std::make_shared<TensorMap>();
    for ( ; itr; itr.nextList()) {
        uint64_t vprop_vid = itr.getSrcIdx() & 0xFFFFFFFF;
        LongVec dest = itr.getDestList();
        uint32_t d_size = dest.size();
        if(d_size != 0){
            if(d_size > k){
                d_size = k;
                random_unique(dest.begin(), dest.end(), k);
            }
            
            std::vector<torch::Tensor> tdests(d_size);
            for(uint32_t i = 0; i < d_size; ++i){
                uint64_t vdid = (uint64_t) (dest[i] & 0xFFFFFFFF);
                tdests[i] = (*x)[vdid];
            }
            torch::Tensor tdest_features = at::vstack(tdests);
            tdest_features = at::mean(tdest_features, {0});
            tdest_features = torch::relu(fc_->forward(tdest_features));
            (*outputs)[vprop_vid] = tdest_features.unsqueeze(0);
        }
    }
    return outputs;
}

std::shared_ptr<TensorMap> GCNLayer::forward(Graph* graph, std::shared_ptr<TensorMap>& x, uint32_t dop, uint32_t k){
    if(dop == 1)
        return forward(graph, x);
    class GCNWorker : public RTIThread{
        public:
            GCNWorker(Graph::iterator& itr, std::shared_ptr<TensorMap>& x, std::shared_ptr<GCNLayer::Linear>& fc, uint32_t k)
                    : itr_(itr), x_(x), fc_(fc), k_(k){}
            void* run(){
                for ( ; itr_; itr_.nextList()) {
                    uint64_t vprop_vid = itr_.getSrcIdx() & 0xFFFFFFFF;
                    LongVec dest = itr_.getDestList();
                    uint32_t d_size = dest.size();
                    if(d_size != 0){
                        if(d_size > k_){
                            d_size = k_;
                            random_unique(dest.begin(), dest.end(), k_);
                        }
                        std::vector<torch::Tensor> tdests(d_size);
                        for(uint32_t i = 0; i < d_size; ++i){
                            uint64_t vdid = (uint64_t) (dest[i] & 0xFFFFFFFF);
                            tdests[i] = (*x_)[vdid];
                        }
                        // float deg = (float) d_size;
                        torch::Tensor tdest_features = at::vstack(tdests);
                        tdest_features = at::mean(tdest_features, {0});
                        tdest_features = torch::relu(fc_->forward(tdest_features));
                        // have unsqueeze run faster
                        outputs_[vprop_vid] = tdest_features.unsqueeze(0); 
                    }
                }
                return nullptr;
            }
            
            TensorMap outputs_;
        protected:
            Graph::iterator& itr_;
            std::shared_ptr<TensorMap>& x_;
            std::shared_ptr<GCNLayer::Linear>& fc_;
            uint32_t k_;
    };
    std::vector<Graph::iterator> itrs = graph->beginMulti(dop, incoming_);
    std::vector<GCNWorker> workers;
    for(uint32_t i = 0; i < dop; i++){
        workers.emplace_back(itrs[i], x, fc_, k);
    }
    for(uint32_t i = 0; i < dop; i++) workers[i].start();
    for(uint32_t i = 0; i < dop; i++) workers[i].join();
    TensorMap& outputs = workers[0].outputs_;
    for(uint32_t i = 1; i < dop; i++){
        outputs.merge(workers[i].outputs_);
    }
    std::shared_ptr<TensorMap> outs = std::make_shared<TensorMap>(std::move(outputs));
    return outs;
}

GCNNodeClassifier::GCNNodeClassifier(Graph* graph, int32_t feature_fid, int32_t label_fid, 
                uint32_t input_size, uint32_t hidden_size, uint32_t num_class, bool incoming, double lr) 
    : graph_(graph), feature_fid_(feature_fid), label_fid_(label_fid){
    input_layer_ = new GCNLayer(input_size, hidden_size, incoming);
    hidden_layer_ = new GCNLayer(hidden_size, hidden_size, incoming);
    pred_layer_ = new GCNLayer(hidden_size, num_class, incoming);

    torch::optim::AdamOptions adam_options(lr);
    optimizer_ = std::make_shared<torch::optim::Adam>(parameters(), adam_options);
}

GCNNodeClassifier::~GCNNodeClassifier(){
    delete input_layer_;
    delete hidden_layer_;
    delete pred_layer_;
}

/**
 * Get labels and features of nodes from graphs. Try on delta first
*/
void GCNNodeClassifier::getTrainingData(LongVec& labels, std::shared_ptr<TensorMap>& features){
    RowTable* vprop_data = reinterpret_cast<RowTable*>(graph_->vertex_prop_[0]);
    IntVec fids{label_fid_, feature_fid_};
    RowTable::iterator itr = vprop_data->begin();
    uint64_t vsize = vprop_data->getNumRecords();
    labels.reserve(vsize);
    vertex_idx_.reserve(vsize);
    uint64_t v = 0;
    while(itr){
        EvalVec vertex = itr.getRecord(fids);
        labels.push_back(vertex[0].getBigInt().get());
        DoubleList& dbl = vertex[1].getDoubleList().get();
        uint64_t vprop_vid = itr.getLptr();
        torch::Tensor t = dbl.toTensor();
        (*features)[vprop_vid] = t.to(torch::kFloat32);
        vertex_idx_.push_back(vprop_vid);
        vertex_ids_[vprop_vid] = v++;
        ++itr;
    }
}

std::shared_ptr<TensorMap> GCNNodeClassifier::forward(std::shared_ptr<TensorMap>& features, uint32_t dop, uint32_t k){
    std::shared_ptr<TensorMap> x = input_layer_->forward(graph_, features, dop, k);
    x = hidden_layer_->forward(graph_, x, dop, k);
    x = pred_layer_->forward(graph_, x, dop, k);
    return x;
}

void GCNNodeClassifier::backward(std::shared_ptr<TensorMap>& prediction, LongVec& labels, uint32_t dop){
    class BackwardWorker : public RTIThread{
        public:
            BackwardWorker(std::shared_ptr<TensorMap> prediction, LongVec& labels, std::vector<uint64_t>& vertex_idx, uint32_t start, uint32_t end)
                : prediction_(prediction), labels_(labels), vertex_idx_(vertex_idx), start_(start), end_(end){};
            void* run(){
                std::default_random_engine gen;
                std::bernoulli_distribution bdist(0.8);
                torch::nn::CrossEntropyLoss criterion;
                uint32_t size = end_ - start_;
                size_t rsize = 0;
                // auto end = size == labels_.size() ? labels_.end() : (labels_.begin()+end_);
                // IntVec lbi(labels_.begin()+start_, end);
                std::vector<torch::Tensor> outputs;
                outputs.reserve(size);
                LongVec lbi;
                lbi.reserve(size);
                for(size_t j = start_; j < end_; j++){
                    if(bdist(gen)){
                        outputs.push_back((*prediction_)[vertex_idx_[j]]);
                        lbi.push_back(labels_[j]);
                        rsize++;
                    }
                }
                outputs.resize(rsize);
                lbi.resize(rsize);
                torch::Tensor pred = at::vstack(outputs);
                torch::Tensor labels = torch::tensor(lbi, torch::kInt64);
                torch::Tensor loss = criterion(pred, labels);
                loss.backward({},c10::optional<bool>(true));
                return nullptr;
            }
        
        protected:
            std::shared_ptr<TensorMap> prediction_;
            LongVec& labels_;
            std::vector<uint64_t>& vertex_idx_;
            uint32_t start_;
            uint32_t end_;
    };
    uint32_t size = vertex_idx_.size();
    uint32_t div = size / dop;
    uint32_t mod = size % dop;
    std::vector<BackwardWorker> bwkrs;
    uint32_t start, end = 0;
    for(size_t t = 0; t < dop; t++){
        start = end;
        end = start + div;
        if(t < mod) ++end;
        bwkrs.emplace_back(prediction, labels, vertex_idx_, start, end);
    }
    for(auto& w : bwkrs) w.start();
    for(auto& w : bwkrs) w.join();
}

void GCNNodeClassifier::train(uint32_t num_epochs, uint32_t dop, uint32_t k){
    LongVec labels;
    std::shared_ptr<TensorMap> features = std::make_shared<TensorMap>();
    getTrainingData(labels, features);
    for(uint32_t i = 0; i < num_epochs; i++){
        optimizer_->zero_grad();
        std::shared_ptr<TensorMap> prediction = forward(features, dop);
        backward(prediction, labels, dop);
        optimizer_->step();
    }
}

};
