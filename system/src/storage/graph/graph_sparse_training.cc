#include "storage/graph/graph_training_data.h"
#include "storage/graph/graph_sparse_training.h"
#include "util/timer.h"

#include <torch/torch.h>

using namespace torch::indexing;

namespace storage {

/**
 * Define base class GNNNodeClassifier
*/
void GNNNodeClassifier::init(int importance_spl, bool multi) { 
    initOptimizer();
    data_manager_ = std::make_shared<TrainingDataManager>(graph_);
    if(multi)
        data_manager_->generateCSRMulti(fids_, graph_->useIncoming(), importance_spl, false);
    else
        data_manager_->generateCSR(fids_, graph_->useIncoming(), importance_spl, false);
}

float GNNNodeClassifier::backward(torch::Tensor& preds) {
    torch::nn::CrossEntropyLoss criterion(torch::nn::CrossEntropyLossOptions().reduction(torch::kMean));
    auto labels = data_manager_->getLabels();
    torch::Tensor loss = criterion(preds, labels);
    float loss_ = loss.item().toFloat();
    loss.backward();
    return loss_;
}

float GNNNodeClassifier::step() {
    float loss = 0.0;
    torch::Tensor prediction = forward();
    loss = backward(prediction);
    return loss;
}

void GNNNodeClassifier::train(uint32_t num_epochs) {
    float loss = 0.0;
    util::Timer timer;
    float t1 = 0.0, t2 = 0.0;
    for(uint32_t i = 0; i < num_epochs; i++) {
        timer.start();
        if(i > 0 && data_update_interval_ > 0 && i % data_update_interval_ == 0) {
            std::cout << "Check CSR at " << i << std::endl;
            graph_->mergeManually(1);
            data_manager_->generateCSRMulti(fids_, graph_->useIncoming(), -1, false);
        }
        timer.stop();
        t1 += timer.getElapsedMilli();
        timer.start();
        emptyGrad();
        loss = step();
        if(save_training_log_)
            add_scalar("Training loss", i, loss);
        stepOptimizer();
        timer.stop();
        t2 += timer.getElapsedMilli();
    }
    std::cout << "Data reload time: " << t1 << " ms" << std::endl;
    std::cout << "Training time: " << t2 << " ms" << std::endl;
}

void GNNNodeClassifier::eval() {}

GCNSparseNodeClassifier::GCNSparseNodeClassifier(Graph* graph, IntVec& fids, 
                uint32_t input_size, uint32_t hidden_size, uint32_t num_class, bool incoming, 
                std::string& log_file, float lr, float dropout, int data_update_interval, bool save_training_log) 
    : GNNNodeClassifier(graph, fids, incoming, lr, dropout, log_file, data_update_interval, save_training_log) {
    input_layer_ = register_module("input_layer", std::make_shared<GCNSparseLayer>(input_size, hidden_size, 0.0, true));
    hidden_layer_ = register_module("hidden_layer", std::make_shared<GCNSparseLayer>(hidden_size, hidden_size, 0.5, true));
    pred_layer_ = register_module("pred_layer", std::make_shared<GCNSparseLayer>(hidden_size, num_class, 0.5, false));
}

void GCNSparseNodeClassifier::initOptimizer() {
    torch::optim::AdamOptions adam_options(lr_);
    adam_options.weight_decay(5e-4);
    optimizer_ = std::make_shared<torch::optim::Adam>(parameters(), adam_options);
}

void GCNSparseNodeClassifier::getWeights(TensorVec& weights) {
    input_layer_->getWeights(weights);
    hidden_layer_->getWeights(weights);
    pred_layer_->getWeights(weights);
}

void GCNSparseNodeClassifier::setWeights(TensorVec& weights) {
    input_layer_->setWeights(weights[0], weights[1]); 
    hidden_layer_->setWeights(weights[2], weights[3]); 
    pred_layer_->setWeights(weights[4], weights[5]); 
}

void GCNSparseNodeClassifier::getGradients(TensorVec& grads) {
    input_layer_->getGradients(grads); 
    hidden_layer_->getGradients(grads); 
    pred_layer_->getGradients(grads); 
}

void GCNSparseNodeClassifier::setGradients(TensorVec& grads) {
    input_layer_->setGradients(grads[0], grads[1]); 
    hidden_layer_->setGradients(grads[2], grads[3]); 
    pred_layer_->setGradients(grads[4], grads[5]);
}

torch::Tensor GCNSparseNodeClassifier::forward() {
    torch::Tensor a = data_manager_->getCSR();
    torch::Tensor st, dt, vt;
    data_manager_->getTransposedCSR(st, dt, vt);
    torch::Tensor features = data_manager_->getFeatures();
    torch::Tensor x = input_layer_->forward(a, features, st, dt, vt);
    x = hidden_layer_->forward(a, x, st, dt, vt);
    x = pred_layer_->forward(a, x, st, dt, vt);
    return x;
}

APPNPNodeClassifier::APPNPNodeClassifier(Graph* graph, IntVec& fids, 
                                        uint32_t input_size, uint32_t hidden_size, uint32_t num_class, 
                                        uint16_t k, float alpha, std::string& log_file, float dropout, 
                                        bool incoming, float lr, int data_update_interval, bool save_training_log) 
    : GNNNodeClassifier(graph, fids, incoming, lr, dropout, log_file, data_update_interval, save_training_log), alpha_(alpha) {
    fc_ = register_module("fc", std::make_shared<Linear>(input_size, hidden_size));
    pred_ = register_module("pred", std::make_shared<Linear>(hidden_size, num_class));
    appnp_ = register_module("appnp_layer", std::make_shared<APPNPLayer>(k, 0.5));
}

void APPNPNodeClassifier::initOptimizer() {
    torch::optim::AdamOptions adam_options(lr_);
    adam_options.weight_decay(5e-4);
    optimizer_ = std::make_shared<torch::optim::Adam>(parameters(), adam_options);
}

torch::Tensor APPNPNodeClassifier::forward() {
    torch::Tensor y = torch::relu(fc_->forward(data_manager_->getFeatures()));
    if(dropout_ > 0.0) y = torch::dropout(y, dropout_, true);
    y = torch::relu(pred_->forward(y));
    if(dropout_ > 0.0) y = torch::dropout(y, dropout_, true);
    torch::Tensor v = data_manager_->getValues() * (1 - alpha_);
    torch::Tensor a = data_manager_->getCSR(v);
    torch::Tensor st, dt, vt;
    data_manager_->getTransposedCSR(st, dt, vt);
    y = appnp_->forward(a, y, st, dt, vt);
    return y;
}

void APPNPNodeClassifier::getWeights(TensorVec &weights) {}
void APPNPNodeClassifier::setWeights(TensorVec &weights) {}

void APPNPNodeClassifier::getGradients(TensorVec &grads) {}
void APPNPNodeClassifier::setGradients(TensorVec &grads) {}

/**
 * GAT implementation
*/

GATNodeClassifier::GATNodeClassifier(Graph* graph, IntVec& fids, 
                uint32_t input_size, uint32_t hidden_size, uint32_t num_class, 
                uint16_t num_head, bool incoming, std::string& log_file, float lr, 
                float dropout, bool agg, int data_update_interval, bool save_training_log) 
    : GNNNodeClassifier(graph, fids, incoming, lr, dropout, log_file, data_update_interval, save_training_log) {
    input_layer_ = register_module("input_layer", std::make_shared<GATLayer>(input_size, hidden_size, num_head, 0.5, false, true));
    uint32_t i_hidden_size = hidden_size;
    if(!agg && num_head > 1) i_hidden_size *= num_head;
    hidden_layer_ = register_module("hidden_layer", std::make_shared<GATLayer>(i_hidden_size, hidden_size, num_head, 0.5, false, true));
    pred_layer_ = register_module("pred_layer", std::make_shared<GATLayer>(i_hidden_size, num_class, 1, 0.0, false, false));
}

void GATNodeClassifier::initOptimizer() {
    torch::optim::AdamOptions adam_options(lr_);
    adam_options.weight_decay(5e-4);
    optimizer_ = std::make_shared<torch::optim::Adam>(parameters(), adam_options);
}

void GATNodeClassifier::init(int importance_spl, bool multi) { 
    initOptimizer();
    data_manager_ = std::make_shared<TrainingDataManager>(graph_);
    if(multi)
        data_manager_->generateCSRMulti(fids_, graph_->useIncoming(), importance_spl, true);
    else
        data_manager_->generateCSR(fids_, graph_->useIncoming(), importance_spl, true);
    compressEdges(data_manager_->getSourceIndex(), data_manager_->getDestIndex());
}

void GATNodeClassifier::compressEdges(torch::Tensor s, torch::Tensor d) {
    IntVec short_src_idx, short_dst_idx, full_idx;
    int edge_size = d.size(0);
    full_idx.reserve(edge_size);
    int32_t *s_ptr = (int32_t *) s.data_ptr();
    int32_t *d_ptr = (int32_t*) d.data_ptr();
    std::unordered_map<int64_t, int32_t> edge_map;
    for(int32_t i = 0; i < (s.size(0) - 1); i++) {
        int32_t st = *s_ptr++; // dest start offset
        int32_t ed = *s_ptr; // dest end ofset
        for(int j = st; j < ed; j++) {
            int32_t dest_id = *(d_ptr + j);
            if(dest_id < i) {
                // this edge has already computed
                int64_t edge_id = ((int64_t) dest_id << 32) + i;
                if(edge_map.count(edge_id) == 0) {
                    short_src_idx.push_back(dest_id);
                    short_dst_idx.push_back(i);
                    int id = edge_map.size();
                    edge_map[edge_id] = id;
                }
                full_idx.push_back(edge_map[edge_id]);
            } else {
                int64_t edge_id = ((int64_t) i << 32) + dest_id;
                short_src_idx.push_back(i);
                short_dst_idx.push_back(dest_id);
                int id = edge_map.size();
                edge_map[edge_id] = id;
                full_idx.push_back(id);
            }
        }
    }
    short_src_idx.shrink_to_fit();
    short_dst_idx.shrink_to_fit();

    torch::TensorOptions options = torch::TensorOptions().dtype(torch::kInt32);
    ss_ = torch::tensor(short_src_idx, options);
    ds_ = torch::tensor(short_dst_idx, options);
    e_ = torch::tensor(full_idx, options);
}

torch::Tensor GATNodeClassifier::forward() {
    torch::Tensor s = data_manager_->getSourceIndex();
    torch::Tensor so = data_manager_->getFullSourceIndex();
    torch::Tensor d = data_manager_->getDestIndex();
    torch::Tensor f = data_manager_->getFeatures();
    torch::Tensor st, dt, vt;
    data_manager_->getTransposedCSR(st, dt, vt);
    torch::Tensor x = input_layer_->forward(s, so, d, f, st, dt, vt, ss_, ds_, e_);
    x = hidden_layer_->forward(s, so, d, x, st, dt, vt, ss_, ds_, e_);
    x = pred_layer_->forward(s, so, d, x, st, dt, vt, ss_, ds_, e_);
    return x;
}

void GATNodeClassifier::getWeights(TensorVec &weights) {}
void GATNodeClassifier::setWeights(TensorVec &weights) {}

void GATNodeClassifier::getGradients(TensorVec &grads) {}
void GATNodeClassifier::setGradients(TensorVec &grads) {}

};
