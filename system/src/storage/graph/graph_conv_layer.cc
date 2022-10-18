#include "storage/graph/graph_conv_layer.h"
#include "util/timer.h"

#include <torch/torch.h>

namespace storage{

Linear::Linear(uint32_t input_size, uint32_t output_size, bool bias) 
    : input_size_(input_size), output_size_(output_size), bias_(bias){
    torch::Tensor w = torch::empty({input_size, output_size});
    torch::nn::init::kaiming_uniform_(w, sqrt(5));
    float bound = 1 / sqrt(input_size);
    w_ = register_parameter("w", w);
    torch::Tensor b = torch::empty({output_size});
    if(bias){
        torch::nn::init::uniform_(b, -bound, bound);
        b_ = register_parameter("b", b);
    }
}

torch::Tensor Linear::forward(const torch::Tensor& y){
    if(bias_)
        return torch::addmm(b_, y, w_);
    return torch::mm(y, w_);
}

void Linear::getWeights(TensorVec& weights){
    weights.push_back(w_);
    if(bias_)
        weights.push_back(b_);
}

void Linear::setWeights(torch::Tensor w){
    torch::autograd::GradMode::set_enabled(false);
    w_.copy_(w);
    torch::autograd::GradMode::set_enabled(true);
}

void Linear::setWeights(torch::Tensor w, torch::Tensor b){
    torch::autograd::GradMode::set_enabled(false);
    w_.copy_(w);
    if(bias_)
        b_.copy_(b);
    torch::autograd::GradMode::set_enabled(true);
}

void Linear::getGradients(TensorVec& grads){
    grads.push_back(w_.grad());
    if(bias_)
        grads.push_back(b_.grad());
}

void Linear::setGradients(torch::Tensor w, torch::Tensor b){
    w_.mutable_grad() = w;
    b_.mutable_grad() = b;
}

void Linear::setGradients(torch::Tensor w){
    w_.mutable_grad() = w;
}

GCNSparseLayer::GCNSparseLayer(uint32_t input_size, uint32_t output_size, float dropout, bool activation) 
    : dropout_(dropout), activation_(activation){
    fc_ = register_module("fc", std::make_shared<Linear>(input_size, output_size, false));
    torch::Tensor b = torch::zeros({output_size}, torch::kFloat32);
    bias_ = register_parameter("bias", b);
}

torch::Tensor GCNSparseLayer::forward(const torch::Tensor& a, const torch::Tensor& x,
                                    const torch::Tensor& st, const torch::Tensor& dt, const torch::Tensor& vt){
    uint32_t feature_size = x.size(1);
    uint32_t hidden_dim = fc_->getOutputSize();
    torch::Tensor outputs = x;
    if(dropout_ > 0.0){
        outputs = torch::dropout(outputs, dropout_, true);
    }
    if(feature_size > hidden_dim){
        outputs = fc_->forward(outputs);
        outputs = SPMM::apply(a, outputs, st, dt, vt);
    }else{
        outputs = SPMM::apply(a, x, st, dt, vt);
        outputs = fc_->forward(outputs);
    }
    outputs += bias_;
    if(activation_) outputs = torch::relu(outputs);
    return outputs;
}

void GCNSparseLayer::getWeights(TensorVec& weights){
    fc_->getWeights(weights);
    weights.push_back(bias_);
}

void GCNSparseLayer::setWeights(torch::Tensor w, torch::Tensor b){
    fc_->setWeights(w);
    torch::autograd::GradMode::set_enabled(false);
    bias_.copy_(b);
    torch::autograd::GradMode::set_enabled(true);
}

void GCNSparseLayer::getGradients(TensorVec& grads){
    fc_->getGradients(grads);
    grads.push_back(bias_.grad());
}

void GCNSparseLayer::setGradients(torch::Tensor w, torch::Tensor b){
    fc_->setGradients(w);
    bias_.mutable_grad() = b;
}

/**
 * APPNP
*/

torch::Tensor APPNPLayer::forward(const torch::Tensor& a, const torch::Tensor& x,
                                    const torch::Tensor& st, const torch::Tensor& dt,
                                    const torch::Tensor& vt){
    torch::Tensor z = x;
    torch::Tensor h = x * alpha_;
    for(int i = 0; i < k_; i++){
        z = SPMM::apply(a, z, st, dt, vt);
        z = z + h;
    }
    return z;
}

/**
 * GAT implementation
*/

GATLayer::GATLayer(uint32_t input_size, uint32_t output_size, uint16_t num_head, float dropout, bool agg, bool activation) 
    : num_head_(num_head), agg_(agg), dropout_(dropout), activation_(activation){
    fc_ = register_module("fc", std::make_shared<Linear>(input_size, output_size*num_head));
    
    torch::Tensor ws = torch::empty({1, num_head, output_size});
    torch::Tensor wn = torch::empty({1, num_head, output_size});
    torch::nn::init::kaiming_uniform_(ws, sqrt(5));
    torch::nn::init::kaiming_uniform_(wn, sqrt(5));
    sf_node_ = register_parameter("sf_node", ws);
    nb_node_ = register_parameter("nb_node", wn);
    torch::Tensor b = torch::zeros({output_size * num_head}, torch::kFloat32);
    bias_ = register_parameter("bias", b);
}

torch::Tensor GATLayer::forward(const torch::Tensor& s, const torch::Tensor& so, const torch::Tensor& d, const torch::Tensor& x,
                                const torch::Tensor& st, const torch::Tensor& dt, const torch::Tensor& vt, 
                                const torch::Tensor& ss, const torch::Tensor& ds, const torch::Tensor& full_e){
    torch::Tensor z = fc_->forward(x);
    int num_v = z.size(0);
    if(num_head_ > 1){
        z = z.view({num_v, num_head_, z.size(1)/num_head_});
    }
    // 2. both sf_x and nb_x are: |V| x H
    torch::Tensor sf_x = (sf_node_ * z).sum(2).squeeze();
    torch::Tensor nb_x = (nb_node_ * z).sum(2).squeeze();
    if(dropout_ > 0.0){
        sf_x = torch::dropout(sf_x, dropout_, true);
        nb_x = torch::dropout(nb_x, dropout_, true);
    }
     
    // 3. project sf_x and nb_x to edges |E| x num_head
    torch::Tensor src = torch::index_select(sf_x, 0, ss);
    torch::Tensor dst = torch::index_select(nb_x, 0, ds);
    torch::Tensor e = torch::leaky_relu(src + dst, 0.02);
    e = torch::index_select(e, 0, full_e);
    
    if(num_head_ > 1){
        TensorVec outs;
        TensorVec eds = e.chunk(num_head_, 1);
        TensorVec zs = z.chunk(num_head_, 1);
        for(int i = 0; i < num_head_; i++){
            torch::Tensor edge_values = EdgeSoftmax::apply(eds[i].squeeze(), so, d);
            torch::Tensor out = VMES::apply(edge_values, zs[i].squeeze(), s, d, st, dt, vt, ss, ds, full_e);
            outs.push_back(out);
        }
        z = torch::cat(outs, 1);
    } 
    else{
        torch::Tensor edge_values = EdgeSoftmax::apply(e, so, d);
        z = VME::apply(edge_values, z, s, so, d, st, dt, vt);
    }
    z += bias_;
    if(dropout_ > 0.0) z = torch::dropout(z, dropout_, true);
    if(activation_) z = torch::elu(z);
    return z;
}


};