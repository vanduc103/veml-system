#ifndef STORAGE_NEURAL_OPS_H_
#define STORAGE_NEURAL_OPS_H_

#include "common/def_const.h"
#include "concurrency/rti_thread.h"
#include "util/timer.h"

#include <iostream>
#include <fstream>
#include <mutex>

#include <torch/torch.h>

namespace storage{
    typedef std::vector<torch::Tensor> TensorVec;
    
    inline void serializeTensor(
                    torch::Tensor& weight, std::string& serialized_weight) {
        std::ostringstream stream;
        torch::save(weight, stream);
        serialized_weight = stream.str();
    }

    inline torch::Tensor deserializeTensor(std::string& serialized_weight){
        std::stringstream stream;
        stream << serialized_weight;
        torch::Tensor w;
        torch::load(w, stream);
        return w;
    }

    inline void strVecToTensorVec(
                    StrVec& serialized_weights, TensorVec& weights) {
        weights.reserve(serialized_weights.size());
        for(auto& sw : serialized_weights){
            torch::Tensor w = deserializeTensor(sw);
            weights.push_back(w);
        }
    }
    

    inline torch::Tensor make_sparse_csr(const torch::Tensor& s, const torch::Tensor& d,
                                        const torch::Tensor& v, int n, bool require_grads){
        auto opts = torch::TensorOptions().dtype(torch::kFloat32)
                                          .layout(torch::kSparseCsr)
                                          .requires_grad(require_grads);
        return torch::sparse_csr_tensor(s, d, v, {n, n}, opts);
    }

    inline torch::Tensor sparse_csr(IntVec& srcs, IntVec& dests, FloatVec& values, bool require_grads){
        torch::Tensor s = torch::tensor(srcs, torch::kInt32);
        torch::Tensor d = torch::tensor(dests, torch::kInt32);
        torch::Tensor v;
        if(values.empty()){
            v = torch::ones({(int) dests.size()}, torch::kFloat32);
        }else{
            v = torch::tensor(values, torch::kFloat32);
        }
        int num_v = srcs.size()-1;
        return make_sparse_csr(s, d, v, num_v, require_grads);
    }

    /**
     * Only use for csr with transposed matrix
     */
    class SPMM : public torch::autograd::Function<SPMM>{
      public:
        static torch::Tensor forward(
                torch::autograd::AutogradContext* ctx,
                torch::Tensor a, torch::Tensor x, 
                torch::Tensor st, torch::Tensor dt, torch::Tensor vt) {
            auto output = torch::_sparse_mm(a, x);
            ctx->save_for_backward({st, dt, vt});
            return output;
        }
        static torch::autograd::tensor_list backward(
                torch::autograd::AutogradContext* ctx,
                torch::autograd::tensor_list grad_outputs) {
            // get csc
            auto saved = ctx->get_saved_variables();
            torch::Tensor& ts = saved[0];
            torch::Tensor& td = saved[1];
            torch::Tensor& tv = saved[2];

            // rebuild tranposed matrix from csc
            int n = ts.size(0) - 1;
            torch::Tensor at = make_sparse_csr(ts, td, tv, n, false);
            // 1. get output
            torch::Tensor grad_output = grad_outputs[0];
            // 2. backward process
            // 2.1 pass grad to weight (easy) grad_w = a.T * grad_out 
            torch::Tensor grad_x = torch::_sparse_mm(at, grad_output);
            // 2.2 pass grad to x_v only (difficult) grad_x = grad_out * w.T
            // torch::Tensor& x = saved[3]; torch::Tensor grad_v = grad_output * x.t();
            torch::Tensor empty = torch::Tensor();
            return {empty, grad_x, empty, empty, empty};
        }
    };

    class EdgeSoftmax : public torch::autograd::Function<EdgeSoftmax>{
      public:
        static torch::Tensor forward(
                torch::autograd::AutogradContext* ctx,
                torch::Tensor e, torch::Tensor s, torch::Tensor d) {
            uint32_t dim = s.size(0);
            torch::Tensor idx = torch::cat({s.to(torch::kInt64), d.to(torch::kInt64)}).reshape({2, dim});    
            auto opts = torch::TensorOptions().dtype(torch::kFloat32)
                                              .layout(torch::kSparse)
                                              .requires_grad(false);
            torch::Tensor a = torch::sparse_coo_tensor(idx, e, opts);
            torch::Tensor edge_values = torch::_sparse_softmax(a, 1);
            
            edge_values = edge_values._values();
            ctx->save_for_backward({edge_values});
            return edge_values;
        }
        static torch::autograd::tensor_list backward(
                torch::autograd::AutogradContext* ctx,
                torch::autograd::tensor_list grad_outputs) {
            // get csc
            auto saved = ctx->get_saved_variables();
            torch::Tensor& edge_values = saved[0];
            torch::Tensor grad_output = grad_outputs[0];
        
            torch::Tensor grad = edge_values * grad_output;

            torch::Tensor empty = torch::Tensor();
            return {grad, empty, empty};
        }
    };

    class VME : public torch::autograd::Function<VME>{
      public:
        static torch::Tensor forward(
                torch::autograd::AutogradContext* ctx,
                torch::Tensor edge_values, torch::Tensor v_values, 
                torch::Tensor s, torch::Tensor so, torch::Tensor d,
                torch::Tensor st, torch::Tensor dt, torch::Tensor vt) {
            int num_v = s.size(0) - 1;
            torch::Tensor a = make_sparse_csr(s, d, edge_values, num_v, false);
            auto output = torch::_sparse_mm(a, v_values);
            torch::Tensor edge_t = torch::index_select(edge_values, 0, vt);
            ctx->save_for_backward({st, dt, edge_t, so, d, v_values});
            return output;
        }
        static torch::autograd::tensor_list backward(
                torch::autograd::AutogradContext* ctx,
                torch::autograd::tensor_list grad_outputs) {
            // get csc
            auto saved = ctx->get_saved_variables();
            torch::Tensor& ts = saved[0];
            torch::Tensor& td = saved[1];
            torch::Tensor& tv = saved[2];
            // rebuild tranposed matrix from csc
            int n = ts.size(0) - 1;
            torch::Tensor at = make_sparse_csr(ts, td, tv, n, false);
            // 1. get output
            torch::Tensor grad_output = grad_outputs[0];
            // 2. backward process
            // 2.1 pass grad to weight (easy) grad_v = a.T * grad_out 
            torch::Tensor grad_v = torch::_sparse_mm(at, grad_output);
            // 2.2 grad_e = grad_out * x.T 
            // or simply dot product of each row grad_out 
            // w/ each each col in x.T if row_i and col_j are an edge
            // this part is the most expensive part
            torch::Tensor& s = saved[3];
            torch::Tensor& d = saved[4];
            torch::Tensor& v_values = saved[5];
            // plz notice that we use inversed graph in all cases => grad_output is for source
            torch::Tensor src = torch::index_select(grad_output, 0, s);
            torch::Tensor dst = torch::index_select(v_values, 0, d);
            torch::Tensor grad_e = src * dst;
            grad_e = grad_e.sum(1).squeeze();

            torch::Tensor empty = torch::Tensor();
            return {grad_e, grad_v, empty, empty, empty, empty, empty, empty};
        }
    };

    class VMES : public torch::autograd::Function<VMES>{
      public:
        static torch::Tensor forward(
                torch::autograd::AutogradContext* ctx,
                torch::Tensor edge_values, torch::Tensor v_values, 
                torch::Tensor s, torch::Tensor d,
                torch::Tensor st, torch::Tensor dt, torch::Tensor vt,
                torch::Tensor ss, torch::Tensor ds, torch::Tensor full_e) {
            int num_v = s.size(0) - 1;
            torch::Tensor a = make_sparse_csr(s, d, edge_values, num_v, false);
            auto output = torch::_sparse_mm(a, v_values);
            torch::Tensor edge_t = torch::index_select(edge_values, 0, vt);
            ctx->save_for_backward({st, dt, edge_t, ss, ds, full_e, v_values});
            return output;
        }
        static torch::autograd::tensor_list backward(
                torch::autograd::AutogradContext* ctx,
                torch::autograd::tensor_list grad_outputs) {
            // get csc
            auto saved = ctx->get_saved_variables();
            torch::Tensor& ts = saved[0];
            torch::Tensor& td = saved[1];
            torch::Tensor& tv = saved[2];
            // rebuild tranposed matrix from csc
            int n = ts.size(0) - 1;
            torch::Tensor at = make_sparse_csr(ts, td, tv, n, false);
            // 1. get output
            torch::Tensor grad_output = grad_outputs[0];
            // 2. backward process
            // 2.1 pass grad to weight (easy) grad_v = a.T * grad_out 
            torch::Tensor grad_v = torch::_sparse_mm(at, grad_output);
            // 2.2 grad_e = grad_out * x.T 
            // or simply dot product of each row grad_out 
            // w/ each each col in x.T if row_i and col_j are an edge
            // this part is the most expensive part
            torch::Tensor& ss = saved[3];
            torch::Tensor& ds = saved[4];
            torch::Tensor& full_e = saved[5];
            torch::Tensor& v_values = saved[6];
            // plz notice that we use inversed graph in all cases => grad_output is for source
            torch::Tensor src = torch::index_select(grad_output, 0, ss);
            torch::Tensor dst = torch::index_select(v_values, 0, ds);
            torch::Tensor grad_e = src * dst;
            grad_e = grad_e.sum(1).squeeze();
            grad_e = torch::index_select(grad_e, 0, full_e);

            torch::Tensor empty = torch::Tensor();
            return {grad_e, grad_v, empty, empty, empty, empty, empty, empty, empty, empty};
        }
    };
}

#endif
