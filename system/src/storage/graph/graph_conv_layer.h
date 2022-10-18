#ifndef STORAGE_GRAPH_GRAPH_CONV_LAYER_H_
#define STORAGE_GRAPH_GRAPH_CONV_LAYER_H_

#include "storage/graph/graph.h"
#include "storage/util/neural_ops.h"

#include <torch/torch.h>

namespace storage 
{
    typedef std::vector<torch::Tensor> TensorVec;

    class Linear : public torch::nn::Module
    {
    public:
        Linear(uint32_t input_size, uint32_t output_size, bool bias = true);
        ~Linear() = default;
        torch::Tensor forward(const torch::Tensor &y);
        void setWeights(torch::Tensor w, torch::Tensor b);
        void setWeights(torch::Tensor w);
        void getWeights(TensorVec &weights);
        void getGradients(TensorVec &grads);
        void setGradients(torch::Tensor w, torch::Tensor b);
        void setGradients(torch::Tensor w);
        uint32_t getInputSize() { return input_size_; }
        uint32_t getOutputSize() { return output_size_; }

    protected:
        uint32_t input_size_, output_size_;
        torch::Tensor w_;
        torch::Tensor b_;
        bool bias_;
    };

    class GCNSparseLayer : public torch::nn::Module
    {
    public:
        GCNSparseLayer();
        GCNSparseLayer(uint32_t input_size, uint32_t output_size,
                        float dropout = 0.5, bool activation = true);
        ~GCNSparseLayer(){};
        torch::Tensor forward(const torch::Tensor &A, const torch::Tensor &X,
                                const torch::Tensor &st, const torch::Tensor &dt, const torch::Tensor &vt);
        void setWeights(torch::Tensor w, torch::Tensor b);
        void getWeights(TensorVec &weights);
        void getGradients(TensorVec &grads);
        void setGradients(torch::Tensor w, torch::Tensor b);

    protected:
        std::shared_ptr<Linear> fc_;
        torch::Tensor bias_;
        float dropout_;
        bool activation_;
    };

    class APPNPLayer : public torch::nn::Module
	{
	public:
		APPNPLayer();
		APPNPLayer(uint16_t k, float alpha) : k_(k), alpha_(alpha){};
		~APPNPLayer(){};
		torch::Tensor forward(const torch::Tensor &a, const torch::Tensor &x,
							  const torch::Tensor &st, const torch::Tensor &dt,
							  const torch::Tensor &vt);

	protected:
		uint16_t k_;
		float alpha_;
	};

    /**
     * GAT Implementation
    */

	class GATLayer : public torch::nn::Module
	{
	public:
		GATLayer();
		GATLayer(uint32_t input_size, uint32_t output_size, uint16_t num_head = 1,
				 float dropout = 0.5, bool agg = false, bool activation = true);
		~GATLayer(){};
		torch::Tensor forward(const torch::Tensor &s, const torch::Tensor &so, const torch::Tensor &d, const torch::Tensor &X,
                            const torch::Tensor &st, const torch::Tensor &dt, const torch::Tensor &vt,
                            const torch::Tensor &ss, const torch::Tensor &ds, const torch::Tensor &full_e);

	protected:
		std::shared_ptr<Linear> fc_;
		torch::Tensor sf_node_;
		torch::Tensor nb_node_;
        torch::Tensor bias_;
		uint16_t num_head_;
		bool agg_;
		float dropout_;
		bool activation_;
	};

};

#endif