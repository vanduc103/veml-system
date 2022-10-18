#ifndef STORAGE_GRAPH_GRAPH_SPARSE_TRAINING_H_
#define STORAGE_GRAPH_GRAPH_SPARSE_TRAINING_H_

#include "storage/graph/graph.h"
#include "storage/graph/graph_delta.h"
#include "storage/graph/graph_main.h"
#include "storage/graph/graph_conv_layer.h"
#include "concurrency/rti_thread.h"
#include "storage/util/neural_ops.h"


#include <unistd.h>

#include <torch/torch.h>
#include <tensorboard_logger.h>

#include <boost/filesystem.hpp>

namespace storage {

	class GNNNodeClassifier : public torch::nn::Module {
		public:
			GNNNodeClassifier(Graph* graph, IntVec& fids, bool incoming, float lr, float dropout,
							std::string& log_file, int data_update_interval = -1, bool save_training_log = true)
				: graph_(graph), fids_(fids), incoming_(incoming), lr_(lr), data_update_interval_(data_update_interval), save_training_log_(save_training_log){
				boost::filesystem::create_directories(log_file);
				log_file += "/tfevents.pb";
				logger_ = std::make_shared<TensorBoardLogger>(log_file.c_str());
			};
			virtual ~GNNNodeClassifier() = default;

			void init(int importance_spl = -1, bool multi = false);		
			virtual void initOptimizer() = 0;
			void emptyGrad() { optimizer_->zero_grad(); }
			void stepOptimizer() { optimizer_->step(); }

			float step();
			void train(uint32_t num_epochs = 1);
			virtual torch::Tensor forward() = 0;
			float backward(torch::Tensor &preds);
			void eval();

			virtual void getWeights(TensorVec &weights) = 0;
			virtual void setWeights(TensorVec &weights) = 0;
			
			virtual void getGradients(TensorVec &grads) = 0;
			virtual void setGradients(TensorVec &grads) = 0;

			void add_scalar(std::string tag, uint32_t idx, float loss) { logger_->add_scalar(tag, idx, loss); }
		
		protected:
			Graph* graph_;
			IntVec fids_;
			bool incoming_;
			float lr_;
            float dropout_;
			int data_update_interval_;
			bool save_training_log_;
			std::shared_ptr<TrainingDataManager> data_manager_;
			std::shared_ptr<torch::optim::Adam> optimizer_;
			std::shared_ptr<TensorBoardLogger> logger_;
	};

	class GCNSparseNodeClassifier : public GNNNodeClassifier {
	
		public:		
			GCNSparseNodeClassifier(Graph *graph, IntVec &fids,
									uint32_t input_size, uint32_t hidden_size, uint32_t num_class,
									bool incoming, std::string& log_file, float lr = 1e-3, float dropout = 0.5,
									int data_update_interval = -1, bool save_training_log = true);
			~GCNSparseNodeClassifier(){};

			void initOptimizer();
			void getWeights(TensorVec &weights);
			void setWeights(TensorVec &weights);
			
			void getGradients(TensorVec &grads);
			void setGradients(TensorVec &grads);

			torch::Tensor forward();

			float backward(torch::Tensor &preds);

		protected:
			std::shared_ptr<GCNSparseLayer> input_layer_;
			std::shared_ptr<GCNSparseLayer> hidden_layer_;
			std::shared_ptr<GCNSparseLayer> pred_layer_;
			
	};

	/**
	 * APPNP node classifier
	*/
	class APPNPNodeClassifier : public GNNNodeClassifier {
		public:
			APPNPNodeClassifier(Graph *graph, IntVec &fids, uint32_t input_size,
								uint32_t hidden_size, uint32_t num_class, uint16_t k,
								float alpha, std::string& log_file, float dropout = 0.5, 
								bool incoming = false, float lr = 1e-3, int data_update_interval = -1,
								bool save_training_log = true);
			~APPNPNodeClassifier(){};
			void initOptimizer();
			torch::Tensor forward();
			void getWeights(TensorVec &weights);
			void setWeights(TensorVec &weights);
			
			void getGradients(TensorVec &grads);
			void setGradients(TensorVec &grads);

		protected:
			float alpha_;
			std::shared_ptr<Linear> fc_;
			std::shared_ptr<Linear> pred_;
			std::shared_ptr<APPNPLayer> appnp_;
	};

	
	/**
	 * GAT node classifier
	*/
	class GATNodeClassifier : public GNNNodeClassifier {
		public:		
			GATNodeClassifier(Graph *graph, IntVec &fids,
							uint32_t input_size, uint32_t hidden_size, uint32_t num_class,
							uint16_t num_head, bool incoming, std::string& log_file, 
							float lr = 1e-3, float dropout = 0.5, bool agg = false, int data_update_interval = -1,
							bool save_training_log = true);
			~GATNodeClassifier(){};
			
			void initOptimizer();
			void init(int importance_spl, bool multi = false);

			void compressEdges(torch::Tensor s, torch::Tensor d);
			torch::Tensor forward();
			void getWeights(TensorVec &weights);
			void setWeights(TensorVec &weights);
			
			void getGradients(TensorVec &grads);
			void setGradients(TensorVec &grads);

		protected:
			std::shared_ptr<GATLayer> input_layer_;
			std::shared_ptr<GATLayer> hidden_layer_;
			std::shared_ptr<GATLayer> pred_layer_;
			torch::Tensor ss_, ds_, e_;
	};

};

#endif // STORAGE_GRAPH_GRAPH_SPARSE_TRAINING_H_

