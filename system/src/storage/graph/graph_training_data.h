#ifndef STORAGE_GRAPH_GRAPH_TRAINING_DATA_H_
#define STORAGE_GRAPH_GRAPH_TRAINING_DATA_H_

#include "storage/graph/graph.h"
#include "storage/graph/graph_main.h"
#include "concurrency/rti_thread.h"

#include "storage/util/neural_ops.h"

#include <torch/torch.h>

namespace storage {
	typedef std::unordered_map<int64_t, int32_t> IntMap;

	inline void random_unique(std::vector<int64_t>::iterator begin,
							std::vector<int64_t>::iterator end, uint32_t k) {
		size_t left = std::distance(begin, end);
		while (k--)
		{
			std::vector<int64_t>::iterator r = begin;
			std::advance(r, rand() % left);
			std::swap(*begin, *r);
			++begin;
			--left;
		}
	}

	class EdgeCollect : public RTIThread {
		public:
			EdgeCollect(Graph* graph, GraphMain::iterator* itr, std::shared_ptr<IntMap>& oldv2newv,
						bool edge_ops, int importance_spl, uint32_t num_v, uint32_t rank)
				: graph_(graph), itr_(itr), oldv2newv_(oldv2newv), edge_ops_(edge_ops),
				importance_spl_(importance_spl), num_v_(num_v), rank_(rank){};
			void* run();

			IntVec srcs, src_os, dests;
			FloatVec out_degree; 
			
		protected: 
			Graph* graph_;
			GraphMain::iterator* itr_;
			std::shared_ptr<IntMap>& oldv2newv_;
			bool edge_ops_;
			int importance_spl_;
			uint32_t num_v_;
			uint32_t rank_;

	};

	class TrainingDataManager {
		public:
			TrainingDataManager(Graph* graph) 
                : graph_(graph), last_update_timestamp_(-1), max_vid_(-1) {
				oldv2newv_ = std::make_shared<IntMap>();
			}
			TrainingDataManager(TrainingDataManager& d){
				graph_ = d.graph_;
				oldv2newv_ = d.oldv2newv_;
				S_ = d.S_;
				So_ = d.So_; 
				D_ = d.D_;
				V_ = d.V_;
				St_ = d.St_;
				Dt_ = d.Dt_;
				Vt_ = d.Vt_; 
				X_ = d.X_; 
				label_ = d.label_;
			}

			~TrainingDataManager(){};
			
			/**
			 * Return vt as index instead of float value (use for edge operator like GAT w/o normalized A )
			*/
			void buildTransposedCSR(IntVec& srcs, IntVec& dests,
                                   torch::Tensor& st, torch::Tensor& dt, torch::Tensor& vt);

			/**
			 * Use for GNN & APPNP w/ A norm by D^-1/2 (A+I) D^(-1/2)
			*/	
			void buildTransposedCSR(IntVec& srcs, IntVec& dests, FloatVec& values, 
                                    torch::Tensor& st, torch::Tensor& dt, torch::Tensor& vt);

			int32_t loadVertexContent(GraphMain& main, IntVec& fids);	
            void initializeGraphIndices(IntVec& srcs, IntVec& dests, FloatVec& out_degree, IntVec& src_os, bool edge_ops);
			void generateCSR(IntVec& fids, bool incoming, int importance_spl, bool edge_ops);
			void generateCSRMulti(IntVec& fids, bool incoming, int importance_spl, bool edge_ops);
			
			torch::Tensor getCSR();
			torch::Tensor getCSR(torch::Tensor& v);
			
			void getTransposedCSR(torch::Tensor&s, torch::Tensor& d, torch::Tensor& v);

			torch::Tensor getLabels() { return label_; }
			torch::Tensor getFeatures() { return X_; }
			torch::Tensor getSourceIndex() { return S_;	}
			torch::Tensor getFullSourceIndex() { return So_; }
			torch::Tensor getDestIndex() { return D_; }
			torch::Tensor getValues() { return V_; }
		
		protected:
			Graph* graph_;
			std::shared_ptr<IntMap> oldv2newv_;
            int64_t last_update_timestamp_;
			int64_t max_vid_; // only load v feature > max_vid
			Mutex mx_csr_;

			torch::Tensor S_;
			torch::Tensor So_; // coo index for attention map on edges
			torch::Tensor D_;
			torch::Tensor V_;
			// St_, Dt_, Vt_ use for A_.T since sparse_csr can't be stored in torch context
			torch::Tensor St_;
			torch::Tensor Dt_;
			torch::Tensor Vt_; 
			torch::Tensor X_; // input_features 
			torch::Tensor label_;
	};

}

#endif // STORAGE_GRAPH_GRAPH_TRAINING_DATA_H_