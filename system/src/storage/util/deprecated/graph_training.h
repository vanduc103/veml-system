#ifndef STORAGE_GRAPH_GRAPH_TRAINING_H_
#define STORAGE_GRAPH_GRAPH_TRAINING_H_

#include "storage/graph/graph.h"
#include "storage/graph/graph_delta.h"
#include "storage/graph/graph_main.h"
#include "concurrency/rti_thread.h"

#include <unistd.h>
#include <ios>
#include <iostream>
#include <fstream>
#include <string>

#include <torch/torch.h>

namespace storage {
    typedef std::unordered_map<uint64_t, torch::Tensor> TensorMap;
    class GCNLayer : public torch::nn::Module{
        public:
            class Linear{
                public:
                    Linear(uint32_t input_size, uint32_t output_size);
                    ~Linear() = default;
                    torch::Tensor forward(torch::Tensor& x);
                protected:
                    std::shared_ptr<torch::Tensor> w_;
                    std::shared_ptr<torch::Tensor> b_;
            };

            GCNLayer(uint32_t input_size, uint32_t output_size, bool incoming);
            ~GCNLayer(){};
            std::shared_ptr<TensorMap> forward(Graph* graph, std::shared_ptr<TensorMap>& x, uint32_t k = 5);
            std::shared_ptr<TensorMap> forward(Graph* graph, std::shared_ptr<TensorMap>& x, uint32_t dop, uint32_t k = 5);

        protected:
            std::shared_ptr<Linear> fc_;
            bool incoming_;
    };

    class GCNNodeClassifier : public torch::nn::Module{
        public:
            GCNNodeClassifier(Graph* graph_, int32_t feature_fid, int32_t label_fid, 
                            uint32_t input_size, uint32_t hidden_size, uint32_t num_class, 
                            bool incoming, double lr=1e-3);
            ~GCNNodeClassifier();
            void getTrainingData(LongVec& labels, std::shared_ptr<TensorMap>& features);
            void train(uint32_t num_epochs = 1, uint32_t dop = 4, uint32_t k = 5);
            std::shared_ptr<TensorMap> forward(std::shared_ptr<TensorMap>& features, uint32_t dop, uint32_t k = 5);
            void backward(std::shared_ptr<TensorMap>& backward, LongVec& labels, uint32_t dop);

        protected:
            Graph* graph_;
            int32_t feature_fid_;
            int32_t label_fid_;
            std::unordered_map<uint64_t, uint64_t> vertex_ids_;
            std::vector<uint64_t> vertex_idx_;
            GCNLayer* input_layer_;
            GCNLayer* hidden_layer_;
            GCNLayer* pred_layer_;
            std::shared_ptr<torch::optim::Adam> optimizer_;
    };

    inline void getMemoryUsage(){
        double vm_usage = 0.0;
        double resident_set = 0.0;
        std::ifstream stat_stream("/proc/self/stat",std::ios_base::in); //get info from proc
        //create some variables to get info
        std::string pid, comm, state, ppid, pgrp, session, tty_nr;
        std::string tpgid, flags, minflt, cminflt, majflt, cmajflt;
        std::string utime, stime, cutime, cstime, priority, nice;
        std::string O, itrealvalue, starttime;
        unsigned long vsize;
        long rss;
        stat_stream >> pid >> comm >> state >> ppid >> pgrp >> session >> tty_nr
        >> tpgid >> flags >> minflt >> cminflt >> majflt >> cmajflt
        >> utime >> stime >> cutime >> cstime >> priority >> nice
        >> O >> itrealvalue >> starttime >> vsize >> rss; // don't care
        stat_stream.close();
        long page_size_kb = sysconf(_SC_PAGE_SIZE) / 1024; // for x86-64 is configured
        vm_usage = vsize / 1024.0 / 1024;
        resident_set = rss * page_size_kb  / 1024;
        std::cout << vm_usage << " " << resident_set << std::endl;
    }

};


#endif

