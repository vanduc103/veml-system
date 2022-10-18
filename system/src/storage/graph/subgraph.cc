#include "common/def_const.h"
#include "storage/util/util_func.h"
#include "storage/graph/subgraph.h"
#include "util/timer.h"

namespace storage{

SubGraph::~SubGraph() {}

SubGraph::SubGraph(std::map<int64_t, int64_t> &newv2oldv,
                std::unordered_map<int64_t, std::vector<int64_t>> &src, 
                std::unordered_map<int64_t, std::vector<int64_t>> &dst,
                std::unordered_map<std::string, EvalVec>& ndata, 
                EvalVec &edata, std::unordered_map<int32_t, int64_t>& num_nodes, 
                StrVec& vprop_map):
    newv2oldv_(newv2oldv), src_(src), dest_(dst), ndata_(ndata), vprop_map_(vprop_map), edata_(edata), num_nodes_(num_nodes){
}

void* SubGraph::getNodeProperties(
        uint32_t col, std::string& prop_name, uint32_t dop){
    int64_t num_cols = numProps(prop_name);
    EvalVec& ndata = getProps(prop_name);
    uint32_t nsize = ndata.size();
    uint32_t num_node = numNodes(prop_name);
    if(num_node > nsize){
        std::string err("node data and nodes are not matched");
        RTI_EXCEPTION(err);
    }
    EvalType type = ndata[col].getEvalType();

    uint32_t i = col;
    if(type == ET_DOUBLE){
        ndata_d.reserve(num_node);
        uint32_t j = 0;
        while(i < nsize){
            ndata_d[j] = ndata[i].getDouble().get();
            i += num_cols;
            j++;
        }

        return ndata_d.data();
    }else if(type == ET_BIGINT){
        ndata_i.reserve(num_node);
        uint32_t j = 0;
        while(i < nsize){
            ndata_i[j] = ndata[i].getBigInt().get();
            i += num_cols;
            j++;
        }
        return ndata_i.data();
    }else if (type == ET_FLOATLIST){
        if(dop <= 1){
            while(i < nsize){
                FloatList& dl = ndata[i].getFloatList();
                dl.copyTo(ndata_f);
                i += num_cols;
            }
        }else{
            class DataWorker : public RTIThread{
              public:
                DataWorker(EvalVec& ndata, uint32_t start,
                        uint32_t end, uint32_t offset, uint32_t num_cols)
                    : ndata_(ndata), start_(start), end_(end), offset_(offset),
                    num_cols_(num_cols){};
                void* run(){
                    uint32_t i = start_ * num_cols_ + offset_;
                    end_ = end_ * num_cols_ + offset_;
                    while(i < end_){
                        FloatList& dl = ndata_[i].getFloatList();
                        dl.copyTo(data_);
                        i += num_cols_;
                    }
                    return nullptr;
                }

                EvalVec& ndata_;
                uint32_t start_;
                uint32_t end_;
                uint32_t offset_;
                uint32_t num_cols_;
                std::vector<float> data_;
            };
            std::vector<DataWorker> dworkers;
            uint32_t d = num_node / dop;
            uint32_t m = num_node % dop;
            uint32_t start, end = 0;
            for(uint32_t t = 0; t < dop; t++){
                start = end;
                end = start + d;
                if(t < m) end++;
                dworkers.emplace_back(ndata, start, end, col, num_cols);
            }        
            for(auto& w : dworkers) w.start();
            for(auto& w : dworkers) w.join();
            for(auto& w : dworkers){
                ndata_f.reserve(ndata_f.size() + w.data_.size());
                std::copy(w.data_.begin(), w.data_.end(),
                        std::back_inserter(ndata_f));
            }
        }
        return ndata_f.data();
    }else if (type == ET_DOUBLELIST){
        if(dop <= 1){
            while(i < nsize){
                DoubleList& dl = ndata[i].getDoubleList();
                dl.copyTo(ndata_d);
                i += num_cols;
            }
        }else{
            class DataWorker : public RTIThread{
              public:
                DataWorker(EvalVec& ndata, uint32_t start,
                        uint32_t end, uint32_t offset, uint32_t num_cols)
                    : ndata_(ndata), start_(start), end_(end), offset_(offset),
                    num_cols_(num_cols){};
                void* run(){
                    uint32_t i = start_ * num_cols_ + offset_;
                    end_ = end_ * num_cols_ + offset_;
                    while(i < end_){
                        DoubleList& dl = ndata_[i].getDoubleList();
                        dl.copyTo(data_);
                        i += num_cols_;
                    }
                    return nullptr;
                }

                EvalVec& ndata_;
                uint32_t start_;
                uint32_t end_;
                uint32_t offset_;
                uint32_t num_cols_;
                std::vector<double> data_;
            };
            std::vector<DataWorker> dworkers;
            uint32_t d = num_node / dop;
            uint32_t m = num_node % dop;
            uint32_t start, end = 0;
            for(uint32_t t = 0; t < dop; t++){
                start = end;
                end = start + d;
                if(t < m) end++;
                dworkers.emplace_back(ndata, start, end, col, num_cols);
            }        
            for(auto& w : dworkers) w.start();
            for(auto& w : dworkers) w.join();
            for(auto& w : dworkers){
                ndata_d.reserve(ndata_d.size() + w.data_.size());
                std::copy(w.data_.begin(), w.data_.end(),
                        std::back_inserter(ndata_d));
            }
        }
        return ndata_d.data();
    }

    return nullptr;
}

uint32_t SubGraph::getVectorSize(uint32_t col, std::string& prop_name){
    EvalVec& ndata = getProps(prop_name);
    if(numNodes(prop_name) > (int)ndata.size()){
        std::string err("node data and nodes are not matched");
        RTI_EXCEPTION(err);
    }
    EvalType type = ndata[col].getEvalType();
    if (type == ET_INTLIST) {
        IntList& il = ndata[col].getIntList();
        return il.size();
    } else if (type == ET_LONGLIST){
        LongList& ll = ndata[col].getLongList();
        return ll.size();
    } else if (type == ET_FLOATLIST){
        FloatList& fl = ndata[col].getFloatList();
        return fl.size();
    } else if (type == ET_DOUBLELIST) {
        DoubleList& dl = ndata[col].getDoubleList();
        return dl.size();
    }
    return 0;
}

}
