#ifndef STORAGE_GRAPH_SUBGRAPH_H
#define STORAGE_GRAPH_SUBGRAPH_H
#include "common/def_const.h"
#include "storage/util/util_func.h"

namespace storage{

class SubGraph{
  public:
    SubGraph(std::map<int64_t, int64_t> &newv2oldv,
            std::unordered_map<int64_t, std::vector<int64_t>> &src, 
            std::unordered_map<int64_t, std::vector<int64_t>> &dst,
            std::unordered_map<std::string, EvalVec>& ndata,                 
            EvalVec &edata, std::unordered_map<int32_t, int64_t>& num_nodes, 
            StrVec& vprop_map);
    virtual ~SubGraph();
  
    std::vector<int64_t> getVertexNativeId(){
        std::vector<int64_t> res;
        for(auto it : newv2oldv_){
            res.push_back(it.second);
        }
        return res;
    }

    std::vector<int64_t> getVertexNewId(){
        std::vector<int64_t> res;
        for(auto it : newv2oldv_){
            res.push_back(it.first);
        }
        return res;
    }

    LongVec getAllEdgeType(){
        LongVec edge_types;
        for(auto itr : src_){
            int64_t edge_type_id = itr.first;
            edge_types.push_back(edge_type_id);
        }
        return edge_types;
    }

    StrVec& getVpropMap(){
        return vprop_map_;
    }

    void* getSrc(int64_t edge_type_id){
        std::vector<int64_t>& src = src_[edge_type_id];
        return src.data();
    }

    void* getDest(int64_t edge_type_id){
        std::vector<int64_t>& dst = dest_[edge_type_id];
        return dst.data();
    }

    /**
     * Properties of all nodes in graph
     * length = num_nodes * #columns
     * @param int col: which column to get properties;
     *  -1 means get all (all columns should have similar types)
     */
    void* getNodeProperties(uint32_t col, std::string& prop_name, uint32_t dop = 1);

    void* getEdgeProperties(){
        return nullptr;
    }

    uint32_t getVectorSize(uint32_t col, std::string& prop_name);

    int64_t numEdges(int64_t edge_type_id){
        std::vector<int64_t>& src = src_[edge_type_id];
        return src.size();
    }
    
    // should revise for hetero
    int64_t numNodes(std::string& prop_name){
        int32_t vpid = -1;
        for(size_t i = 0; i < vprop_map_.size(); i++){
            if(prop_name.compare(vprop_map_[i]) == 0){
                vpid = i;
                break;
            }
        }
        if(vpid < 0)
            return num_nodes_.begin()->second;
            
        return num_nodes_[vpid];
    }

    EvalVec& getProps(std::string& prop_name){
        EvalVec& ndata = prop_name.empty() ? ndata_.begin()->second : ndata_[prop_name];
        return ndata;
    }

    int64_t numProps(std::string& prop_name){
        size_t size = 0;
        size = getProps(prop_name).size();
        return size / numNodes(prop_name);
    }

  protected:
    std::map<int64_t, int64_t> newv2oldv_;
    std::unordered_map<int64_t, std::vector<int64_t>> src_;
    std::unordered_map<int64_t, std::vector<int64_t>> dest_;
    std::unordered_map<std::string, EvalVec> ndata_;
    StrVec vprop_map_;
    EvalVec edata_;
    std::unordered_map<int32_t, int64_t> num_nodes_;

    std::vector<int64_t> ndata_i;
    std::vector<float> ndata_f;
    std::vector<double> ndata_d;
};

}
#endif
