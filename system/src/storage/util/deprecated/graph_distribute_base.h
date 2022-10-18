#ifndef STORAGE_UTIL_GRAPH_DISTRIBUTE_BASE_H_
#define STORAGE_UTIL_GRAPH_DISTRIBUTE_BASE_H_

namespace storage{
class GraphDistributeBase{
    public:
        virtual void distributeVertices() = 0;
        virtual void distributeEdges() = 0;
        virtual void waitVDistribute() = 0;
        virtual void waitEDistribute() = 0;
};  
};

#endif