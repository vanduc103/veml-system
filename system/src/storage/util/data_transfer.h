// Copyright 2021 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

// Project include
#include "storage/def_const.h"

// Project include
#include "storage/relation/table.h"
#include "storage/graph/graph.h"

// Other include
#include <grpcpp/grpcpp.h>

// gRPC generated file
#include "interface/interface.grpc.pb.h"

namespace storage {

class TransferRequester {
  public:
    class TransferObject {
      public:
        enum Type { TABLE, GRAPH, PA, SGMT } ;
        TransferObject(Type type, std::string addr)
            : type_(type), addr_(addr) {}

        Type type_;
        std::string addr_;
    };

    class TransferTable : public TransferObject {
      public:
        TransferTable(std::string addr, Table* table, std::string tname)
            : TransferObject(TransferObject::TABLE, addr),
            table_(table), tname_(tname) {}
        Table* table_;
        std::string tname_;
    };

    class TransferGraph : public TransferObject {
      public:
        TransferGraph(std::string addr, Graph* graph, std::string gname)
            : TransferObject(TransferObject::GRAPH, addr),
            graph_(graph), gname_(gname) {}
        Graph* graph_;
        std::string gname_;
    };

    class TransferPA: public TransferObject {
      public:
        TransferPA(std::string addr, int32_t pa_id)
            : TransferObject(TransferObject::PA, addr),
            pa_id_(pa_id) {}
        int32_t pa_id_;
    };

    class TransferSGMT : public TransferObject {
      public:
        TransferSGMT(std::string addr, int32_t pa_id, int sgmt_id)
            : TransferObject(TransferObject::SGMT, addr),
            pa_id_(pa_id), sgmt_id_(sgmt_id) {}
        int32_t pa_id_;
        int32_t sgmt_id_;
    };

    class TransferWorker : public RTIThread {
      public:
        TransferWorker(int start, int end,
                std::vector<TransferObject*>& objects)
            : start_(start), end_(end), objects_(objects) {}

        void* run();

      private:
        int start_;
        int end_;
        std::vector<TransferObject*>& objects_;
    };
    
    TransferRequester(int dop=8) : dop_(dop) {}

    void add(std::string addr, Table* table, std::string tname);
    void add(std::string addr, Graph* graph, std::string gname);
    void add(std::string addr, uint32_t pa_id);
    void run();

  private:
    int dop_;
    std::vector<TransferObject*> table_;
    std::vector<TransferObject*> graph_;
    std::vector<TransferObject*> paged_array_;
    std::vector<TransferObject*> segment_;
};

class TransferHandler {
  public:
    TransferHandler(int32_t type, const std::string& name,
            int32_t pa_id, int32_t sgmt_id, const std::string& binary)
        : type_((TransferRequester::TransferObject::Type)type), name_(name),
        pa_id_(pa_id), sgmt_id_(sgmt_id), binary_(binary) {}

    void run();

    TransferRequester::TransferObject::Type type_;
    const std::string name_;
    int32_t pa_id_;
    int32_t sgmt_id_;
    std::string binary_;
};

}  // namespace storage
