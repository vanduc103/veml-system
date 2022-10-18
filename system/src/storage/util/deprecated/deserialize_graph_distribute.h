#ifndef STORAGE_UTIL_DESERIALIZE_GRAPH_DISTRIBUTE_H_
#define STORAGE_UTIL_DESERIALIZE_GRAPH_DISTRIBUTE_H_

#include "common/def_const.h"
#include "storage/catalog.h"
#include <boost/asio/io_service.hpp>
#include <boost/asio/write.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/archive/binary_iarchive.hpp>

using namespace boost::asio;
using namespace boost::asio::ip;

namespace storage{

class DeserializeGraphDistribute{
    public:
        DeserializeGraphDistribute() = default;
        ~DeserializeGraphDistribute() = default;

        void deserializeValue(boost::archive::binary_iarchive& ia, EvalValue& val);
        void deserializeRecord(boost::archive::binary_iarchive& ia, EvalVec& props);
        void deserializeTable(boost::archive::binary_iarchive& ia, std::vector<EvalVec>& props);

        void deserializeVertexBatch(std::string& msg);
        void deserializeEdgeBatch(std::string& msg);
        
        void deserializeGraph(std::vector<std::string>& msg);
        
};
};

#endif