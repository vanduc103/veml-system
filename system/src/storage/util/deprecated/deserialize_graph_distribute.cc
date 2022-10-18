#include "common/def_const.h"
#include "storage/catalog.h"
#include "storage/util/deserialize_graph_distribute.h"
#include "storage/graph/graph.h"

#include <iostream>
#include <boost/asio.hpp>
#include <boost/asio/io_service.hpp>
#include <boost/asio/write.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/make_shared.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/serialization/vector.hpp>


using namespace boost::asio;
using namespace boost::asio::ip;

namespace storage {

void DeserializeGraphDistribute::deserializeValue(boost::archive::binary_iarchive& ia, EvalValue& val){
    EvalType evaltype;
    ia >> evaltype;
    switch (evaltype){
        case ET_BIGINT: {
            int64_t v;
            ia >> v;
            val = EvalValue(BigInt(v));
            break;
        }
        case ET_DOUBLE: {
            double v;
            ia >> v;
            val = EvalValue(Double(v));
            break;
        }
        case ET_STRING: {
            std::string v;
            ia >> v;
            val = EvalValue(String(v));
            break;
        }
        case ET_BOOL: {
            bool v;
            ia >> v;
            val = EvalValue(Bool(v));
            break;
        }
        case ET_DOUBLEVEC: {
            DoubleVec v;
            ia >> v;
            val = EvalValue(v);
            break;
        }
        case ET_INTVEC: {
            IntVec v;
            ia >> v;
            val = EvalValue(v);
            break;
        }
        default: {
            std::string err("invalid value type @deserializeValue, ");
            RTI_EXCEPTION(err);
        }
    }
}

void DeserializeGraphDistribute::deserializeRecord(boost::archive::binary_iarchive& ia, EvalVec& props){
    size_t size;
    ia >> size;
    for(size_t i = 0; i < size; i++){
        EvalValue v;
        deserializeValue(ia, v);
        props.push_back(v);
    }
}

void DeserializeGraphDistribute::deserializeTable(boost::archive::binary_iarchive& ia, std::vector<EvalVec>& props){
    size_t size;
    ia >> size;
    for(size_t i = 0; i < size; i++){
        EvalVec v;
        deserializeRecord(ia, v);
        props.push_back(v);
    }
}

void DeserializeGraphDistribute::deserializeVertexBatch(std::string& msg){
    try{
        std::stringstream is;
        is << msg;
        boost::archive::binary_iarchive ia(is);
        std::string gname;
        ia >> gname;
        Graph* graph = Metadata::getGraphFromName(gname);
        // 1. deserialize vertex
        std::vector<EvalVec> vprops;
        deserializeTable(ia, vprops);
        IntVec vpids;
        ia >> vpids;
        // 2. insert vertex to graph
        graph->insertVertexBatch(vprops, vpids, 0, 16);
    }catch(std::exception& e){
        std::cout << "de" << e.what() << std::endl;
    }
    
}

void DeserializeGraphDistribute::deserializeEdgeBatch(std::string& msg){
    try{
        std::stringstream is;
        is << msg;
        boost::archive::binary_iarchive ia(is);
        std::string gname;
        ia >> gname;
        Graph* graph = Metadata::getGraphFromName(gname);
        // 1. deserialize edges
        EvalVec src, dest;
        deserializeRecord(ia, src);
        deserializeRecord(ia, dest);
        IntVec src_vpid, dest_vpid;
        ia >> src_vpid;
        ia >> dest_vpid;
        std::vector<EvalVec> eprops;
        deserializeTable(ia, eprops);
        bool use_incoming, only_in;
        ia >> use_incoming;
        ia >> only_in;
        // 2. deserialize edges
        graph->insertEdgeBatch(src, dest, eprops, src_vpid, dest_vpid, !use_incoming, only_in);
    }catch(std::exception& e){
        std::cout << "de" << e.what() << std::endl;
    }
}

void DeserializeGraphDistribute::deserializeGraph(std::vector<std::string>& msg){
    try{
        if(msg.size() < 1){
            std::string err("Serialized messages must be at least 1");
            RTI_EXCEPTION(err);
        }        
        // 2. deserialize graph
        Graph::deserializeGraphPartition(msg);
    }catch(std::exception& e){
        std::cout << "de" << e.what() << std::endl;
    }
}

};