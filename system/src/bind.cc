// Project include
#include "storage/catalog.h"
#include "common/types/user_defined/metadata.h"
#include "storage/relation/row_table.h"
#include "storage/relation/column_table.h"
#include "storage/graph/graph.h"
//#include "storage/graph/graph_sparse_training.h"
#include "storage/graph/subgraph.h"
#include "storage/util/data_transfer.h"
#include "storage/util/py_dataloader.h"
#include "persistency/archive_manager.h"

// Other include
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/numpy.h>

//#include <torch/torch.h>

namespace py = pybind11;

PYBIND11_MODULE(rti_python_lib, m) {
    m.doc() = "RTI python module";
    
    // Metadata interface(table)
    m.def("init_metadata", &Metadata::initMetadata)
        .def("create_table", &Table::create, "",
                py::arg("type"), py::arg("name"), py::arg("fields"),
                py::arg("increment_fid") = -1,
                py::arg("increment_start") = 0,
                py::return_value_policy::reference)
        .def("create_by_schema", &Table::createFromFile,
                "", py::arg("type"), py::arg("name"),
                py::arg("schema"), py::arg("delim"),
                py::return_value_policy::reference)
        .def("load_table", &Table::load,
                "", py::arg("type"), py::arg("name"), py::arg("schema"),
                py::arg("csv"), py::arg("delim"),
                py::arg("seq") = false, py::arg("header") = true,
                py::return_value_policy::reference)
        .def("drop_table", &Metadata::releaseTable)
        .def("get_table_type", &Metadata::getTableType,
                "", py::return_value_policy::reference)
        .def("get_row_table", &Metadata::getRowTable,
                "", py::return_value_policy::reference)
        .def("get_col_table", &Metadata::getColTable,
                "", py::return_value_policy::reference)
        .def("table_exists", &Metadata::tableExists)
        .def("create_index", &Table::createIndex)
        .def("memory_statistics", &Metadata::memoryStat);

    // Metadata interface(graph)
    m.def("create_graph", &Graph::create, "", py::arg("name"),
            py::arg("vpname"), py::arg("vpfields"),
            py::arg("vpfid"), py::arg("vptype"),
            py::arg("epfields"), py::arg("eptype"),
            py::arg("use_incoming"), py::arg("merge_bot_on"),
            py::arg("is_partition"), py::arg("part_type"), py::arg("nodes"),
            py::return_value_policy::reference)
        .def("drop_graph", &Metadata::releaseGraph)
        .def("get_graph", &Metadata::getGraphFromName,
                "", py::return_value_policy::reference)
        .def("graph_exists", &Metadata::graphExists);

    py::class_<persistency::ArchiveManager>(m, "persistency")
        .def_static("archive", static_cast<bool (*) (const std::string)>
                (&persistency::ArchiveManager::archive))
        .def_static("recover", static_cast<bool (*) (const std::string, int)>
                (&persistency::ArchiveManager::recover));

    py::class_<CreateFieldInfo>(m, "create_field_info")
        .def(py::init<std::string, ValueType>());

    py::enum_<Table::Type>(m, "table_type")
        .value("row", Table::Type::ROW)
        .value("col", Table::Type::COLUMN)
        .export_values();

    py::enum_<ValueType>(m, "value_type")
        .value("vt_bigint", ValueType::VT_BIGINT)
        .value("vt_int", ValueType::VT_INT)
        .value("vt_double", ValueType::VT_DOUBLE)
        .value("vt_float", ValueType::VT_FLOAT)
        .value("vt_decimal", ValueType::VT_DECIMAL)
        .value("vt_varchar", ValueType::VT_VARCHAR)
        .value("vt_char", ValueType::VT_CHAR)
        .value("vt_bool", ValueType::VT_BOOL)
        .value("vt_timestamp", ValueType::VT_TIMESTAMP)
        .value("vt_date", ValueType::VT_DATE)
        .value("vt_intlist", ValueType::VT_INTLIST)
        .value("vt_longlist", ValueType::VT_LONGLIST)
        .value("vt_floatlist", ValueType::VT_FLOATLIST)
        .value("vt_doublelist", ValueType::VT_DOUBLELIST)
        .value("vt_udt", ValueType::VT_UDT)
        .export_values();

    py::enum_<EvalType>(m, "eval_type")
        .value("et_bigint", EvalType::ET_BIGINT)
        .value("et_double", EvalType::ET_DOUBLE)
        .value("et_string", EvalType::ET_STRING)
        .value("et_bool", EvalType::ET_BOOL)
        .value("et_intlist", EvalType::ET_INTLIST)
        .value("et_longlist", EvalType::ET_LONGLIST)
        .value("et_floatlist", EvalType::ET_FLOATLIST)
        .value("et_doublelist", EvalType::ET_DOUBLELIST)
        .value("et_intvec", EvalType::ET_INTVEC)
        .value("et_longvec", EvalType::ET_LONGVEC)
        .value("et_floatvec", EvalType::ET_FLOATVEC)
        .value("et_doublevec", EvalType::ET_DOUBLEVEC)
        .export_values();

    py::class_<AddressPair>(m, "address");

    py::class_<Table>(m, "table")
        .def("get_field_info", [](Table* table){
            FieldInfoVec finfos;
            table->getFieldInfo(finfos);
            py::dict fields;
            int i = 0;
            for(auto fi : finfos){
                fields[fi->getFieldName()] = i;
                i++;
            }
            return fields;
        });

    py::class_<RowTable>(m, "row_table")
        .def("name", &Table::getTableName)
        .def("create_index", &Table::createIndex)
        .def("import_csv", &Table::importCSV)
        .def("partition", &Table::partition)
        .def("is_partitioned", &Table::isPartitioned)
        .def("set_partition", &Table::setPartition)
        .def("find_node", &Table::findNode)
        .def("part_nodes", &Table::getPartNodes)
        .def("insert_record", (AddressPair
                (RowTable::*)(EvalVec&)) &RowTable::insertRecord)
        .def("insert_record_batch", (bool
                (Table::*)(std::vector<EvalVec>&)) &Table::insertRecord)
        .def("num_fields", &Table::getNumFields)
        .def("get_field_info", &Table::getFieldInfoStr)
        .def("get_part_fid", &Table::getPartFid)
        .def("get_num_records", &Table::getNumRecords)
        .def("get_record", (EvalVec
                (RowTable::*)(LogicalPtr)) &RowTable::getRecord)
        .def("get_record", (EvalVec
                (RowTable::*)(LogicalPtr, IntVec&)) &RowTable::getRecord)
        .def("index_search", (std::vector<EvalVec>
                    (Table::*)(int64_t fid, EvalValue& val, IntVec fids))
                    &Table::indexSearch)
/**        .def("to_tensor_i", [](RowTable* table) {
            torch::Tensor tensor = table->toTensor("int");
            auto a = tensor.sizes();
            return py::array_t<int>(
                    {a[0], a[1]}, tensor.data_ptr<int>());
        }, py::return_value_policy::reference)
        .def("to_tensor_l", [](RowTable* table) {
            torch::Tensor tensor = table->toTensor("long");
            auto a = tensor.sizes();
            return py::array_t<long>(
                    {a[0], a[1]}, tensor.data_ptr<long>());
        }, py::return_value_policy::reference)
        .def("to_tensor_f", [](RowTable* table) {
            torch::Tensor tensor = table->toTensor("float");
            auto a = tensor.sizes();
            return py::array_t<float>(
                    {a[0], a[1]}, tensor.data_ptr<float>());
        }, py::return_value_policy::reference)
        .def("to_tensor_d", [](RowTable* table) {
            torch::Tensor tensor = table->toTensor("double");
            auto a = tensor.sizes();
            return py::array_t<double>(
                    {a[0], a[1]}, tensor.data_ptr<double>());
        }, py::return_value_policy::reference)*/
        .def("begin", (RowTable::iterator
                (RowTable::*)()) &RowTable::begin)
        .def("get_field_info", [](RowTable* table){
            FieldInfoVec finfos;
            table->getFieldInfo(finfos);
            py::dict fields;
            int i = 0;
            for(auto fi : finfos){
                fields[fi->getFieldName()] = i;
                i++;
            }
            return fields;
        });

    py::class_<RowTable::iterator>(m, "row_table_iter")
        .def("next", &RowTable::iterator::next)
        .def("good", &RowTable::iterator::good)
        .def("get", (EvalVec
                (RowTable::iterator::*)())
                    &RowTable::iterator::getRecord)
        .def("get", (EvalVec
                (RowTable::iterator::*)(IntVec&))
                    &RowTable::iterator::getRecord)
        .def("to_strvec", (StrVec
                (RowTable::iterator::*)())
                    &RowTable::iterator::getRecordStr)
        .def("to_strvec", (StrVec
                (RowTable::iterator::*)(IntVec&))
                    &RowTable::iterator::getRecordStr)
        .def("ptr", (void* (RowTable::iterator::*) ())
                    &RowTable::iterator::getPtr)
        .def("lptr", &RowTable::iterator::getLptr);

    py::class_<ColumnTable>(m, "col_table")
        .def("name", &Table::getTableName)
        .def("create_index", &Table::createIndex)
        .def("import_csv", &Table::importCSV)
        .def("partition", &Table::partition)
        .def("is_partitioned", &Table::isPartitioned)
        .def("set_partition", &Table::setPartition)
        .def("find_node", &Table::findNode)
        .def("part_nodes", &Table::getPartNodes)
        .def("insert_record", (AddressPair
                (ColumnTable::*)(std::vector<EvalValue>&))
                &ColumnTable::insertRecord)
        .def("insert_record_batch", (bool
                (Table::*)(std::vector<EvalVec>&)) &Table::insertRecord)
        .def("num_fields", &Table::getNumFields)
        .def("get_field_info", &Table::getFieldInfoStr)
        .def("get_part_fid", &Table::getPartFid)
        .def("get_num_records", &Table::getNumRecords)
        .def("get_record", (EvalVec
                (ColumnTable::*)(LogicalPtr)) &ColumnTable::getRecord)
        .def("get_record", (EvalVec
                (ColumnTable::*)(LogicalPtr, IntVec&)) &ColumnTable::getRecord)
        .def("index_search", (std::vector<EvalVec>
                    (Table::*)(int64_t fid, EvalValue& val, IntVec fids))
                    &Table::indexSearch)
        .def("begin", (ColumnTable::iterator
                (ColumnTable::*)()) &ColumnTable::begin)
        .def("get_field_info", [](ColumnTable* table) {
            FieldInfoVec finfos;
            table->getFieldInfo(finfos);
            py::dict fields;
            int i = 0;
            for(auto fi : finfos){
                fields[fi->getFieldName()] = i;
                i++;
            }
            return fields;
        });

    py::class_<ColumnTable::iterator>(m, "col_table_iter")
        .def("next", &ColumnTable::iterator::next)
        .def("good", &ColumnTable::iterator::good)
        .def("get", (EvalVec
                (ColumnTable::iterator::*)())
                    &ColumnTable::iterator::getRecord)
        .def("get", (EvalVec
                (ColumnTable::iterator::*)(IntVec&))
                    &ColumnTable::iterator::getRecord)
        .def("to_strvec", (StrVec
                (ColumnTable::iterator::*)())
                    &ColumnTable::iterator::getRecordStr)
        .def("to_strvec", (StrVec
                (ColumnTable::iterator::*)(IntVec&))
                    &ColumnTable::iterator::getRecordStr)
        .def("ptr", (void* (ColumnTable::iterator::*) ())
                    &ColumnTable::iterator::getPtr)
        .def("lptr", &ColumnTable::iterator::getLptr);

    py::class_<TransferHandler>(m, "transfer_handler")
        .def(py::init<int32_t, const std::string&,
                int32_t, int32_t, const std::string&>())
        .def("run", &TransferHandler::run);

    py::class_<Graph>(m, "graph")
        .def("get_name", &Graph::getName)
        .def("get_vpname", &Graph::getVPName)
        .def("get_vpfields", &Graph::getVPFields)
        .def("get_epfields", &Graph::getEPFields)
        .def("use_incoming", &Graph::useIncoming)
        .def("partition", [](Graph* graph,
                    std::string& type, StrVec& nodes, py::kwargs kwargs) {
            py::gil_scoped_release release{};
            if(kwargs && kwargs.contains("dop"))
                return graph->partition(type, nodes, kwargs["dop"].cast<int32_t>());
            return graph->partition(type, nodes, 1);
        })
        .def("is_partitioned", &Graph::isPartitioned)
        .def("part_nodes", &Graph::getPartNodes)
        .def("find_node", &Graph::findNode)
        .def("merge", (void (Graph::*)(size_t)) &Graph::mergeManually)
        .def("merge", (void (Graph::*)(size_t, size_t)) &Graph::mergeManually)
        .def("get_num_vertex", &Graph::getNumVertex)
        .def("get_num_edges", &Graph::getNumEdges)
        .def("define_vertex", &Graph::defineVertex)
        .def("vertex_prop_exists", &Graph::vertexPropExists)
        .def("find_vertex_prop_id", &Graph::findVertexPropId)
        .def("import_vertex", &Graph::importVertex)
        .def("insert_vertex", (int64_t
                (Graph::*)(EvalVec&, int32_t)) &Graph::insertVertex)
        .def("insert_vertex_batch", (bool
                (Graph::*)(std::vector<EvalVec>&, IntVec&, int32_t, int32_t))
                &Graph::insertVertexBatch, py::call_guard<py::gil_scoped_release>())
        .def("insert_vertex_batch", [](Graph* graph, std::shared_ptr<MeshEvalWrapper>& values, IntVec& vpids, int32_t vpid, int32_t dop){
            graph->insertVertexBatch(values->data_, vpids, vpid, dop);
        }, py::call_guard<py::gil_scoped_release>())
        .def("import_edge", &Graph::importEdge)
        .def("insert_edge", (bool
                (Graph::*)(int64_t, int64_t, EvalVec&, 
                    bool, bool)) &Graph::insertEdge)
        .def("insert_edge", (bool
                (Graph::*)(EvalValue, EvalValue, EvalVec&,
                    int32_t, int32_t, bool, bool)) &Graph::insertEdge)
        .def("insert_edge", (bool
                (Graph::*)(EvalVec&, EvalVec&, EvalVec&,
                    int32_t, int32_t)) &Graph::insertEdge)
        .def("insert_edge_batch", (bool
                (Graph::*)(EvalVec&, EvalVec&, std::vector<EvalVec>&,
                    IntVec&, IntVec&, bool, bool, int))
                &Graph::insertEdgeBatch, py::call_guard<py::gil_scoped_release>())
        .def("insert_edge_batch", [](Graph* graph, std::shared_ptr<EvalVecWrapper>& src, std::shared_ptr<EvalVecWrapper>& dst,
                                    std::vector<EvalVec>& erop, std::shared_ptr<IntVecWrapper>& svprop, std::shared_ptr<IntVecWrapper>& dvprop,
                                    bool ignore_in, bool only_in, int dop){
            graph->insertEdgeBatch(src->data_, dst->data_, erop, svprop->data_, dvprop->data_, ignore_in, only_in, dop);
        }, py::call_guard<py::gil_scoped_release>())
        .def("adjacent_list", [](Graph* graph, EvalValue src,
                    IntVec& fids, bool incoming, int32_t vpid) {
                std::vector<EvalVec> dest;
                EvalVec eprops;
                graph->adjacentList(src, dest, fids, eprops, incoming, vpid);
                return std::make_tuple(dest, eprops);
            })
        .def("get_edge_list", [](Graph* graph) {
                EvalVec src, dest;
                graph->getEdgeList(src, dest);
                return std::make_tuple(src, dest);
            })
        .def("get_vertex", (EvalVec (Graph::*)
                    (EvalValue&, int32_t, int64_t, IntVec&)) &Graph::getVertex)
        .def("get_vertex_list", [](Graph* graph) {
                EvalVec vkey;
                IntVec vpid;
                std::vector<EvalVec> vprop;
                graph->getVertexList(vkey, vpid, vprop);
                return std::make_tuple(std::make_tuple(vkey, vpid), vprop);
            })
        .def("vertex_exists", (bool (Graph::*) (EvalValue&, int32_t)) &Graph::vertexExists)
        .def("random_walk_sample", &Graph::randomWalkSample,  
            py::return_value_policy::take_ownership,
            py::call_guard<py::gil_scoped_release>())
        .def("node_subgraph", [](Graph* graph, LongVec& seeding_nodes, IntVec& vfids, IntVec& efids, py::kwargs kwargs){
            // py::gil_scoped_release release;
            if(kwargs && kwargs.contains("dop"))
                return graph->sampleNodeSubgraph(seeding_nodes, vfids, efids, kwargs["dop"].cast<uint32_t>());
            
            return graph->sampleNodeSubgraph(seeding_nodes, vfids, efids, 1);
        }, py::return_value_policy::take_ownership, py::call_guard<py::gil_scoped_release>())
        .def("hetero_subgraph",
                &Graph::sampleHeteroSubGraph,
                py::return_value_policy::take_ownership,
                py::call_guard<py::gil_scoped_acquire>())
        .def("sample_neighbors",
                &Graph::sample_neighbors,
                py::return_value_policy::take_ownership,
                py::call_guard<py::gil_scoped_acquire>());

    py::class_<SubGraph>(m, "subgraph")
        .def("src", &SubGraph::getSrc, py::return_value_policy::reference)
        .def("dest", &SubGraph::getDest, py::return_value_policy::reference)
        .def("vertex_native_ids", &SubGraph::getVertexNativeId, py::return_value_policy::take_ownership)
        .def("vertex_new_ids", &SubGraph::getVertexNewId, py::return_value_policy::take_ownership)
        .def("ndata", [](SubGraph* sg, uint32_t col, std::string vprop_name, py::kwargs kwargs){
            py::gil_scoped_release release{};                
            if(kwargs && kwargs.contains("dop"))
                return sg->getNodeProperties(col, vprop_name, kwargs["dop"].cast<uint32_t>());
            return sg->getNodeProperties(col, vprop_name, 1);
        }, py::arg("col") = 0, py::arg("vprop_name") = "", py::return_value_policy::take_ownership)
        .def("edata", &SubGraph::getEdgeProperties, py::return_value_policy::take_ownership)
        .def("vector_size", [](SubGraph* sg, uint32_t col, std::string vprop_name){
            return sg->getVectorSize(col, vprop_name);
        }, py::arg("col") = 0, py::arg("vprop_name") = "")
        .def("num_nodes", [](SubGraph* sg, std::string vprop_name){
            return sg->numNodes(vprop_name);
        }, py::arg("vprop_name") = "")
        .def("num_edges", &SubGraph::numEdges)
        .def("get_edge_types", &SubGraph::getAllEdgeType)
        .def("get_vprop_map", &SubGraph::getVpropMap);
    /**
    py::class_<GCNSparseNodeClassifier, std::shared_ptr<GCNSparseNodeClassifier>>(m, "gcn_node_classifier")
        .def(py::init<Graph*, IntVec&, uint32_t, uint32_t, uint32_t, bool, std::string&, float, float, int, bool>())
        .def("init", &GCNSparseNodeClassifier::init, py::call_guard<py::gil_scoped_release>())
        .def("set_weights", [](GCNSparseNodeClassifier* gcn, StrVec& serialized_weights){
            TensorVec weights;
            strVecToTensorVec(serialized_weights, weights);
            gcn->setWeights(weights);
        })
        .def("get_weights", [](GCNSparseNodeClassifier* gcn){
            TensorVec weights;
            gcn->getWeights(weights);
            std::vector<py::bytes> bweights;
            bweights.reserve(weights.size());
            for(auto& w : weights){
                std::string sw;
                serializeTensor(w, sw);
                bweights.push_back(py::bytes(sw));
            }
            return bweights;
        })
        .def("get_gradients", [](GCNSparseNodeClassifier* gcn){
            TensorVec grads;
            gcn->getGradients(grads);
            std::vector<py::bytes> bgrads;
            bgrads.reserve(grads.size());
            for(auto& w : grads){
                std::string sw;
                serializeTensor(w, sw);
                bgrads.push_back(py::bytes(sw));
            }         
            return bgrads;     
        })
        .def("set_gradients", [](GCNSparseNodeClassifier* gcn, StrVec& serialized_gradients){
            TensorVec weights;
            strVecToTensorVec(serialized_gradients, weights);
            gcn->setGradients(weights);
        })
        .def("sum_gradients", [](GCNSparseNodeClassifier* gcn, std::vector<StrVec> grads){
            std::vector<TensorVec> all_grads;
            // first node grad
            TensorVec weights;
            strVecToTensorVec(grads[0], weights);
            size_t s = weights.size();
            all_grads.reserve(s);
            for(size_t i = 0; i < s; i++){
                all_grads.emplace_back();
                all_grads[i].push_back(weights[i]);
            }
            for(size_t j = 1; j < grads.size(); j++){
                TensorVec wts;
                strVecToTensorVec(grads[j], wts);
                for(size_t i = 0; i < s; i++){
                    auto& gw = all_grads[i];
                    gw.push_back(weights[i]);
                }
            }
            // sum grads correspondingn to weights
            TensorVec sum_grads;
            sum_grads.reserve(s);
            for(auto& w : all_grads){
                torch::Tensor wa = torch::stack(w, 0);
                wa = torch::sum(wa, 0);
                sum_grads.push_back(wa);
            }
            gcn->setGradients(sum_grads);
        })
        .def("init_optimizer", &GCNSparseNodeClassifier::initOptimizer, py::call_guard<py::gil_scoped_release>())
        .def("train", &GCNSparseNodeClassifier::train, py::call_guard<py::gil_scoped_release>())
        .def("eval", &GCNSparseNodeClassifier::eval, py::call_guard<py::gil_scoped_release>())
        .def("step", &GCNSparseNodeClassifier::step, py::call_guard<py::gil_scoped_release>())
        .def("empty_grad", &GCNSparseNodeClassifier::emptyGrad, py::call_guard<py::gil_scoped_release>())
        .def("step_optimizer", &GCNSparseNodeClassifier::stepOptimizer, py::call_guard<py::gil_scoped_release>())
        .def("add_scalar", &GCNSparseNodeClassifier::add_scalar);
        

    py::class_<GATNodeClassifier, std::shared_ptr<GATNodeClassifier>>(m, "gat_node_classifier")
        .def(py::init<Graph*, IntVec&, uint32_t, uint32_t, uint32_t, uint16_t, bool, std::string&, float, float, bool, int, bool>())
        .def("init", &GATNodeClassifier::init, py::call_guard<py::gil_scoped_release>())
        .def("train", &GATNodeClassifier::train, py::call_guard<py::gil_scoped_release>());
    
    py::class_<APPNPNodeClassifier, std::shared_ptr<APPNPNodeClassifier>>(m, "appnp_node_classifier")
        .def(py::init<Graph*, IntVec&, uint32_t, uint32_t, uint32_t, uint16_t, float, std::string&, float, bool, float, int, bool>())
        .def("init", &APPNPNodeClassifier::init, py::call_guard<py::gil_scoped_release>())
        .def("train", &APPNPNodeClassifier::train, py::call_guard<py::gil_scoped_release>());
    */
    py::class_<EvalValue>(m, "eval_value")
        .def("get_type", &EvalValue::getEvalType)
        .def(py::init<const BigInt&>())
        .def(py::init<const Double&>())
        .def(py::init<const Decimal&>())
        .def(py::init<const Bool&>())
        .def(py::init<const String&>())
        .def(py::init<const Timestamp&>())
        .def(py::init<const IntList&>())
        .def(py::init<const LongList&>())
        .def(py::init<const FloatList&>())
        .def(py::init<const DoubleList&>())
        .def(py::init<const IntVec&>())
        .def(py::init<const LongVec&>())
        .def(py::init<const FloatVec&>())
        .def(py::init<const DoubleVec&>())
        .def("gen_bigint", &EvalValue::gen_bigint)
        .def("gen_double", &EvalValue::gen_double)
        .def("gen_decimal", &EvalValue::gen_decimal)
        .def("gen_bool", &EvalValue::gen_bool)
        .def("gen_string", &EvalValue::gen_string)
        .def("gen_timestamp", &EvalValue::gen_timestamp)
        .def("gen_ilist", &EvalValue::gen_ilist)
        .def("gen_llist", &EvalValue::gen_llist)
        .def("gen_flist", &EvalValue::gen_flist)
        .def("gen_dlist", &EvalValue::gen_dlist)
        .def("gen_ivec", &EvalValue::gen_ivec)
        .def("gen_lvec", &EvalValue::gen_lvec)
        .def("gen_fvec", &EvalValue::gen_fvec)
        .def("gen_dvec", &EvalValue::gen_dvec)
        .def("to_string", &EvalValue::to_string)
        .def("bigint", &EvalValue::getBigInt)
        .def("double", &EvalValue::getDouble)
        .def("decimal", &EvalValue::getDecimal)
        .def("bool", &EvalValue::getBool)
        .def("timestamp", &EvalValue::getTS)
        .def("string", &EvalValue::getString)
        .def("intlist", &EvalValue::getIntList)
        .def("longlist", &EvalValue::getLongList)
        .def("floatlist", &EvalValue::getFloatList)
        .def("doublelist", &EvalValue::getDoubleList)
        .def("intvec", &EvalValue::getIntVec)
        .def("longvec", &EvalValue::getLongVec)
        .def("floatvec", &EvalValue::getFloatVec)
        .def("doublevec", &EvalValue::getDoubleVec);

    py::class_<BigInt>(m, "bigint")
        .def(py::init<int64_t>())
        .def("get", &BigInt::get);

    py::class_<Double>(m, "double")
        .def(py::init<double>())
        .def("get", &Double::get);

    py::class_<Decimal>(m, "decimal")
        .def(py::init<const std::string>())
        .def("get", &Decimal::get);

    py::class_<String>(m, "string")
        .def(py::init<const std::string>())
        .def("get", &String::get);

    py::class_<Bool>(m, "bool")
        .def(py::init<bool>())
        .def("get", &Bool::get);

    py::class_<IntList>(m, "intlist")
        .def(py::init<>())
        .def("at", &IntList::operator[])
        .def("get", [](IntList &il){
            IntVec data;
            il.copyTo(data);
            return data;
        })
        .def("ref", [](IntList &il){
            static IntVec data;
            il.copyTo(data);
            return data.data();
        })
        .def("append", (void (IntList::*)(int64_t)) &IntList::push_back);

    py::class_<LongList>(m, "longlist")
        .def(py::init<>())
        .def("at", &LongList::operator[])
        .def("get", [](LongList &il){
            LongVec data;
            il.copyTo(data);
            return data;
        })
        .def("ref", [](LongList &il){
            static LongVec data;
            il.copyTo(data);
            return data.data();
        })
        .def("append", (void (LongList::*)(int64_t)) &LongList::push_back);

    py::class_<FloatList>(m, "floatlist")
        .def(py::init<>())
        .def("at", &FloatList::operator[])
        .def("size", &FloatList::size)
        .def("get", [](FloatList &dl){
            FloatVec data;
            dl.copyTo(data);
            return data;
        })
        .def("ref", [](FloatList &dl){
            static FloatVec data;
            dl.copyTo(data);
            return data.data();
        })
        .def("append", (void (FloatList::*)(double)) &FloatList::push_back);

    py::class_<DoubleList>(m, "doublelist")
        .def(py::init<>())
        .def("at", &DoubleList::operator[])
        .def("size", &DoubleList::size)
        .def("get", [](DoubleList &dl){
            DoubleVec data;
            dl.copyTo(data);
            return data;
        })
        .def("ref", [](DoubleList &dl){
            static DoubleVec data;
            dl.copyTo(data);
            return data.data();
        })
        .def("append", (void (DoubleList::*)(double)) &DoubleList::push_back);

    py::class_<MeshEvalWrapper, std::shared_ptr<MeshEvalWrapper>>(m, "mesh_eval_wrapper")
        .def(py::init<std::vector<EvalVec>&>());

    py::class_<EvalVecWrapper, std::shared_ptr<EvalVecWrapper>>(m, "eval_vec_wrapper")
        .def(py::init<EvalVec&>());

    py::class_<IntVecWrapper, std::shared_ptr<IntVecWrapper>>(m, "int_vec_wrapper")
        .def(py::init<IntVec&>());
    
    py::class_<LongVecWrapper, std::shared_ptr<LongVecWrapper>>(m, "long_vec_wrapper")
        .def(py::init<LongVec&>());

    py::class_<FloatVecWrapper, std::shared_ptr<FloatVecWrapper>>(m, "float_vec_wrapper")
        .def(py::init<FloatVec&>());

    py::class_<DoubleVecWrapper, std::shared_ptr<DoubleVecWrapper>>(m, "double_vec_wrapper")
        .def(py::init<DoubleVec&>());


    py::class_<PyDataloader>(m, "py_dataloader")
        .def(py::init<>())
        .def("add_col", (void (PyDataloader::*)(LongVec&)) &PyDataloader::addCol)
        .def("add_col_np", [](PyDataloader* loader, py::array_t<int> array){
            size_t l = array.size();
            int* ptr = const_cast<int*>(array.data());
            EvalVec vec;
            vec.reserve(l);
            size_t i = 0;
            while(i < l) {
                vec.push_back(EvalValue(BigInt(*ptr)));
                ++ptr;
                ++i;
            }
            loader->addCol(vec);
        })
        .def("add_col", (void (PyDataloader::*)(DoubleVec&)) &PyDataloader::addCol)
        .def("add_col_np", [](PyDataloader* loader, py::array_t<double> array){
            size_t l = array.size();
            double* ptr = const_cast<double*> (array.data());
            EvalVec vec;
            vec.reserve(l);
            size_t i = 0;
            while(i < l) {
                vec.push_back(EvalValue(Double(*ptr)));
                ++ptr;
                ++i;
            }
            loader->addCol(vec);
        })
        .def("add_doublevec_col", (void (PyDataloader::*)(std::vector<DoubleVec>&)) &PyDataloader::addCol)
        .def("add_doublevec_col_np", [](PyDataloader* loader, py::array_t<double> array){
            py::buffer_info buf = array.request();
            int row = buf.shape[0];
            int col = buf.shape[1];
            double* ptr = (double*) buf.ptr;
            EvalVec vec;
            vec.reserve(row);
            for(int i = 0; i < row; i++) {
                int st = i * col;
                int ed = st + col;
                DoubleVec cvec(ptr + st, ptr + ed);
                vec.push_back(EvalValue(cvec));
            }
            loader->addCol(vec);            
        })
        .def("add_floatvec_col", (void (PyDataloader::*)(std::vector<FloatVec>&)) &PyDataloader::addCol)
        .def("add_floatvec_col_np", [](PyDataloader* loader, py::array_t<float> array){
            py::buffer_info buf = array.request();
            int row = buf.shape[0];
            int col = buf.shape[1];
            float* ptr = (float*) buf.ptr;
            EvalVec vec;
            vec.reserve(row);
            for(int i = 0; i < row; i++) {
                int st = i * col;
                int ed = st + col;
                FloatVec cvec(ptr + st, ptr + ed);
                vec.push_back(EvalValue(cvec));
            }
            loader->addCol(vec);            
        })
        .def("add_bool_col", &PyDataloader::addBoolCol)
        .def("add_bool_col_np", [](PyDataloader* loader, py::array_t<bool> array){
            size_t l = array.size();
            bool* ptr = const_cast<bool*> (array.data());
            size_t i = 0;
            EvalVec vec;
            vec.reserve(l);
            while(i < l) {
                vec.push_back(EvalValue(Bool(*ptr)));
                ++ptr;
                ++i;
            }
            loader->addCol(vec);
        })
        .def("build_row_content", &PyDataloader::buildRowContent)
        .def("get_row_content", &PyDataloader::getRowContent, py::return_value_policy::reference)
        .def("get_col", &PyDataloader::getCol, py::return_value_policy::reference);
    

    py::class_<ArrayUtil>(m, "array_util")
        .def(py::init<>())
        .def("to_intlist", [](ArrayUtil* c, py::array_t<int> array){
            c->convert(array);
            return c->getIntVec();
        }, py::return_value_policy::reference)
        .def("to_longlist", [](ArrayUtil* c, py::array_t<int64_t> array){
            c->convert(array);
            return c->getLongVec();
        }, py::return_value_policy::reference)
        .def("to_evallist", [](ArrayUtil* c, py::array_t<int> array){
            c->convertEval(array);
            return c->getEvalVec();
        }, py::return_value_policy::reference)
        .def("ones", (std::shared_ptr<IntVecWrapper> (ArrayUtil::*)(size_t)) &ArrayUtil::ones, py::return_value_policy::reference)
        .def("zeros", (std::shared_ptr<IntVecWrapper> (ArrayUtil::*)(size_t)) &ArrayUtil::zeros, py::return_value_policy::reference);

}
