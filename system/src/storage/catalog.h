// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef STORAGE_CATALOG_H_
#define STORAGE_CATALOG_H_

// Project include
#include "storage/base/paged_array.h"
#include "common/types/types.h"

// Other include
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>

namespace storage {

/**
 * @brief field information of relational table
 * @author Ilju Lee, ijlee@kdb.snu.ac.kr
 */
struct CreateFieldInfo {
    CreateFieldInfo() {}

    /**
     * @brief CreateFieldInfo constructor
     * @param std::string field_name: name of field
     * @param ValueType value_type: value type
     */
    CreateFieldInfo(std::string field_name, ValueType value_type):
        field_name_(field_name), value_type_(value_type),
        size_(valueSizeOf(value_type)) {}

    /**
     * @brief CreateFieldInfo constructor for value type CHAR
     * @param std::string field_name: name of field
     * @param ValueType value_type: value type
     * @param uint32_t size: size of value
     */
    CreateFieldInfo(std::string field_name, ValueType value_type, uint32_t size):
        field_name_(field_name), value_type_(value_type),
        size_(size + 2) /* to store size(1B) */ {
        assert(value_type == VT_CHAR);
    }

    std::string field_name_;
    ValueType   value_type_;
    int32_t     size_;

  private:
    friend class boost::serialization::access;
    template <class Archive>
    void serialize(Archive & ar, const unsigned int version) {
        ar & field_name_;
        ar & value_type_;
        ar & size_;
    }
};

typedef std::vector<CreateFieldInfo> CreateFieldVec;

/**
 * @brief metadata that manages data about data stores in RTI
 * @author Ilju Lee, ijlee@kdb.snu.ac.kr
 */
class Metadata {
  public:
    /**
     * @breif init metadata
     */
    static void initMetadata()
        { Metadata::getMetadata(); }

    /**
     * @brief add new table to metadta
     * @param Table* table: table to add
     * @return id of table
     */
    static uint32_t addNewTable(Table* table)
        { return getMetadata()->addNewTable_(table); }

    /**
     * @brief add new table to metadta
     * @detail should be called when recovery
     * @param Table* table: table to add
     * @param uint32_t table_id: id of table
     * @return id of table
     */
    static uint32_t addNewTable(Table* table, uint32_t table_id)
        { return getMetadata()->addNewTable_(table, table_id); }

    /**
     * @brief add new paged_array to metadta
     * @param PABase* pa: paged_array to add
     * @return id of page_array
     */
    static uint32_t addNewPagedArray(PABase* pa)
        { return getMetadata()->addNewPagedArray_(pa); }

    /**
     * @brief add new paged_array to metadta
     * @detail should be called when recovery
     * @param PABase* pa: paged_array to add
     * @param LogicalPtr idx: id of paged_array
     * @return id of paged_array
     */
    static uint32_t addNewPagedArray(PABase* pa, LogicalPtr idx)
        { return getMetadata()->addNewPagedArray_(pa, idx); }

    /**
     * @brief add new graph to metadta
     * @param Graph graph: graph to add
     * @return id of graph
     */
    static uint32_t addNewGraph(Graph* graph)
        { return getMetadata()->addNewGraph_(graph); }

    /**
     * @brief add new graph to metadta
     * @detail should be called when recovery
     * @param Graph* graph: graph to add
     * @param uint32_t graph_id: id of graph
     */
    static uint32_t addNewGraph(Graph* graph, uint32_t graph_id)
        { return getMetadata()->addNewGraph_(graph, graph_id); }

    /**
     * @brief remove a paged_array from metadata
     * @param uint32_t pa_id: id of paged_array
     */
    static void removePAInfo(uint32_t pa_id)
        { getMetadata()->removePAInfo_(pa_id); }

    /**
     * @brief remove multiple paged_array from metadata
     * @param std::vector<uint32_t> pa_ids: ids of multiple paged_array
     */
    static void removePAInfo(std::vector<uint32_t> pa_ids)
        { getMetadata()->removePAInfo_(pa_ids); }

    /**
     * @brief remove a table from metadata
     * @param std::string table_name: name of table
     */
    static void releaseTable(std::string table_name)
        { getMetadata()->releaseTable_(table_name); }

    /**
     * @brief remove a table from metadata
     * @param uint32_t table_id: id of table
     */
    static void releaseTableById(uint32_t table_id)
        { getMetadata()->releaseTableById_(table_id); }

    /**
     * @brief remove a graph from metadata
     * @param std::string graph_name: name of graph
     */
    static void releaseGraph(std::string graph_name)
        { getMetadata()->releaseGraph_(graph_name); }

    /**
     * @brief remove a graph from metadata
     * @param uint32_t graph_id: id of graph
     */
    static void releaseGraphById(uint32_t graph_id)
        { getMetadata()->releaseGraphById_(graph_id); }

    /**
     * @brief get metadata singleton
     * @return metadata ref
     */
    static Metadata* getMetadata() {
        if (!Metadata::metadata_)
            metadata_ = new Metadata();
        return metadata_;
    }

    /**
     * @brief sync singleton metadata
     * @param unsigned long ref: metadata ref
     */
    static void syncMetadata(unsigned long ref) {
        Metadata::metadata_ = reinterpret_cast<Metadata*>(ref);
    }

    /**
     * @brief get table from metadata
     * @param uint32_t table_id: id of table
     * @return table
     */
    static Table* getTable(uint32_t table_id)
        { return getMetadata()->getTable_(table_id); }

    /**
     * @brief get table from metadata
     * @param std::string name: name of table
     * @return table
     */
    static Table* getTableFromName(const std::string& name)
        { return getMetadata()->getTableFromName_(name); }

    /**
     * @brief get table type as string
     * @param std::string name: name of table
     * @return type of table
     */
    static std::string getTableType(const std::string& name)
        { return getMetadata()->getTableType_(name); }

    /**
     * @brief reset table name
     * @param std::string old, old name of table
     * @param std::string new_, new name of table
     */
    static void resetTableName(std::string old, std::string new_)
        { return getMetadata()->resetTableName_(old, new_); }

    /**
     * @brief get row table from metadata
     * @param std::string name: name of table
     * @return row table
     */
    static RowTable* getRowTable(const std::string& name)
        { return getMetadata()->getRowTable_(name); }

    /**
     * @brief get column table from metadata
     * @param std::string name: name of table
     * @return column table
     */
    static ColumnTable* getColTable(const std::string& name)
        { return getMetadata()->getColTable_(name); }

    /**
     * @brief get all tables
     * @param std::vector<Table*> tables
     */
    static void getTableList(
            std::vector<Table*>& tables, bool include_metadata=false)
        { return getMetadata()->getTableList_(tables, include_metadata); }

    /**
     * @brief check if the table exists
     * @param std::string tname: name of table
     */
    static bool tableExists(std::string& tname)
        { return getMetadata()->tableExists_(tname); }

    /**
     * @brief get paged_array from metadata
     * @param uint32_t pa_id: id of paged_array
     * @return paged_array
     */
    static PABase* getPagedArray(uint32_t pa_id)
        { return getMetadata()->getPagedArray_(pa_id); }

    /**
     * @brief get graph from metadata
     * @param uint32_t graph_id: id of graph
     * @return graph
     */
    static Graph* getGraph(uint32_t graph_id)
        { return getMetadata()->getGraph_(graph_id); }

    /**
     * @brief get graph from metadata
     * @param std::string name: name of graph
     * @return graph
     */
    static Graph* getGraphFromName(const std::string& name)
        { return getMetadata()->getGraphFromName_(name); }

    /**
     * @brief reset graph name
     * @param std::string old, old name of graph
     * @param std::string new_, new name of graph
     */
    static void resetGraphName(std::string old, std::string new_)
        { return getMetadata()->resetGraphName_(old, new_); }

    /**
     * @brief get all graphs
     * @param std::vector<Graph*> graphs
     */
    static void getGraphList(
            std::vector<Graph*>& graphs, bool include_metadata=false)
        { return getMetadata()->getGraphList_(graphs, include_metadata); }

    /**
     * @brief check if the graph exists
     * @param std::string gname: name of graph
     */
    static bool graphExists(std::string& gname)
        { return getMetadata()->graphExists_(gname); }

    /**
     * @brief get memory usage of catalog
     * @return byte size of memory
     */
    static size_t getCatalogSize()
        { return getMetadata()->getCatalogSize_(); }

    /**
     * @brief get memory usage
     */
    static bool memoryStat(std::string path,
            bool verbose, int level, std::string addr)
        { return getMetadata()->memoryStat_(path, verbose, level, addr); }

  private:
    DISALLOW_COPY_AND_ASSIGN(Metadata);

    /**
     * @brief metadata constructor
     */
    Metadata();

    /**
     * @brief add new table to metadta
     * @param Table* table: table to add
     * @return id of table
     */
    uint32_t addNewTable_(Table* table);

    /**
     * @brief add new table to metadta
     * @detail should be called when recovery
     * @param Table* table: table to add
     * @param uint32_t table_id: id of table
     * @return id of table
     */
    uint32_t addNewTable_(Table* table, uint32_t table_id);

    /**
     * @brief add new paged_array to metadta
     * @param PABase* pa: paged_array to add
     * @return id of page_array
     */
    uint32_t addNewPagedArray_(PABase* pa);

    /**
     * @brief add new paged_array to metadta
     * @detail should be called when recovery
     * @param PABase* pa: paged_array to add
     * @param LogicalPtr idx: id of paged_array
     * @return id of paged_array
     */
    uint32_t addNewPagedArray_(PABase* pa, LogicalPtr idx);

    /**
     * @brief add new graph to metadta
     * @param Graph graph: graph to add
     * @return id of graph
     */
    uint32_t addNewGraph_(Graph* graph);

    /**
     * @brief add new graph to metadta
     * @detail should be called when recovery
     * @param Graph* graph: graph to add
     * @param uint32_t graph_id: id of graph
     */
    uint32_t addNewGraph_(Graph* graph, uint32_t graph_id);

    /**
     * @brief remove a paged_array from metadata
     * @param uint32_t pa_id: id of paged_array
     */
    void removePAInfo_(uint32_t pa_id);

    /**
     * @brief remove multiple paged_array from metadata
     * @param std::vector<uint32_t> pa_ids: ids of multiple paged_array
     */
    void removePAInfo_(std::vector<uint32_t> pa_ids);

    /**
     * @brief remove a table from metadata
     * @param std::string table_name: name of table
     */
    void releaseTable_(std::string table_name);

    /**
     * @brief remove a table from metadata
     * @param uint32_t table_id: id of table
     */
    void releaseTableById_(uint32_t table_id);

    /**
     * @brief remove a graph from metadata
     * @param std::string graph_name: name of graph
     */
    void releaseGraph_(std::string graph_name);

    /**
     * @brief remove a graph from metadata
     * @param uint32_t graph_id: id of graph
     */
    void releaseGraphById_(uint32_t graph_id);

    /**
     * @brief get table from metadata
     * @param uint32_t table_id: id of table
     * @return table
     */
    Table* getTable_(uint32_t table_id);

    /**
     * @brief get table from metadata
     * @param std::string name: name of table
     * @return table
     */
    Table* getTableFromName_(const std::string& name);

    /**
     * @brief get table type as string
     * @param std::string name: name of table
     * @return type of table
     */
    std::string getTableType_(const std::string& name);

    /**
     * @brief reset table name
     * @param std::string old, old name of table
     * @param std::string new_, new name of table
     */
    void resetTableName_(std::string old, std::string new_);

    /**
     * @brief get row table from metadata
     * @param std::string name: name of table
     * @return row table
     */
    RowTable* getRowTable_(const std::string& name);

    /**
     * @brief get column table from metadata
     * @param std::string name: name of table
     * @return col table
     */
    ColumnTable* getColTable_(const std::string& name);

    /**
     * @brief get all tables
     * @param std::vector<Table*> tables: return tables
     */
    void getTableList_(
            std::vector<Table*>& tables, bool include_metadata);

    /**
     * @brief check if the table exists
     * @param std::string tname: name of table
     */
    bool tableExists_(std::string& tname);

    /**
     * @brief get paged_array from metadata
     * @param uint32_t pa_id: id of paged_array
     * @return paged_array
     */
    PABase* getPagedArray_(uint32_t pa_id);

    /**
     * @brief get graph from metadata
     * @param uint32_t graph_id: id of graph
     * @return graph
     */
    Graph* getGraph_(uint32_t graph_id);

    /**
     * @brief get graph from metadata
     * @param std::string name: name of graph
     * @return graph
     */
    Graph* getGraphFromName_(const std::string& name);

    /**
     * @brief reset graph name
     * @param std::string old, old name of graph
     * @param std::string new_, new name of graph
     */
    void resetGraphName_(std::string old, std::string new_);

    /**
     * @brief get all graphs
     * @param std::vector<Graph*> graphs
     */
    void getGraphList_(
            std::vector<Graph*>& graphs, bool include_metadata);

    /**
     * @brief check if the graph exists
     * @param std::string gname: name of graph
     */
    bool graphExists_(std::string& gname);

    /**
     * @brief get memory usage of catalog
     * @return byte size of memory
     */
    size_t getCatalogSize_();

    /**
     * @brief get memory usage
     */
    bool memoryStat_(std::string path,
            bool verbose, int level, std::string addr);

    /*
     * member variables
     */
    PagedArray<PAInfo_T>    sys_paged_array_;
    PagedArray<TableInfo_T> sys_tables_;
    PagedArray<GraphInfo_T> sys_graphs_;

    static Metadata* metadata_;
};

}  // namespace storage

inline int64_t getHashVal(int64_t id, uint32_t hash_divisor_) {
    int64_t hash_val = id % hash_divisor_;
    if (hash_val < 0) hash_val += hash_divisor_;

    return hash_val;
}

#endif  // STORAGE_CATALOG_H_
