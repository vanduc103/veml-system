// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

// Related include
#include "storage/catalog.h"

// Project include
#include "storage/relation/table.h"
#include "storage/relation/row_table.h"
#include "storage/relation/column_table.h"
#include "storage/graph/graph.h"

namespace storage {

Metadata* Metadata::metadata_ = nullptr;

Metadata::Metadata()
    : sys_paged_array_(true, sizeof(PAInfo_T)),
    sys_tables_(true, sizeof(TableInfo_T)),
    sys_graphs_(true, sizeof(GraphInfo_T)) {}

/*
 * add metadata
 */
uint32_t Metadata::addNewTable_(Table* table) {
    TableInfo ti(table->getTableName().c_str(), table);
    LogicalPtr table_id = sys_tables_.insert(ti).lptr_;
    assert(table_id <= std::numeric_limits<uint32_t>::max());

    return table_id;
}

uint32_t Metadata::addNewTable_(Table* table, uint32_t idx) {
    if (sys_tables_.isValid(idx)) {
        std::string err("invalid index @Metadata::addNewTable");
        RTI_EXCEPTION(err);
    }

    TableInfo ti(table->getTableName().c_str(), table);
    LogicalPtr table_id = sys_tables_.insert(ti, idx).lptr_;
    assert(table_id == idx);
    assert(table_id <= std::numeric_limits<uint32_t>::max());

    return table_id;
}

uint32_t Metadata::addNewPagedArray_(PABase* pa) {
    PAInfo pi(pa);
    LogicalPtr pa_id = sys_paged_array_.insert(pi).lptr_;
    assert(pa_id <= std::numeric_limits<uint32_t>::max());

    return pa_id;
}

uint32_t Metadata::addNewPagedArray_(PABase* pa, LogicalPtr idx) {
    if (sys_paged_array_.isValid(idx)) {
        std::string err("invalid index @Metadata::addNewPagedArray");
        RTI_EXCEPTION(err);
    }

    PAInfo pi(pa);
    LogicalPtr pa_id = sys_paged_array_.insert(pi, idx).lptr_;
    assert(pa_id == idx);
    assert(pa_id <= std::numeric_limits<uint32_t>::max());

    return pa_id;
}

uint32_t Metadata::addNewGraph_(Graph* graph) {
    GraphInfo gi(graph->getName().c_str(), graph);
    LogicalPtr graph_id = sys_graphs_.insert(gi).lptr_;
    assert(graph_id <= std::numeric_limits<uint32_t>::max());

    return graph_id;
}

uint32_t Metadata::addNewGraph_(Graph* graph, uint32_t idx) {
    if (sys_graphs_.isValid(idx)) {
        std::string err("invalid index @Metadata::addNewGraph");
        RTI_EXCEPTION(err);
    }

    GraphInfo gi(graph->getName().c_str(), graph);
    LogicalPtr graph_id = sys_graphs_.insert(gi, idx).lptr_;
    assert(graph_id == idx);
    assert(graph_id <= std::numeric_limits<uint32_t>::max());

    return graph_id;
}

/*
 * remove metadata
 */
void Metadata::removePAInfo_(uint32_t pa_id) {
    sys_paged_array_.erase(pa_id);
}

void Metadata::removePAInfo_(std::vector<uint32_t> pa_ids) {
    for(auto it = pa_ids.begin(); it != pa_ids.end() ; it++){
        sys_paged_array_.erase(*it);
    }
}

void Metadata::releaseTableById_(uint32_t table_id) {
    Table* table = getTable_(table_id);
    if (table != nullptr) {
        sys_tables_.erase(table_id);
        delete table;
    }
}

void Metadata::releaseTable_(std::string table_name) {
    Table* table = getTableFromName_(table_name);
    if (table != nullptr) {
        uint32_t table_id = table->getTableId();
        releaseTableById_(table_id);
    }
}

void Metadata::releaseGraph_(std::string graph_name) {
    Graph* graph = getGraphFromName_(graph_name);
    if (graph != nullptr) {
        uint32_t graph_id = graph->getId();
        releaseGraphById_(graph_id);
    }
}

void Metadata::releaseGraphById_(uint32_t graph_id) {
    Graph* graph = getGraph_(graph_id);
    if (graph != nullptr) {
        sys_graphs_.erase(graph_id);
        delete graph;
    }
}

/*
 * get metadata
 */
Table* Metadata::getTable_(uint32_t table_id) {
    LogicalPtr lptr = table_id;
    TableInfo& ti = sys_tables_[lptr];
    return ti.getPtr();
}

Table* Metadata::getTableFromName_(const std::string& name) {
    auto itr = sys_tables_.begin();
    for ( ; itr; ++itr) {
        if (itr.isValid()) {
            TableInfo* ti = reinterpret_cast<TableInfo*>(itr.getPptr());
            if (strcmp(name.c_str(), ti->getTableName()) == 0) {
                return ti->getPtr();
            }
        }
    }

    return nullptr;
}

std::string Metadata::getTableType_(const std::string& name) {
    Table* t = getTableFromName_(name);
    if (t == nullptr) {
        std::string err("wrong table name @Metadata::getRowTable");
        RTI_EXCEPTION(err);
        return nullptr;
    }

    if (t->getTableType() == Table::ROW) {
        return "ROW";
    } else if (t->getTableType() == Table::COLUMN) {
        return "COLUMN";
    }
    return "";
}

void Metadata::resetTableName_(std::string old, std::string new_) {
    auto itr = sys_tables_.begin();
    for ( ; itr; ++itr) {
        if (itr.isValid()) {
            TableInfo* ti = reinterpret_cast<TableInfo*>(itr.getPptr());
            if (ti->getTableName() == old) {
                ti->setTableName(new_.c_str());
                ti->getPtr()->setTableName(new_);
            }
        }
    }
}

RowTable* Metadata::getRowTable_(const std::string& name) {
    Table* t = getTableFromName_(name);
    if (t == nullptr) {
        std::string err("wrong table name @Metadata::getRowTable");
        RTI_EXCEPTION(err);
        return nullptr;
    }

    if (t->getTableType() == Table::ROW) {
        return reinterpret_cast<RowTable*>(t);
    }

    std::string err("wrong table type @Metadata::getRowTable");
    RTI_EXCEPTION(err);
    return nullptr;
}

ColumnTable* Metadata::getColTable_(const std::string& name) {
    Table* t = getTableFromName_(name);
    if (t == nullptr) {
        std::string err("wrong table name @Metadata::getColTable");
        RTI_EXCEPTION(err);
        return nullptr;
    }

    if (t->getTableType() == Table::COLUMN) {
        return reinterpret_cast<ColumnTable*>(t);
    }

    std::string err("wrong table type @Metadata::getColTable");
    RTI_EXCEPTION(err);
    return nullptr;
}

void Metadata::getTableList_(
        std::vector<Table*>& tables, bool include_metadata) {
    auto itr = sys_tables_.begin();
    for ( ; itr; ++itr) {
        if (itr.isValid()) {
            TableInfo* ti = reinterpret_cast<TableInfo*>(itr.getPptr());
            std::string sys_prefix(ti->getTableName(), ti->getTableName()+5);
            if (sys_prefix.compare("$SYS$") != 0)
                tables.push_back(ti->getPtr());
        }
    }
}

bool Metadata::tableExists_(std::string& tname) {
    return getTableFromName_(tname) != nullptr;
}

PABase* Metadata::getPagedArray_(uint32_t pa_id) {
    PAInfo& pi = sys_paged_array_[pa_id];
    return pi.getPtr();
}

Graph* Metadata::getGraph_(uint32_t graph_id) {
    GraphInfo& gi = sys_graphs_[graph_id];
    return gi.getPtr();
}

Graph* Metadata::getGraphFromName_(const std::string& name) {
    auto itr = sys_graphs_.begin();
    for ( ; itr; ++itr) {
        if (itr.isValid()) {
            GraphInfo* gi = reinterpret_cast<GraphInfo*>(itr.getPptr());
            if (strcmp(name.c_str(), gi->getGraphName()) == 0) {
                return gi->getPtr();
            }
        }
    }

    return nullptr;
}

void Metadata::resetGraphName_(std::string old, std::string new_) {
    auto itr = sys_graphs_.begin();
    for ( ; itr; ++itr) {
        if (itr.isValid()) {
            GraphInfo* gi = reinterpret_cast<GraphInfo*>(itr.getPptr());
            if (gi->getGraphName() == old) {
                gi->setGraphName(new_.c_str());
                gi->getPtr()->setName(new_);
            }
        }
    }
}

void Metadata::getGraphList_(
        std::vector<Graph*>& graphs, bool include_metadata) {
    auto itr = sys_graphs_.begin();
    for ( ; itr; ++itr) {
        if (itr.isValid()) {
            GraphInfo* gi = reinterpret_cast<GraphInfo*>(itr.getPptr());
            std::string sys_prefix(gi->getGraphName(), gi->getGraphName()+5);
            if (sys_prefix.compare("$SYS$") != 0)
                graphs.push_back(gi->getPtr());
        }
    }
}

bool Metadata::graphExists_(std::string& gname) {
    return getGraphFromName_(gname) != nullptr;
}

size_t Metadata::getCatalogSize_() {
    return sys_paged_array_.getMemoryUsage() + sys_tables_.getMemoryUsage()
        + sys_graphs_.getMemoryUsage();
}

bool Metadata::memoryStat_(std::string path,
        bool verbose, int level, std::string addr) {
    bool fstream = false;
    std::ostringstream ss;
    std::ofstream fs;
    if (path.size() != 0) {
        fs.open(path, std::fstream::app);
        if (!fs.good()) return false;
        fstream = true;
    }

    size_t total = 0;

    // get metadata memory usage;
    size_t metadata_usage = sys_paged_array_.getMemoryUsage()
                            + sys_tables_.getMemoryUsage()
                            + sys_graphs_.getMemoryUsage();
    total += metadata_usage;

    // get table memory usage
    std::vector<Table*> tables;
    getTableList_(tables, false);
    for (auto t : tables) {
        if (t->getTableName().find("$DELTA$") != std::string::npos) continue;
        if (t->getTableName().find("$VP$") != std::string::npos) continue;
        if (t->getTableName().find("$EP$") != std::string::npos) continue;

        if (fstream)
            total += t->getMemoryUsage(fs, verbose, level);
        else
            total += t->getMemoryUsage(ss, verbose, level);
    }
    
    // get graph memory usage
    std::vector<Graph*> graphs;
    getGraphList(graphs, false);
    for (auto g : graphs) {
        if (fstream)
            total += g->getMemoryUsage(fs, verbose, level);
        else
            total += g->getMemoryUsage(ss, verbose, level);
    }

    if (!fstream) {
        std::cout << ss.str() << std::flush;
    }

    if (!fstream) {
        std::cout << "Metadata => " << memory_unit_str(metadata_usage) << '\n';
        std::cout << '\n';
        std::cout << "RTI(" << addr << ") Total Memory Usage: "
            << memory_unit_str(total) << '\n';
        std::cout << "---------------------------------------------" << "\n\n";
    } else {
        fs << "Metadata => " << memory_unit_str(metadata_usage) << '\n';
        fs << '\n';
        fs << "RTI(" << addr << ") Total Memory Usage: "
            << memory_unit_str(total) << '\n';
        fs << "---------------------------------------------" << "\n\n";
    }

    return true;
}

}   // namespace storage
