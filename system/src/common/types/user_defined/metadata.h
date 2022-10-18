// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef COMMON_TYPES_USER_DEFINED_METADATA_H_
#define COMMON_TYPES_USER_DEFINED_METADATA_H_

// Project include
#include "common/def_const.h"
#include "common/types/def_const.h"

// C & C++ system inclue
#include <cstring>
#include <cassert>

// Other include
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>

class EvalValue;

namespace storage {
    class Table;
    class BTreeIndex;
    class paged_array_base;
    class Graph;
}

namespace boost {
namespace serialization {
    class access;
}
}

using namespace storage;

#pragma pack(push, 1)
class TableInfo {
  public:
    TableInfo(const char* table_name, Table* table) {
        assert(strlen(table_name) <= 254);
        strcpy(table_name_, table_name);
        table_ = table;
    }

    TableInfo(const TableInfo& other) {
        (*this) = other;
    }

    TableInfo& operator=(const TableInfo& other) {
        strcpy(table_name_, other.table_name_);
        table_ = other.table_;
        return *this;
    }

    const char* getTableName() const { return table_name_; }
    void setTableName(const char* new_name) {
        memset(table_name_, 0, 255);
        strcpy(table_name_, new_name);
    }
    Table*      getPtr() const       { return table_; }

    // for paged_array interface
    uint32_t    hash(uint32_t seed) { return 0; }
    TableInfo   get() const         { return *this; }
    TableInfo   getValue() const    { return *this; }
    PhysicalPtr getEval()           { return (PhysicalPtr)this; }

  private:
    char        dummy1;
    char        table_name_[255];
    Table*      table_;
};

class IndexInfo {
  public:
    IndexInfo(const char* index_name, BTreeIndex* index) {
        assert(strlen(index_name) <= 254);
        strcpy(index_name_, index_name);
        index_ = index;
    }

    IndexInfo(const IndexInfo& other) {
        (*this) = other;
    }

    IndexInfo& operator=(const IndexInfo& other) {
        strcpy(index_name_, other.index_name_);
        index_ = other.index_;
        return *this;
    }

    const char* getIndexName() const { return index_name_; }
    BTreeIndex* getPtr() const       { return index_; }

    // for paged_array interface
    uint32_t    hash(uint32_t seed) { return 0; }
    IndexInfo   get() const         { return *this; }
    IndexInfo   getValue() const    { return *this; }
    PhysicalPtr getEval()           { return (PhysicalPtr)this; }

  private:
    char        dummy1;
    char        index_name_[255];
    BTreeIndex* index_;
};

class FieldInfo {
 public:
    FieldInfo() { }
    FieldInfo(const char* table_name, const char* field_name,
            int32_t field_id, ValueType value_type,
            const char* value_type_name, int32_t size, int32_t offset)
        : field_id(field_id), value_type(value_type), size(size), offset(offset) {
        assert(strlen(table_name) <= 254);
        assert(strlen(field_name) <= 254);
        assert(strlen(value_type_name) <= 10);
        strcpy(this->table_name, table_name);
        strcpy(this->field_name, field_name);
        strcpy(this->value_type_name, value_type_name);
    }

    FieldInfo(const FieldInfo& other) {
        (*this) = other;
    }

    FieldInfo& operator=(const FieldInfo& other) {
        strcpy(table_name, other.table_name);
        dummy2 = other.dummy2;
        strcpy(field_name, other.field_name);
        field_id = other.field_id;
        value_type = other.value_type;
        dummy3 = other.dummy3;
        strcpy(value_type_name, other.value_type_name);
        size = other.size;
        offset = other.offset;
        return *this;
    }

    const char* getTableName() const        { return table_name; }
    const char* getFieldName() const        { return field_name; }
    int32_t     getFieldId() const          { return field_id; }
    ValueType   getValueType() const        { return value_type; }
    const char* getValueTypeName() const    { return value_type_name; }
    int32_t     getSize() const             { return size; }
    int32_t     getOffset() const           { return offset; }

    // for paged_array interface
    uint32_t    hash(uint32_t seed)         { return 0; }
    FieldInfo   get() const                 { return *this; }
    FieldInfo   getValue() const            { return *this; }
    PhysicalPtr getEval()                   { return (PhysicalPtr)this; }

 private:
    /*
     * serialize
     */
    friend class boost::serialization::access;
    template <class Archive>
    void serialize(Archive & ar, const unsigned int version){
        ar & table_name;
        ar & dummy2;
        ar & field_name;
        ar & field_id;
        ar & value_type;
        ar & dummy3;
        ar & value_type_name;
        ar & size;
        ar & offset;
    }

    /*
     * member variables
     */
    char        table_name[255];
    char        dummy2;
    char        field_name[255];
    int32_t     field_id;
    ValueType   value_type;
    char        dummy3;
    char        value_type_name[11];
    int32_t     size;
    int32_t     offset;
};

typedef std::vector<FieldInfo*> FieldInfoVec;

class PAInfo {
  public:
    PAInfo(paged_array_base* pa) : pa_(pa) {}
    paged_array_base*   getPtr() const      { return  pa_; }

    // for paged_array interface
    uint32_t    hash(uint32_t seed)         { return 0; }
    PAInfo      get() const                 { return *this; }
    PAInfo      getValue() const            { return *this; }
    PhysicalPtr getEval()                   { return (PhysicalPtr)this; }

  private:
    paged_array_base*   pa_;
};

class GraphInfo {
  public:
    GraphInfo(const char* graph_name, Graph* graph) {
        assert(strlen(graph_name) <= 254);
        strcpy(graph_name_, graph_name);
        graph_ = graph;
    }

    GraphInfo(const GraphInfo& other) {
        (*this) = other;
    }

    GraphInfo& operator=(const GraphInfo& other) {
        strcpy(graph_name_, other.graph_name_);
        graph_ = other.graph_;
        return *this;
    }

    const char*  getGraphName() const       { return graph_name_; }
    void setGraphName(const char* new_name) {
        memset(graph_name_, 0, 255);
        strcpy(graph_name_, new_name);
    }
    Graph*       getPtr() const             { return graph_; }

    // for paged_array interface
    uint32_t    hash(uint32_t seed)         { return 0; }
    GraphInfo   get() const                 { return *this; }
    GraphInfo   getValue() const            { return *this; }
    PhysicalPtr getEval()                   { return (PhysicalPtr)this; }

  private:
    char        dummy;
    char        graph_name_[255];
    Graph*      graph_;
};
#pragma pack(pop)

#endif  // COMMON_TYPES_USER_DEFINED_METADATA_H_
