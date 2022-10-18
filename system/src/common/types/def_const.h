// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef COMMON_TYPES_DEF_CONST_H_
#define COMMON_TYPES_DEF_CONST_H_

// Projec include
#include "common/def_const.h"

// ValueType & its utilility functions
enum ValueType {
    VT_BIGINT,
    VT_INT,
    VT_DOUBLE,
    VT_FLOAT,
    VT_DECIMAL,
    VT_VARCHAR,
    VT_CHAR,
    VT_BOOL,
    VT_DATE,
    VT_TIME,
    VT_TIMESTAMP,
    VT_INTLIST,
    VT_LONGLIST,
    VT_FLOATLIST,
    VT_DOUBLELIST,
    VT_UDT,
    NUM_VT
};

std::string getNameOfValueType(ValueType type);
ValueType getValueType(std::string s);

static const uint16_t MAX_EMBEDDED_SIZE = 13;

typedef ValueType VT;

#endif  // COMMON_TYPES_DEF_CONST_H_
