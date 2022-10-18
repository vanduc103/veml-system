// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef COMMON_TYPES_TYPES_H_
#define COMMON_TYPES_TYPES_H_

// Project include
#include "common/types/element_type.h"
#include "common/types/basic/types.h"
#include "common/types/user_defined/types.h"

/*
 * define of basic types
 */
typedef FixedSizeElement<BigInt, BigInt> BigInt_T;
static_assert(sizeof(BigInt_T) == 8, "");

typedef FixedSizeElement<Integer, BigInt> Integer_T;
static_assert(sizeof(Integer_T) == 4, "");

typedef FixedSizeElement<Double, Double> Double_T;
static_assert(sizeof(Double_T) == 8, "");

typedef FixedSizeElement<Float, Double> Float_T;
static_assert(sizeof(Float_T) == 4, "");

typedef FixedSizeElement<Decimal, Decimal> Decimal_T;
static_assert(sizeof(Decimal_T) == 8, "");

typedef VarSizeElement<String> Varchar_T;
static_assert(sizeof(Varchar_T) == 16, "");

typedef EmbeddedElement<String> Char_T;

typedef FixedSizeElement<Bool, Bool> Bool_T;
static_assert(sizeof(Bool_T) == 1, "");

typedef FixedSizeElement<Date, Date> Date_T;
static_assert(sizeof(Date_T) == 4, "");

typedef FixedSizeElement<Time, Time> Time_T;
static_assert(sizeof(Time_T) == 4, "");

typedef FixedSizeElement<Timestamp, Timestamp> Timestamp_T;
static_assert(sizeof(Timestamp_T) == 8, "");

/*
 * define of user defined types
 */
typedef FixedSizeElement<Tuple>      Tuple_T;
typedef FixedSizeElement<IntList>    IntList_T;
typedef FixedSizeElement<LongList>   LongList_T;
typedef FixedSizeElement<FloatList>  FloatList_T;
typedef FixedSizeElement<DoubleList> DoubleList_T;

// metadata info
typedef FixedSizeElement<PAInfo>    PAInfo_T;
typedef FixedSizeElement<TableInfo> TableInfo_T;
typedef FixedSizeElement<FieldInfo> FieldInfo_T;
typedef FixedSizeElement<IndexInfo> IndexInfo_T;
typedef FixedSizeElement<GraphInfo> GraphInfo_T;

#define VALUE_TYPE_EXECUTION(type, EXECUTION)\
{\
    switch (type)\
    {\
        case VT_BIGINT:\
        {\
            typedef BigInt_T TYPE;\
            { EXECUTION; }\
            break;\
        }\
        case VT_INT:\
        {\
            typedef Integer_T TYPE;\
            { EXECUTION; }\
            break;\
        }\
        case VT_DOUBLE:\
        {\
            typedef Double_T TYPE;\
            { EXECUTION; }\
            break;\
        }\
        case VT_FLOAT:\
        {\
            typedef Float_T TYPE;\
            { EXECUTION; }\
            break;\
        }\
        case VT_DECIMAL:\
        {\
            typedef Decimal_T TYPE;\
            { EXECUTION; }\
            break;\
        }\
        case VT_VARCHAR:\
        {\
            typedef Varchar_T TYPE;\
            { EXECUTION; }\
            break;\
        }\
        case VT_CHAR:\
        {\
            typedef Char_T TYPE;\
            { EXECUTION; }\
            break;\
        }\
        case VT_BOOL:\
        {\
            typedef Bool_T TYPE;\
            { EXECUTION; }\
            break;\
        }\
        case VT_DATE:\
        {\
            typedef Date_T TYPE;\
            { EXECUTION; }\
            break;\
        }\
        case VT_TIME:\
        {\
            typedef Time_T TYPE;\
            { EXECUTION; }\
            break;\
        }\
        case VT_TIMESTAMP:\
        {\
            typedef Timestamp_T TYPE;\
            { EXECUTION; }\
            break;\
        }\
        case VT_INTLIST:\
        {\
            typedef IntList_T TYPE;\
            { EXECUTION; }\
            break;\
        }\
        case VT_LONGLIST:\
        {\
            typedef LongList_T TYPE;\
            { EXECUTION; }\
            break;\
        }\
        case VT_FLOATLIST:\
        {\
            typedef FloatList_T TYPE;\
            { EXECUTION; }\
            break;\
        }\
        case VT_DOUBLELIST:\
        {\
            typedef DoubleList_T TYPE;\
            { EXECUTION; }\
            break;\
        }\
        default:\
        {\
            RTIException e;\
            e.message = "not supported ValueType, @VALUE_TYPE_EXECUTION, ";\
            std::cerr << e.message << type << ": " ;\
            throw e;\
        }\
    }\
}

bool        isVarType(ValueType type);
uint32_t    valueSizeOf(ValueType type);

#endif  // COMMON_TYPES_TYPES_H_
