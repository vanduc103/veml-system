// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

// Related include
#include "common/types/types.h"

bool isVarType(ValueType type) {
    switch (type) {
        case VT_BIGINT:
        case VT_INT:
        case VT_DOUBLE:
        case VT_FLOAT:
        case VT_DECIMAL:
        case VT_CHAR:
        case VT_BOOL:
        case VT_DATE:
        case VT_TIME:
        case VT_TIMESTAMP:
        case VT_INTLIST:
        case VT_LONGLIST:
        case VT_FLOATLIST:
        case VT_DOUBLELIST:
            return false;
        case VT_VARCHAR:
            return true;
        default:
            std::string err = "not supported value type, isVarType, ";
            RTI_EXCEPTION(err);
            return false;
    }
}

uint32_t valueSizeOf(ValueType type) {
    VALUE_TYPE_EXECUTION(type, return sizeof(TYPE); );

    assert(false && "can't reach here");
    return 0;
}
