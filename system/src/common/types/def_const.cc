// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

// Related include
#include "common/types/def_const.h"

// C & C++ system include
#include <cstring>
#include <cassert>

std::string getNameOfValueType(ValueType type) {
    switch (type) {
        case VT_BIGINT:
            return "BIGINT";
        case VT_INT:
            return "INT";
        case VT_DOUBLE:
            return "DOUBLE";
        case VT_FLOAT:
            return "FLOAT";
        case VT_DECIMAL:
            return "DECIMAL";
        case VT_VARCHAR:
            return "VARCHAR";
        case VT_CHAR:
            return "CHAR";
        case VT_BOOL:
            return "BOOL";
        case VT_DATE:
            return "DATE";
        case VT_TIME:
            return "TIME";
        case VT_TIMESTAMP:
            return "TIMESTAMP";
        case VT_INTLIST:
            return "INTLIST";
        case VT_LONGLIST:
            return "LONGLIST";
        case VT_FLOATLIST:
            return "FLOATLIST";
        case VT_DOUBLELIST:
            return "DOUBLELIST";
        case VT_UDT:
            return "USER_DEFINED";
        default:
            std::string msg = "not supported value type @getNameOfValueType(ValueType type)\n";
            RTI_EXCEPTION(msg);
    }
}

ValueType getValueType(std::string s){
    if (s=="BIGINT") {return VT_BIGINT;}
	else if(s=="INT") {return VT_INT;}
	else if(s=="DOUBLE") {return VT_DOUBLE;}
	else if(s=="FLOAT") {return VT_FLOAT;}
	else if(s=="DECIMAL") {return VT_DECIMAL;}
	else if(s=="VARCHAR") {return VT_VARCHAR;}
	else if(s=="CHAR") {return VT_CHAR;}
	else if(s=="BOOL") {return VT_BOOL;}
	else if(s=="DATE") {return VT_DATE;}
	else if(s=="TIME") {return VT_TIME;}
	else if(s=="TIMESTAMP") {return VT_TIMESTAMP;}
	else if(s=="INTLIST") {return VT_INTLIST;}
	else if(s=="LONGLIST") {return VT_LONGLIST;}
	else if(s=="FLOATLIST") {return VT_FLOATLIST;}
	else if(s=="DOUBLELIST") {return VT_DOUBLELIST;}
	else if(s=="USER_DEFINED") {return VT_UDT;}
	else {
        std::string msg = "not supported value type @getValueType(std::string s)\n";
        RTI_EXCEPTION(msg);
    }
}
