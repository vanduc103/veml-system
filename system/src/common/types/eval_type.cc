// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

// Related include
#include "common/types/eval_type.h"

const std::string EvalValue::ns_err = "not supported ValueType, ";

template <>
IntList& EvalValue::get() {
    assert(type == ET_INTLIST);
    return *reinterpret_cast<IntList*>(std::get<PhysicalPtr>(data));
}

template <>
LongList& EvalValue::get() {
    assert(type == ET_LONGLIST);
    return *reinterpret_cast<LongList*>(std::get<PhysicalPtr>(data));
}

template <>
FloatList& EvalValue::get() {
    assert(type == ET_FLOATLIST);
    return *reinterpret_cast<FloatList*>(std::get<PhysicalPtr>(data));
}

template <>
DoubleList& EvalValue::get() {
    assert(type == ET_DOUBLELIST);
    return *reinterpret_cast<DoubleList*>(std::get<PhysicalPtr>(data));
}

template <>
const IntList& EvalValue::get() const {
    assert(type == ET_INTLIST);
    return *reinterpret_cast<const IntList*>(std::get<PhysicalPtr>(data));
}

template <>
const LongList& EvalValue::get() const {
    assert(type == ET_LONGLIST);
    return *reinterpret_cast<const LongList*>(std::get<PhysicalPtr>(data));
}

template <>
const FloatList& EvalValue::get() const {
    assert(type == ET_FLOATLIST);
    return *reinterpret_cast<const FloatList*>(std::get<PhysicalPtr>(data));
}

template <>
const DoubleList& EvalValue::get() const {
    assert(type == ET_DOUBLELIST);
    return *reinterpret_cast<const DoubleList*>(std::get<PhysicalPtr>(data));
}

std::string EvalValue::to_string() const {
    if (getEvalType() == ET_BOOL) {
        if (get<Bool>().get()) {
            return "True";
        } else {
            return "False";
        }
    }

    if (getEvalType() == ET_INTLIST) {
        return getIntListConst().to_string();
    }
    if (getEvalType() == ET_LONGLIST) {
        return getLongListConst().to_string();
    }
    if (getEvalType() == ET_FLOATLIST) {
        return getFloatListConst().to_string();
    }
    if (getEvalType() == ET_DOUBLELIST) {
        return getDoubleListConst().to_string();
    }
    if (getEvalType() == ET_INTVEC) {
        return vec_to_string(getIntVecConst());
    }
    if (getEvalType() == ET_LONGVEC) {
        return vec_to_string(getLongVecConst());
    }
    if (getEvalType() == ET_FLOATVEC) {
        return vec_to_string(getFloatVecConst());
    }
    if (getEvalType() == ET_DOUBLEVEC) {
        return vec_to_string(getDoubleVecConst());
    }

    std::stringstream ss;
    ss << *(this);
    return ss.str();
}
