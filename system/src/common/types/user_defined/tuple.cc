// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

// Related include
#include "common/types/user_defined/tuple.h"

// Project include
#include "common/types/eval_type.h"
#include "common/types/types.h"

Tuple Tuple::getValue() const {
    return Tuple((PhysicalPtr)this);
}

PhysicalPtr Tuple::getEval() {
    return (PhysicalPtr)(this);
}

EvalValue Tuple::getValue(ValueType vt, uint32_t offset) {
    PhysicalPtr val = val_ + offset;
    EvalValue eval;
    try {
        VALUE_TYPE_EXECUTION(vt,
            eval = reinterpret_cast<TYPE*>(val)->getEval())
    } catch (RTIException& e) {
        std::cout << e.message << std::endl;
    }

    return eval;
}

uint32_t Tuple::hash(uint32_t seed, ValueType vt, uint32_t offset) {
    PhysicalPtr val = val_ + offset;
    uint32_t hash_val = 0;
    
    VALUE_TYPE_EXECUTION(vt,
        EvalValue eval = reinterpret_cast<TYPE*>(val)->getEval();\
        hash_val = eval.hash(seed))
    
    return hash_val;
}
