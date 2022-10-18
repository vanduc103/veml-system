// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef COMMON_TYPES_USER_DEFINED_TUPLE_H_
#define COMMON_TYPES_USER_DEFINED_TUPLE_H_

// Project include
#include "common/def_const.h"
#include "common/types/def_const.h"

class EvalValue;

class Tuple {
  public:
    Tuple(const Tuple& other) : val_(other.val_) {}
    Tuple(PhysicalPtr val) : val_(val) {}

    /*
     * functions as a storage_type
     */
    Tuple       getValue() const;
    PhysicalPtr getEval();

    /*
     * functions as a eval_type
     */
    PhysicalPtr get() const { return val_; }
    EvalValue   getValue(ValueType vt, uint32_t offset);

    uint32_t    hash(uint32_t seed, ValueType vt, uint32_t offset);

  private:
    const PhysicalPtr val_;
};

#endif  // COMMON_TYPES_USER_DEFINED_TUPLE_H_
