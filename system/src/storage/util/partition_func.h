// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef STORAGE_UTIL_PARTITION_FUNC_H_
#define STORAGE_UTIL_PARTITION_FUNC_H_

// Project include
#include "common/def_const.h"
#include "storage/catalog.h"

namespace storage {

typedef std::function<int32_t (EvalValue)> PartEvalFunc;

class PartitionFunc  {
  public:
    enum PartitionType {
        HASH_MOD,
        RANGE,
    };

    /**
     * @brief evaluate partition id
     */
    virtual void init() = 0;
    virtual int32_t evaluate(EvalValue val) = 0;
    virtual PartitionType getType() = 0;

    static PartEvalFunc generate(PartitionFunc* func) {
        return std::bind(&PartitionFunc::evaluate,
                    func, std::placeholders::_1);
    }

  protected:
    PartitionType type_;
};

class HashMod : public PartitionFunc {
  public:
    static HashMod* create(uint32_t mod) {
        return new HashMod(mod);
    }

    void init() {}
    int32_t evaluate(EvalValue val);
    PartitionType getType() { return HASH_MOD; }

  protected:
    HashMod(uint32_t mod) : mod_(mod) {}

  protected:
    uint32_t mod_;
};

class Range : public PartitionFunc {
  public:
    static Range* create(LongVec& split) {
        return new Range(split);
    }

    void init() {};
    int32_t evaluate(EvalValue val);
    PartitionType getType() { return RANGE; }

  protected:
    Range(LongVec& split) : split_(split) {}

  protected:
    LongVec split_;
};

}  // namespace storage

#endif  // STORAGE_UTIL_PARTITION_FUNC_H_
