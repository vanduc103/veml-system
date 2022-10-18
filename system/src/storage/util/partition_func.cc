// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

// Related include
#include "storage/util/partition_func.h"

namespace storage {

int32_t HashMod::evaluate(EvalValue val) {
    if (val.getEvalType() == ET_BIGINT) {
        return val.getBigInt().get() % mod_;
    }

    return val.hash(0) % mod_;
}

int32_t Range::evaluate(EvalValue val) {
    int64_t v = val.getBigInt().get();
    auto itr = std::upper_bound(split_.begin(), split_.end(), v);

    return itr - split_.begin();
}

}  // namespace storage
