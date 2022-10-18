// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

// Related include
#include "storage/util/segment_map.h"

namespace storage {

void SegmentMap::serialize(boost::archive::binary_oarchive& oa) const {
    oa << nf_sgmt_header_;
    oa << nf_sgmt_tail_;
}

void SegmentMap::deserialize(boost::archive::binary_iarchive& ia) {
    ia >> nf_sgmt_header_;
    ia >> nf_sgmt_tail_;
}

}  // namespace storage
