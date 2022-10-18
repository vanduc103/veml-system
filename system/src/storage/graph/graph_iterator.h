// Copyright 2021 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef STORAGE_GRAPH_GRAPH_ITERATOR_H_
#define STORAGE_GRAPH_GRAPH_ITERATOR_H_

// Project include
#include "common/def_const.h"

namespace storage {

class GraphIterator {
  public:
    virtual ~GraphIterator() {}
    virtual int64_t getSrc() = 0;
    virtual int64_t getDest() = 0;
    virtual int64_t getEprop() = 0;
    virtual LongVec getDests() = 0;
    virtual LongVec getEprops() = 0;
    virtual int64_t getMin() = 0;
    virtual int64_t getMax() = 0;

    virtual void next() = 0;
    virtual void nextList(bool skip_empty=true) = 0;
    virtual bool isEnded() = 0;
};

}  // namespace storage

#endif  // STORAGE_GRAPH_GRAPH_ITERATOR_H_
