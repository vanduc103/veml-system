// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef CONCURRENCY_VERSION_H_
#define CONCURRENCY_VERSION_H_

// Project include
#include "common/def_const.h"
#include "common/types/eval_type.h"

// C & C++ system include
#include <vector>

class Version
{
  public:
    enum VersionType { INSERT, UPDATE, DELETE, LIST };

    Version(VersionType type, int64_t tid, int64_t start_ts, uint32_t pa_id, LogicalPtr idx,
                PhysicalPtr ptr, int32_t len, uint32_t element_idx, PhysicalPtr intList);
    Version(const Version &obj);

    /*
     * getter/setter
     */
    VersionType     getType();
    int64_t         getTid();
    int64_t         getStartTs();
    int64_t         getCts();

    uint32_t        getPAId();
    LogicalPtr      getIdx();
    EvalValue       getImage();
    uint32_t        getElementIdx();
    PhysicalPtr     getIntList();

    Version*        getPrev();
    Version*        getNextInTx();
    Version*        getLast();
    Version*        getLatest(int64_t start, int64_t current,
                            Version*& latest, bool& has_insert);
    Version*        getClosed(Version*& latest, bool& has_insert);

    void            setPrev(Version* version) { prev_ = version; }
    void            setNext(Version* version);

    /*
     * operation
     */
    void            commit(int64_t cts);
    void            abort();

    void            consolidate();
    void            clearVersionList();

  private:
    /*
     * tx info
     */
    VersionType     type_;
    int64_t         tid_;       // transaction id
    int64_t         start_ts_;  // start timestamp 
    int64_t         cts_;       // commit timestamp

    /*
     * version info
     */
    uint32_t        pa_id_;     // paged_array id
    LogicalPtr      idx_;       // idx @ paged_array
    PhysicalPtr     image_;     // after image
    int32_t         len_;

    /*
     * List specific variables
     */
    //uint32_t        column_idx_;    // where Intlist exists
    uint32_t        element_idx_;   // element index in intlist
    PhysicalPtr     intList_;

    /*
     * link info
     */
    Version*        prev_;      // prev version @ slot
    Version*        next_;      // next version @ tx
};

#endif  // CONCURRENCY_VERSION_H_
