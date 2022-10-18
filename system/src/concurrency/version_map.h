// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef CONCURRENCY_VERSION_MAP_H_
#define CONCURRENCY_VERSION_MAP_H_

// Project include
#include "concurrency/mutex_lock.h"
#include "storage/util/btree.h"
class Version;

struct VersionMapItem {
    VersionMapItem(int64_t tid, Version* version)
        : tid_(tid), version_(version) {}

    int64_t     tid_;
    Version*    version_;
};

class VersionMap : public storage::BTree<long, VersionMapItem> {
  public:
    bool remove(iterator& itr, std::vector<Version*>& deads);

  protected:
    bool removeBottomUp(Node* node, int idx,
            Flag& flag, std::vector<Version*>& deads);
    void removeBottomUpBranch(Node* node, int idx, Flag& flag);

};

#endif  // CONCURRENCY_VERSION_MAP_H_
