// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

// Related include
#include "concurrency/version.h"
#include "concurrency/transaction.h"

// Project include
#include "storage/catalog.h"
//#include "memory/rti_memory_pool.h"

/*
 * constructor
 */
Version::Version(VersionType type, int64_t tid, int64_t start_ts,
        uint32_t pa_id, LogicalPtr idx, PhysicalPtr ptr, int32_t len, uint32_t element_idx, PhysicalPtr intList)
    : type_(type), tid_(tid), start_ts_(start_ts), cts_(Transaction::NOT_STARTED),
    pa_id_(pa_id), idx_(idx), element_idx_(element_idx), intList_(intList), prev_(nullptr), next_(nullptr) {
    if (len > 0) {
        image_ = ptr;
        //image_ = new (RTIMemoryPool::get(tid)) char[len];
        //memcpy(image_, ptr, len);
    }
}

/*
 * getter
 */
Version::VersionType Version::getType() {
    return type_;
}

int64_t Version::getTid() {
    return tid_;
}

int64_t Version::getStartTs() {
    return start_ts_;
}

int64_t Version::getCts() {
    return cts_;
}

uint32_t Version::getPAId() {
    return pa_id_;
}

LogicalPtr Version::getIdx() {
    return idx_;
}

EvalValue Version::getImage() {
    return image_;
}

uint32_t Version::getElementIdx() {
    return element_idx_;
}

PhysicalPtr Version::getIntList() {
    return intList_;
}

Version* Version::getPrev() {
    return prev_;
}

Version* Version::getNextInTx() {
    return next_;
}

Version* Version::getLast() {
    if(next_) {
        return next_->getLast();
    } else {
        return this;
    }
}

Version* Version::getLatest(
        int64_t start, int64_t current, Version*& latest, bool& has_insert) {
    int64_t ts = -1;
    switch (TransactionManager::getLevel()) {
        case TransactionManager::READ_UNCOMMITTED:
            assert(false && "not implenmented yet");
            break;
        case TransactionManager::READ_COMMITTED:
            ts = current;
            break;
        case TransactionManager::REPEATABLE_READ:
            ts = start;
            break;
        case TransactionManager::SERIALIZABLE:
            assert(false && "not implenmented yet");
            break;
        default:
            assert(false && "wront tx isolation type");
            break;
    }

    if (cts_ > 0 && ts > cts_) {
        if ((latest == nullptr) || (latest && cts_ > latest->getCts())) {
            latest = this;
        }
    }


    has_insert |= (type_ == INSERT);

    if (prev_) {
        return prev_->getLatest(start, current, latest, has_insert);
    } else {
        return latest;
    }
}

Version* Version::getClosed(Version*& latest, bool& has_insert) {
    if (cts_ == -1)
        return nullptr;

    has_insert |= (type_ == INSERT);
    if ((latest == nullptr) || (latest && cts_ > latest->getCts())) {
        latest = this;
    }

    if (prev_) {
        return prev_->getClosed(latest, has_insert);
    } else {
        return latest;
    }
}

/*
 * setter
 */
void Version::setNext(Version* version) {
    assert(next_ == nullptr);
    next_ = version;
}

/*
 * operation
 */
void Version::commit(int64_t cts) {
    if ( getType() != 3) {
        cts_ = cts;
    } else {
        //IntList* dest_list = reinterpret_cast<IntList*>(intList_);
        //dest_list->commitStatus(element_idx_, cts);
    }
}

void Version::abort() {
    if ( getType() != 3) {
        cts_ = Transaction::ABORTED;
    } else {
        //IntList* dest_list = reinterpret_cast<IntList*>(intList_);
        //dest_list->abortStatus(element_idx_);
    }
}

void Version::consolidate() {
    if ( getType() != 3) {
        PABase* pa = Metadata::getPagedArray(pa_id_);
        pa->consolidateVersion(idx_);
    } else {
        //IntList* dest_list = reinterpret_cast<IntList*>(intList_);
        //dest_list->consolidateStatus(element_idx_, cts_);
    }
}

void Version::clearVersionList() {
    if (prev_) {
        if (prev_->getCts() != -1) {
            prev_->clearVersionList();
            //delete prev_;
            //delete prev_; deallocate from RTIMemoryPool
            setPrev(nullptr);
        }
    }
}
