// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

// Related include
#include "concurrency/transaction.h"

// Project include
#include "storage/catalog.h"
#include "persistency/log_manager.h"
//#include "memory/rti_memory_pool.h"

using namespace persistency;

Transaction::Transaction() {
    tid_ = TransactionManager::getGlobalTid();
    start_ts_ = current_ts_ = cts_ = NOT_STARTED;
    versions_ = nullptr;

    log_buffer_ << LogManager::START;
    log_buffer_ << tid_;
}

void Transaction::start() {
    start_ts_ = current_ts_ = TransactionManager::getGlobalTs();
    TransactionManager::addActiveTx(tid_, start_ts_);
}

void Transaction::commit() {
    cts_ = TransactionManager::getGlobalTs();
    Version* version = versions_;
    while(version != nullptr) {
        version->commit(cts_);
        version = version->getNextInTx();
    }

    TransactionManager::quitTx(tid_, start_ts_, versions_);

    log_buffer_ << LogManager::END;
    LogManager::logDML(tid_, log_buffer_.getSize(), log_buffer_.getData());
}

void Transaction::abort() {
    cts_ = ABORTED;
    Version* version = versions_;
    while(version != nullptr) {
        version->abort();
        version = version->getNextInTx();
    }
    
    TransactionManager::quitTx(tid_, start_ts_, versions_);
}

void Transaction::startNewStmt() {
    current_ts_ = TransactionManager::getGlobalTs();
}

Version* Transaction::createVersion(Version::VersionType type,
        int64_t tid, int64_t start_ts, uint32_t pa_id, LogicalPtr idx,
        PhysicalPtr ptr, uint32_t len, uint32_t element_idx, PhysicalPtr intList) {
    //Version* version = new (RTIMemoryPool::get(tid))
    //                    Version(type, tid, start_ts, pa_id, idx, ptr, len);
    Version* version = new Version(type, tid, start_ts, pa_id, idx, ptr, len, element_idx, intList);
    
    if (versions_ == nullptr) {
        versions_ = version;
    } else {
        versions_->getLast()->setNext(version);
    }

    log_buffer_ << LogManager::DML;
    log_buffer_ << start_ts << pa_id << idx;
    log_buffer_.write(ptr, len);

    return version;
}

void* TransactionManager::Worker::run() {
    while (true) {
        RTIThread::sleep(GC_INTERVAL);
        TransactionManager::consolidate();
    }

    return NULL;
}

int64_t TransactionManager::getGlobalTid_() {
    int64_t tid = global_tid_.fetch_add(1);
    return tid;
}

int64_t TransactionManager::getGlobalTs_() {
    int64_t ts = global_ts_.fetch_add(1);
    return ts;
}

TransactionManager::Level TransactionManager::getLevel_() {
    return level_;
}

void TransactionManager::addActiveTx_(int64_t tid, int64_t ts) {
    int map_id = tid % NUM_ACTIVE_MAP;
    ReadLock read_lock(mx_active_tx_[map_id]);
    active_tx_map_[map_id].insert(ts, VersionMapItem(tid, nullptr));
}

void TransactionManager::quitTx_(int64_t tid, int64_t ts, Version* version) {
    int map_id = tid % NUM_ACTIVE_MAP;
    ReadLock read_lock(mx_active_tx_[map_id]);
    active_tx_map_[map_id].update(ts, VersionMapItem(-1, version));
}

void TransactionManager::consolidate_() {
    std::vector<Version*> deads;
    for (int i = 0; i < NUM_ACTIVE_MAP; i++) {
        WriteLock write_lock(mx_active_tx_[i]);
        auto itr = active_tx_map_[i].begin();
        while (true) {
            if (!active_tx_map_[i].remove(itr, deads)) break;
        }

        //std::cout << i << "th map removed: " << deads.size()
        //    << ", remains: " << active_tx_map_[i].size() << std::endl;
    }

    for (auto version : deads) {
        while (version != nullptr) {
            version->consolidate();
            version = version->getNextInTx();
        }
    }

    for (auto version : deads) {
        delete version;
    }
}
