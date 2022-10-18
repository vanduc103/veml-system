// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef CONCURRENCY_TRANSACTION_H_
#define CONCURRENCY_TRANSACTION_H_

// Project include
#include "concurrency/version.h"
#include "concurrency/version_map.h"
#include "concurrency/rti_thread.h"
#include "util/buffer.h"

/*
 * Transaction isolation level
 * READ UNCOMMITTED 0 - not supported yet
 * READ COMMITTED 1
 * REPEATABLE READ 2
 * SERIALIZABLE 3 - not supported yet
 */
#define ISOLATION_LEVEL 1
#define GC_INTERVAL 1000

class EvalValue;

class Transaction
{
  public:
    enum State { NOT_STARTED = -1, ABORTED = -2 };
    Transaction();

    // getter
    int64_t     getTid()                    { return tid_; }
    int64_t     getStartTs()                { return start_ts_; }
    int64_t     getTs()                     { return current_ts_; }
    int64_t     getCts()                    { return cts_; }

    // operation
    void        start();
    void        commit();
    void        abort();
    void        startNewStmt();

    // MVCC management
    Version*    createVersion(Version::VersionType type, int64_t tid, int64_t start_ts, uint32_t pa_id, 
                    LogicalPtr idx, PhysicalPtr ptr, uint32_t len, uint32_t element_idx = 0, PhysicalPtr intList = nullptr);

  private:
    int64_t                 tid_;           // transaction_id
    int64_t                 start_ts_;      // start timestamp  
    int64_t                 current_ts_;    // current timestamp
    int64_t                 cts_;           // commit timestamp
    util::BufferWriter      log_buffer_;
    Version*                versions_;
};

class TransactionManager
{
  public:
    enum Level {
        READ_UNCOMMITTED = 0,
        READ_COMMITTED = 1,
        REPEATABLE_READ = 2,
        SERIALIZABLE = 3
    };

    TransactionManager(int level) : level_(Level(level)), global_tid_(0), global_ts_(0) {
        worker_.start();
    }

    // getter
    static TransactionManager* getManager() {
        static TransactionManager transaction_manager_(ISOLATION_LEVEL);
        return &transaction_manager_;
    }

    static int64_t getGlobalTid()
        { return getManager()->getGlobalTid_(); }

    static int64_t getGlobalTs()
        { return getManager()->getGlobalTs_(); }

    static Level getLevel()
        { return getManager()->getLevel_(); }

    static void addActiveTx(int64_t tid, int64_t ts)
        { getManager()->addActiveTx_(tid, ts); }

    static void quitTx(int64_t tid, int64_t ts, Version* version)
        { getManager()->quitTx_(tid, ts, version); }

    static void consolidate()
        { getManager()->consolidate_(); }

  private:
    class Worker : public RTIThread {
      public:
        Worker() {}

        void*   run();
    };

    int64_t     getGlobalTid_();
    int64_t     getGlobalTs_();
    int64_t     getMinTs_();
    Level       getLevel_();

    void        addActiveTx_(int64_t tid, int64_t ts);
    void        quitTx_(int64_t tid, int64_t ts, Version* version);

    void        consolidate_();

    Level                           level_;
    std::atomic<int64_t>            global_tid_;
    std::atomic<int64_t>            global_ts_;
    std::atomic<int64_t>            tmp_stat_;

    Mutex                           mx_active_tx_[NUM_ACTIVE_MAP];
    VersionMap                      active_tx_map_[NUM_ACTIVE_MAP];

    Worker                          worker_;
};

#endif  // CONCURRENCY_TRANSACTION_H_
