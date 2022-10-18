// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef PERSISTENCY_LOG_MANAGER_H_
#define PERSISTENCY_LOG_MANAGER_H_

// Project include
#include "common/def_const.h"
#include "concurrency/mutex_lock.h"

// C & C++ system include
#include <fcntl.h>

namespace persistency {

namespace util {
    class BufferReader;
}

class LogManager
{
  public:
    enum Type { START, DML, END };
    LogManager(); 

    ~LogManager() {
        for (unsigned i = 0; i < NUM_LOG_FILE; i++) {
            if(fp_[i]) fclose(fp_[i]);
        }
    }

    static LogManager* getManager() {
        static LogManager log_manager_;
        return &log_manager_;
    }

    static void setEnable(bool enable)
        { getManager()->setEnable_(enable); }

    static bool isEnable()
        { return getManager()->isEnable_(); }

    static void truncate()
        { getManager()->truncate_(); }

    static void logDML(int64_t tid, size_t size, const char* data)
        { getManager()->logDML_(tid, size, data); }

    static void recover()
        { getManager()->recover_(); }

  private:
    // management
    void setEnable_(bool enable);
    bool isEnable_();
    void updateLsn_(int64_t lsn);
    void truncate_();

    // log
    void logDML_(int64_t tid, size_t size, const char* data);

    // recover
    void recover_();
    void recoverSingle_();
    void recoverMulti_();
    void recover_(util::BufferReader& buffer);
    void recoverDML_(util::BufferReader& reader);

    std::string             log_file_;
    std::atomic<int64_t>    lsn_;

    Mutex                   mx_logger_[NUM_LOG_FILE];
    FILE*                   fp_[NUM_LOG_FILE];    

    bool                    enable_;
};

inline void LogManager::updateLsn_(int64_t lsn) {
    if (lsn > lsn_)
        lsn_ = lsn;
}

inline void LogManager::truncate_() {
    if (!enable_)
        return;

    for (unsigned i = 0; i < NUM_LOG_FILE; i++) {
        if (fp_[i])
            fclose(fp_[i]);

        std::stringstream ss;
        ss << log_file_ << "_" << i;

        fp_[i] = fopen(ss.str().c_str(), "w+");
    }
}

inline void LogManager::logDML_(int64_t tid, size_t size, const char* data) {
    if (!enable_)
        return;

    int64_t lsn = lsn_.fetch_add(1);
    int id = tid % NUM_LOG_FILE;
    FILE* fp = fp_[id];

    WriteLock write_lock(mx_logger_[id]);

    fwrite(&lsn, sizeof(int64_t), 1, fp);
    fwrite(&size, sizeof(size_t), 1, fp);
    fwrite(data, size, 1, fp);
    fflush(fp);
}

}  // namespace persistency

#endif  // PERSISTENCY_LOG_MANAGER_H_
