// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef CONCURRENCY_MUTEX_LOCK_H_
#define CONCURRENCY_MUTEX_LOCK_H_

// C & C++ system include
#include <sys/time.h>
#include <atomic>

// Other include
#include <boost/thread/thread.hpp>

/*
 * boost library mutex/lock
 */
namespace boost
{
    typedef boost::shared_mutex Mutex;
    typedef boost::shared_lock<boost::shared_mutex> ReadLock;
    typedef boost::unique_lock<boost::shared_mutex> WriteLock;
}


/*
 * light-weight lock
 */
#pragma once

#ifndef __SPIN
#define __SPIN sleep(0)
#endif

struct try_to_lock_t
{
};

const try_to_lock_t try_to_lock = {};

class ReadLock;
class WriteLock;

class Mutex
{
  public:
    Mutex() : m_readers_(0), m_writers_(0) {}
    ~Mutex() {
        assert(m_readers_ == 0);
        assert(m_writers_ == 0);
    }
    explicit Mutex(const Mutex & mutex) : m_readers_(0), m_writers_(0) {}
    Mutex& operator=(const Mutex & other) { return *this; }

    /*
     * functions for shared lock
     *  shared_lock()
     *  shared_unlock()
     *  try_shared_lock()
     */
    void shared_lock();
    bool try_shared_lock();
    void shared_unlock();

    /*
     * functions for exclusive lock
     *  lock()
     *  unlock()
     *  try_lock()
     */
    void lock(); 
    bool try_lock();
    void unlock();

    /*
     * functions for upgrade/downgrade lock
     * upgrade_lock()
     */
    void upgrade_lock();
    bool try_upgrade_lock();
    void downgrade_lock();

    uint64_t get_num_readers() { return m_readers_; }
    uint64_t get_num_writers() { return m_writers_; }

  private:
    std::atomic<uint64_t> m_readers_;
    std::atomic<uint64_t> m_writers_;
};

class ReadLock
{
  public:
    ReadLock(Mutex& mutex) : mutex_(&mutex) {
        mutex_->shared_lock();
        owns_lock_ = true;
    }

    ReadLock(Mutex& mutex, try_to_lock_t) : mutex_(&mutex) {
        owns_lock_ = mutex_->try_shared_lock();
    }

    ReadLock(Mutex* mutex, bool owns_lock)
        : mutex_(mutex), owns_lock_(owns_lock) {}

    ~ReadLock() {
        if (owns_lock_)
            mutex_->shared_unlock();
    }

    bool owns_lock() { return owns_lock_; }
    void unlock() {
        if (owns_lock_) {
            mutex_->shared_unlock();
            owns_lock_ = false;
        }
    }

    WriteLock upgrade_lock();
    WriteLock upgrade_lock(try_to_lock_t);

  private:
    Mutex* mutex_;
    bool owns_lock_;
};

class WriteLock
{
  public:
    WriteLock(Mutex& mutex) : mutex_(&mutex) {
        mutex_->lock();
        owns_lock_ = true;
    }

    WriteLock(Mutex& mutex, try_to_lock_t) : mutex_(&mutex) {
        owns_lock_ = mutex_->try_lock();
    }

    WriteLock(Mutex* mutex, bool owns_lock)
        : mutex_(mutex), owns_lock_(owns_lock) {}

    ~WriteLock() {
        if (owns_lock_)
            mutex_->unlock();
    }

    bool owns_lock() { return owns_lock_; }
    void unlock() {
        if (owns_lock_) {
            mutex_->unlock();
            owns_lock_ = false;
        }
    }

    ReadLock downgrade_lock();

  private:
    Mutex* mutex_;
    bool owns_lock_;
};

inline void Mutex::shared_lock() {
    while (1) {
        if (m_writers_ != 0) {
            __SPIN;
            continue;
        }

        m_readers_.fetch_add(1);

        if (m_writers_.load() == 0)
            break;

        m_readers_.fetch_sub(1);
        __SPIN;
    }
}

inline bool Mutex::try_shared_lock() {
    if (m_writers_ != 0) {
        return false;
    }

    m_readers_.fetch_add(1);

    if (m_writers_.load() == 0)
        return true;

    m_readers_.fetch_sub(1);
    return false;
}

inline void Mutex::shared_unlock() {
    m_readers_.fetch_sub(1);
}

inline void Mutex::lock() {
    while (1) {
        if (m_writers_.load() != 0 || m_readers_ != 0) {
            __SPIN;
            continue;
        }

        uint64_t prev = m_writers_.fetch_add(1);

        if (prev == 0 && m_readers_.load() == 0) {
            break;
        }

        m_writers_.fetch_sub(1);
        __SPIN;
    }
}

inline bool Mutex::try_lock() {
    if (m_writers_.load() !=0 || m_readers_ != 0) {
        return false;
    }

    uint64_t prev = m_writers_.fetch_add(1);

    if (prev == 0 && m_readers_.load() == 0) {
        return true;
    }

    m_writers_.fetch_sub(1);

    return false;
}

inline void Mutex::unlock() {
    m_writers_.fetch_sub(1);
}

inline void Mutex::upgrade_lock() {
    while (1) {
        if (m_writers_.load() != 0 || m_readers_ != 1) {
            __SPIN;
            continue;
        }

        uint64_t prev = m_writers_.fetch_add(1);
        if (prev == 0 && m_readers_.load() == 1) {
            m_readers_.fetch_sub(1);
            break;
        }

        m_writers_.fetch_sub(1);
        __SPIN;
    }
}

inline bool Mutex::try_upgrade_lock() {
    if (m_writers_.load() != 0 || m_readers_ > 1) {
        m_readers_.fetch_sub(1);
        return false;
    }

    uint64_t prev = m_writers_.fetch_add(1);
    if (prev == 0 && m_readers_.load() == 1) {
        m_readers_.fetch_sub(1);
        return true;
    }

    m_writers_.fetch_sub(1);
    m_readers_.fetch_sub(1);

    return false;
}

inline void Mutex::downgrade_lock() {
    m_readers_.fetch_add(1);
    m_writers_.fetch_sub(1);
}

inline WriteLock ReadLock::upgrade_lock() {
    if (owns_lock_) {
        mutex_->upgrade_lock();
        owns_lock_ = false;
        return WriteLock(mutex_, true);
    }

    return WriteLock(mutex_, false);
}

inline WriteLock ReadLock::upgrade_lock(try_to_lock_t) {
    bool success = false;
    if (owns_lock_) {
        success = mutex_->try_upgrade_lock();
        owns_lock_ = false;
    }

    return WriteLock(mutex_, success);
}

inline ReadLock WriteLock::downgrade_lock() {
    if (owns_lock_) {
        mutex_->downgrade_lock();
        owns_lock_ = false;
        return ReadLock(mutex_, true);
    }

    return ReadLock(mutex_, false);
}

#endif  // CONCURRENCY_MUTEX_LOCK_H_
