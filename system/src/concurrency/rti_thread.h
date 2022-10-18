// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr
//
#ifndef CONCURRENCY_RTITHREAD_H_
#define CONCURRENCY_RTITHREAD_H_

// C & C++ system include
#include <pthread.h>
#include <chrono>
#include <thread>

/*
 * RTIThread: provide interface for threaded execution
 */
class RTIThread {
  public:
    RTIThread() : tid_(0), running_(0), detached_(0) {}
    virtual ~RTIThread() {
        if (running_ == 1 && detached_ == 0) {
            pthread_detach(tid_);
        }
        if (running_ == 1) {
            pthread_cancel(tid_);
        }
    }

    static void* runThread(void* arg) {
        return ((RTIThread*)arg)->run();
    }

    virtual int start() {
        int res = pthread_create(&tid_, NULL, runThread, this);
        if (res == 0) {
            running_ = 1;
        }

        return res;
    }

    virtual int join() {
        int res = -1;
        if (running_ == 1) {
            res = pthread_join(tid_, NULL);

            if (res == 0) {
                running_ = 0;
                detached_ = 1;
            }
        }

        return res;
    }

    virtual int detach() {
        int res = -1;
        if (running_ == 1 && detached_ == 0) {
            res = pthread_detach(tid_);

            if (res == 0) {
                detached_ = 1;
            }
        }

        return res;
    }

    virtual int cancel() {
        pthread_cancel(tid_);
        return 0;
    }

    pthread_t self() { return tid_; }

    static void sleep(int msec) {
        std::this_thread::sleep_for(std::chrono::milliseconds(msec));
    }

    virtual void* run() = 0;

  private:
    pthread_t tid_;
    int running_;
    int detached_;
};

#endif  // CONCURRENCY_RTITHREAD_H_
