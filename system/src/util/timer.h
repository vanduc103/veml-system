// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef UTIL_TIMER_H_
#define UTIL_TIMER_H_

// C & C++ system include
#include <chrono>
#include <sys/time.h>
#include <cassert>
#include <string>

namespace util {

class Timer {
 public:
    Timer() : started_(false) {}

    void start() {
        assert(started_ == false && "already started");
        gettimeofday(&start_tv_, 0);
        started_ = true;
    }

    void stop() {
        assert(started_ == true && "not started");
        gettimeofday(&end_tv_, 0);
        started_ = false;
    }

    long getElapsedMicro() {
        assert(started_ == false && "in running");
        return ((end_tv_.tv_sec - start_tv_.tv_sec) * 1000 * 1000)
            + (end_tv_.tv_usec - start_tv_.tv_usec);
    }
    long getElapsedMilli() { return getElapsedMicro() / 1000; }
    long getElapsedSecond() { return getElapsedMicro() / 1000 / 1000; }

    static std::string currentDateTime() {
        time_t now = time(0);
        struct tm  tstruct;
        char       buf[80];
        tstruct = *localtime(&now);
        strftime(buf, sizeof(buf), "%Y-%m-%d.%X", &tstruct);
        return buf;
    }

    static void printCurrent(std::string msg) {
        std::cout << msg << " : " << currentDateTime() << std::endl;
    }

    void print(std::string msg) {
        long elapsed = getElapsedMilli();

        printf("%s: %ld ms\n", msg.c_str(), elapsed);
    }

    static int64_t currentTimestamp(){
        using std::chrono::high_resolution_clock;
        using std::chrono::duration_cast;
        using std::chrono::milliseconds;
        auto t = high_resolution_clock::now();
        auto d = duration_cast<milliseconds>(t.time_since_epoch()).count();
        return d;
    }

 private:
    bool started_;
    timeval start_tv_;
    timeval end_tv_;
};

}  // namespace util

#endif  // UTIL_TIMER_H_
