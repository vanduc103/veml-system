// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef COMMON_TYPES_BASIC_TIME_H_
#define COMMON_TYPES_BASIC_TIME_H_

// Project include
#include "common/def_const.h"
#include "common/types/def_const.h"
#include "common/types/hash_util.h"

// C & C++ system include
#include <cstring>
#include <ostream>
#include <sstream>
#include <limits>
#include <regex>

class Time {
  public:
    // "HH:mm:SS"
    Time() : time_(0) {}

    explicit Time(std::string val) {
        std::regex re("(\\d{2}):(\\d{2}):(\\d{2})");
        std::smatch match;

        if (std::regex_match(val, match, re)) {
            if (match.size() == 4) {
                uint32_t hour = atoi(match[1].str().c_str());
                uint32_t min = atoi(match[2].str().c_str());
                uint32_t sec = atoi(match[3].str().c_str());

                // check timestamp range
                valid_check(hour, min, sec);

                // set time
                set_time(hour, min, sec);
                return;
            }
        }
        std::string err("failed to parse time data");
        RTI_EXCEPTION(err);
    }

    Time(const Time& other) : time_(other.time_) {
        valid_check();
    }

    Time(uint16_t hour, uint8_t min, uint8_t sec) {
        valid_check(hour, min, sec);
        set_time(hour, min, sec);
    }

    inline Time& operator=(const Time& other) {
        this->time_ = other.time_;
        valid_check();

        return *this;
    }

    inline bool operator==(const Time& other) const { return this->time_ == other.time_; }
    inline bool operator!=(const Time& other) const { return !(*this  == other); }
    inline bool operator< (const Time& other) const { return this->time_ < other.time_; }
    inline bool operator> (const Time& other) const { return other < *this; }
    inline bool operator<=(const Time& other) const { return !(*this > other); }
    inline bool operator>=(const Time& other) const { return !(*this < other); }

    friend inline std::ostream& operator<<(std::ostream& os, const Time& time) {
        os << std::setw(2) << std::setfill('0') << static_cast<int>(time.get_hour());
        os << ":" << std::setw(2) << std::setfill('0') << static_cast<int>(time.get_min());
        os << ":" << std::setw(2) << std::setfill('0') << static_cast<int>(time.get_sec());
        return os;
    }

    std::string to_string() {
        std::stringstream ss;
        ss << (*this);
        return ss.str();
    }

    inline uint32_t get_hour() const {
        return (time_ & 0xFFFF0000) >> 16;
    }

    inline uint32_t get_min() const {
        return (time_ & 0x0000FF00) >> 8;
    }
    
    inline uint32_t get_sec() const {
        return (time_ & 0x000000FF);
    }

    inline int add_sec(int sec) {
        uint32_t new_sec = get_sec() + sec;
        set_sec(new_sec % 60);
        return add_min(new_sec / 60);
    }

    inline int add_min(int min) {
        uint32_t new_min = get_min() + min;
        set_min(new_min % 60);
        return add_hour(new_min / 60);
    }

    inline int add_hour(int hour) {
        uint32_t new_hour = get_hour() + hour;
        set_hour(new_hour % 24);
        return new_hour / 24;
    }

    Time get() const            { return *this; }
    Time getEval() const        { return *this; }
    Time getValue() const       { return *this; }
    size_t hash(uint32_t seed)  { return HashUtil::hash(this, sizeof(Time), seed); }

    friend class Timestamp;

  private:
    void set_time(uint32_t hour, uint32_t min, uint32_t sec) {
        time_ = (hour << 16) + (min << 8) + sec;
    }

    void set_hour(uint32_t hour) {
        time_ = (time_ & 0x0000FFFF) + (hour << 16);
    }

    void set_min(uint32_t min) {
        time_ = (time_ & 0xFFFF00FF) + (min << 8);
    }
    
    void set_sec(uint32_t sec) {
        time_ = (time_ & 0xFFFFFF00) + sec;
    }

    void valid_check() {
        valid_check(get_hour(), get_min(), get_sec());
    }

    void valid_check(uint32_t hour, uint32_t min, uint32_t sec) {
        // check timestamp range
        if (hour > 23 || min >59 || sec > 59) {
            std::string err = "invalid range for time, " + std::to_string(hour)
                + ":" + std::to_string(min) + ":" + std::to_string(sec);
            RTI_EXCEPTION(err)
        }
    }

    uint32_t time_;
};

namespace std {

template<>
class numeric_limits<Time> {
 public:
    static Time min() noexcept { return Time("00:00:00"); }
    static Time max() noexcept { return Time("23:59:59"); }
};

} // namespace std

#endif  // COMMON_TYPES_BASIC_TIME_H_
