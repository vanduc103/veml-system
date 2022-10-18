// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef COMMON_TYPES_BASIC_TIMESTAMP_H_
#define COMMON_TYPES_BASIC_TIMESTAMP_H_

// Project include
#include "common/def_const.h"
#include "common/types/def_const.h"
#include "common/types/hash_util.h"
#include "common/types/basic/date.h"
#include "common/types/basic/time.h"

// C & C++ system include
#include <sstream>

class Timestamp {
  public:
    Timestamp() {}

    explicit Timestamp(std::string val) {
        // parse date and time
        std::regex re("(\\d{4}-\\d{2}-\\d{2}) (\\d{2}:\\d{2}:\\d{2})");
        std::smatch match;

        if (std::regex_match(val, match, re)) {
            if (match.size() == 3) {
                date_ = Date(match[1].str().c_str());
                time_ = Time(match[2].str().c_str());
                return;
            }
        }
        
        std::string err("failed to parse timestamp data");
        RTI_EXCEPTION(err);
    }

    Timestamp(const Timestamp& other) : date_(other.date_), time_(other.time_) {}
    Timestamp(uint16_t year, uint8_t month, uint8_t day,
            uint16_t hour, uint8_t min, uint8_t sec) {
        date_ = Date(year, month, day);
        time_ = Time(hour, min, sec);
    }

    Timestamp(const Date& other) : date_(other) {}

    inline Timestamp& operator=(const Timestamp& other) {
        this->date_ = other.date_;
        this->time_ = other.time_;

        return *this;
    }

    inline bool operator==(const Timestamp& other) const { return get_ts() == other.get_ts(); }
    inline bool operator!=(const Timestamp& other) const { return !(*this  == other); }
    inline bool operator< (const Timestamp& other) const { return get_ts() < other.get_ts(); }
    inline bool operator> (const Timestamp& other) const { return other < *this; }
    inline bool operator<=(const Timestamp& other) const { return !(*this > other); }
    inline bool operator>=(const Timestamp& other) const { return !(*this < other); }

    inline bool operator==(const Date& other) const { return get_ts() == other.get_ts(); }
    inline bool operator!=(const Date& other) const { return !(*this  == other); }
    inline bool operator< (const Date& other) const { return get_ts() < other.get_ts(); }
    inline bool operator> (const Date& other) const { return other < *this; }
    inline bool operator<=(const Date& other) const { return !(*this > other); }
    inline bool operator>=(const Date& other) const { return !(*this < other); }

    friend inline std::ostream& operator<<(std::ostream& os, const Timestamp& ts) {
        os << ts.date_ << " " << ts.time_;
        return os;
    }

    std::string to_string() {
        std::stringstream ss;
        ss << (*this);
        return ss.str();
    }

    inline uint64_t get_ts() const {
        return ((uint64_t)(date_.date_) << 32) + time_.time_;
    }

    inline Timestamp add_sec(int sec) {
        int day = time_.add_sec(sec);
        date_.add_days(day);
        return *this;
    }

    inline Timestamp add_min(int min) {
        int day = time_.add_min(min);
        date_.add_days(day);
        return *this;
    }

    inline Timestamp add_hour(int hour) {
        int day = time_.add_hour(hour);
        date_.add_days(day);
        return *this;
    }
    
    inline Timestamp add_days(int days) {
        date_.add_days(days);
        return *this;
    }

    Timestamp get() const       { return *this; }
    Timestamp getEval() const   { return *this; }
    Timestamp getValue() const  { return *this; }
    size_t hash(uint32_t seed)  { return HashUtil::hash(this, sizeof(Time), seed); }

  private:
    Date date_;
    Time time_;
};

namespace std {

template<>
class numeric_limits<Timestamp> {
 public:
    static Timestamp min() noexcept { return Timestamp("0000-00-00 00:00:00"); }
    static Timestamp max() noexcept { return Timestamp("9999-12-31 23:59:59"); }
};

} // namespace std

#endif  // COMMON_TYPES_BASIC__H_
