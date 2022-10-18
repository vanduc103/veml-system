// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef COMMON_TYPES_BASIC_DATE_H_
#define COMMON_TYPES_BASIC_DATE_H_

// Project include
#include "common/def_const.h"
#include "common/types/def_const.h"
#include "common/types/hash_util.h"

// C & C++ system include
#include <cstring>
#include <ostream>
#include <sstream>
#include <limits>
#include <iomanip>
#include <ctime>
#include <regex>

class Timestamp;

class Date  {
 public:
    // "YYYY-MM-DD"
    Date() : date_(0) {}

    explicit Date(std::string val) {
        std::regex re("(\\d{2})-(\\d{2})-(\\d{2})");
        std::smatch match;

        if (std::regex_match(val, match, re)) {
            if (match.size() == 4) {
                uint32_t year = atoi(match[1].str().c_str());
                uint32_t month = atoi(match[2].str().c_str());
                uint32_t day = atoi(match[3].str().c_str());

                // check timestamp range
                valid_check(year, month, day);

                // set date
                set_date(year, month, day);
                return;
            }
        }
        std::string err("failed to parse date data");
        RTI_EXCEPTION(err);
    }

    Date(const Date& other) : date_(other.date_) {
        valid_check();
    }

    Date(uint16_t year, uint8_t month, uint8_t day) {
        valid_check(year, month, day);
        set_date(year, month, day);
    }

    inline Date& operator=(const Date& other) {
        this->date_ = other.date_;
        valid_check();

        return *this;
    }

    inline bool operator==(const Date& other) const { return this->date_ == other.date_; }
    inline bool operator!=(const Date& other) const { return !(*this  == other); }
    inline bool operator< (const Date& other) const { return this->date_ < other.date_; }
    inline bool operator> (const Date& other) const { return other < *this; }
    inline bool operator<=(const Date& other) const { return !(*this > other); }
    inline bool operator>=(const Date& other) const { return !(*this < other); }

    bool operator==(const Timestamp& other) const;
    bool operator!=(const Timestamp& other) const;
    bool operator< (const Timestamp& other) const;
    bool operator> (const Timestamp& other) const;
    bool operator<=(const Timestamp& other) const;
    bool operator>=(const Timestamp& other) const;

    friend inline std::ostream& operator<<(std::ostream& os, const Date& date) {
        os << date.get_year();
        os << "-" << std::setw(2) << std::setfill('0') << static_cast<int>(date.get_month());
        os << "-" << std::setw(2) << std::setfill('0') << static_cast<int>(date.get_day());
        return os;
    }

    std::string to_string() {
        std::stringstream ss;
        ss << (*this);
        return ss.str();
    }

    inline uint32_t get_year() const {
        return (date_ & 0xFFFF0000) >> 16;
    }

    inline uint32_t get_month() const {
        return (date_ & 0x0000FF00) >> 8;
    }
    
    inline uint32_t get_day() const {
        return (date_ & 0x000000FF);
    }

    inline Date add_days(int days) {
        struct tm date = { 0, 0, 12 };
        date.tm_year = get_year() - 1900;
        date.tm_mon = get_month() - 1;
        date.tm_mday = get_day();

        const time_t ONE_DAY = 24 * 60 * 60 ;
        time_t date_seconds = mktime(&date) + (days * ONE_DAY) ;

        date = *localtime(&date_seconds);

        set_date(date.tm_year + 1900, date.tm_mon + 1, date.tm_mday);
        return *this;
    }

    inline uint64_t get_ts() const {
        return ((uint64_t)date_) << 32;
    }

    Date get() const            { return *this; }
    Date getEval() const        { return *this; }
    Date getValue() const       { return *this; }
    size_t hash(uint32_t seed)  { return HashUtil::hash(this, sizeof(Date), seed); }

    friend class Timestamp;

 private:
    void set_date(uint32_t year, uint32_t month, uint32_t day) {
        date_ = (year << 16) + (month << 8) + day;
    }

    void valid_check() {
        valid_check(get_year(), get_month(), get_day());
    }

    void valid_check(uint32_t year, uint32_t month, uint32_t day) {
        // check timestamp range
        if (year > 9999 || month < 1 || month > 12
                || day < 1 || day > 31) {
            std::string err = "invalid range for date, " + std::to_string(year)
                + "-" + std::to_string(month) + "-" + std::to_string(day);
            RTI_EXCEPTION(err)
        }
    }

    uint32_t date_;
};

namespace std {

template<>
class numeric_limits<Date> {
 public:
    static Date min() noexcept { return Date("0000-00-00"); }
    static Date max() noexcept { return Date("9999-12-31"); }
};

} // namespace std

#endif  // COMMON_TYPES_BASIC_DATE_H_
