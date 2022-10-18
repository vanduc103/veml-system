// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef COMMON_TYPES_BASIC_STRING_H_
#define COMMON_TYPES_BASIC_STRING_H_

// Project include
#include "common/types/def_const.h"
#include "common/types/hash_util.h"

// C & C++ system include
#include <ostream>
#include <cstring>

class String {
 public:
    explicit String(const char * const val) : val_(val) { }
    explicit String(const char * const val, int size)
                : val_(val) { (*this) = this->clone(size); }
    String(const std::string str)
                : val_(str.c_str()) { (*this) = this->clone(); }
    String(const std::string str, int size)
                : val_(str.c_str()) { (*this) = this->clone(size); }
    String(const char * const val, uint16_t size) : String(val) { }
    String(const String& other) : val_(other.val_) { }

    inline void operator=(const String& other) { val_ = other.val_; }

    inline bool operator==(const String& other) const { return strcmp(val_, other.val_) == 0; }
    inline bool operator!=(const String& other) const { return !(*this  == other); }
    inline bool operator< (const String& other) const { return strcmp(val_, other.val_) < 0; }
    inline bool operator> (const String& other) const { return other < *this; }
    inline bool operator<=(const String& other) const { return !(*this > other); }
    inline bool operator>=(const String& other) const { return !(*this < other); }

    friend inline std::ostream& operator<<(std::ostream& os, const String& str) {
        os << str.val_;
        return os;
    }

    const char* ptr() const         { return val_; }
    String      getEval() const     { return *this; }
    String      getValue() const    { return *this; }
    bool        isEmbedded() const  { return strlen(val_) < MAX_EMBEDDED_SIZE; }
    size_t      size() const        { return strlen(val_) + 1; }
    size_t      hash(uint32_t seed) { return HashUtil::hash(val_, size(), seed); }
    std::string get() const         { return val_; }

    String clone() const {
        size_t len = size();
        char* new_val = new char[len];
        memcpy(new_val, val_, len);
        return String(new_val);
    }

    String clone(size_t len) const {
        char* new_val = new char[len+1];
        memset(new_val, 0, len+1);
        memcpy(new_val, val_, len);
        return String(new_val);
    }

    String() : val_(nullptr) {}
    const char * val_;
};

#endif  // COMMON_TYPES_STRING_H_
