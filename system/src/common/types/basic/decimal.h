// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef COMMON_TYPES_BASIC_DECIMAL_H_
#define COMMON_TYPES_BASIC_DECIMAL_H_

// Project include
#include "common/types/def_const.h"

// C & C++ system include
#include <cmath>
#include <limits>
#include <cstring>

// Other include
#include <ttmath/ttmathint.h>

// Fixed precision & scale decimal
// Precision: 19. Scale: 4
// Example:  123456789012345.6789
// total: 19 <----  15 ----><-4->
class Decimal {
 public:
    Decimal() {}
    typedef ttmath::Int<1> TTInt;
    typedef ttmath::Int<2> TTLInt;

    // Assume(sjkim): only valid format - xxxxxxx.xxxx or xxxxxx
    explicit Decimal(std::string val) : Decimal(val.c_str()) {}
    explicit Decimal(char const* val);

    Decimal(const Decimal& val) : val_(val.val_) {}

    // Ctor for casting from BigInt
    explicit Decimal(const BigInt& val) : val_(val.get() * k_scale_factor) {}
    explicit Decimal(const Double& val) {
        double double_val = val.get() * ((double)k_scale_factor);
        val_ = ((int)double_val);
    }

    // Used for arithmetic result & JIT-compile
    explicit Decimal(const TTInt& val) : val_(val) {}

    inline Decimal& operator=(const Decimal& rhs) {
        this->val_ = rhs.val_;
        return *this;
    }

    inline Decimal operator+(const Decimal& other) const { return Decimal(this->val_ + other.val_); }
    inline Decimal operator-(const Decimal& other) const { return Decimal(this->val_ - other.val_); }
    inline Decimal operator*(const Decimal& other) const {
        TTLInt lhs_lint(this->val_);
        TTLInt rhs_lint(other.val_);
        return Decimal(lhs_lint * rhs_lint / k_scale_factor);
    }
    inline Decimal operator/(const Decimal& other) const {
        TTLInt lhs_lint(this->val_);
        TTLInt rhs_lint(other.val_);
        return Decimal(lhs_lint * k_scale_factor / rhs_lint);
    }

    inline bool operator==(const Decimal& other) const { return this->val_ == other.val_; }
    inline bool operator!=(const Decimal& other) const { return !(*this  == other); }
    inline bool operator< (const Decimal& other) const { return this->val_ < other.val_; }
    inline bool operator> (const Decimal& other) const { return other < *this; }
    inline bool operator<=(const Decimal& other) const { return !(*this > other); }
    inline bool operator>=(const Decimal& other) const { return !(*this < other); }

    friend inline std::ostream& operator<<(std::ostream& os, const Decimal& decimal) {
        TTInt remainder = decimal.val_ % k_scale_factor; remainder.Abs();
        os << decimal.val_ / decimal.k_scale_factor << "." << std::setw(decimal.k_scale) << std::setfill('0') << remainder;
        return os;
    }

    Decimal     get() const             { return *this; }
    Decimal     getEval() const         { return *this; }
    Decimal     getValue() const        { return *this; }
    size_t      hash(uint32_t seed)     { return HashUtil::hash(this, sizeof(Decimal), seed); }
    TTInt       getTTInt() const        { return val_; }

 private:
    static const uint32_t k_precison = 19;
    static const uint32_t k_scale = 4;
    static const uint32_t k_scale_factor = 10000;

    TTInt val_;
};

inline Decimal::Decimal(char const * val) {
    // Get integer part
    const char* token = strchr(val, '.');
    TTLInt integer = atol(val);

    // Get fractional part
    TTInt fractional(0);
    if (token != nullptr) { // it has fractional part
        token += 1;
        fractional = atol(token);
        size_t len_fractional = strlen(token);
        if (len_fractional > k_scale)
            fractional /= static_cast<uint32_t>(pow(10, len_fractional - k_scale));
        else if (len_fractional < k_scale)
            fractional *= static_cast<uint32_t>(pow(10, k_scale - len_fractional));
    }
    assert(fractional < k_scale_factor);

    if (integer >= 0)
        val_ = integer * k_scale_factor + fractional;
    else
        val_ = integer * k_scale_factor - fractional;
}

namespace std {

template<>
class numeric_limits<Decimal> {
 public:
    static Decimal min() noexcept { Decimal::TTInt val; val.SetMin(); return Decimal(val); }
    static Decimal max() noexcept { Decimal::TTInt val; val.SetMax(); return Decimal(val); }
};

} // namespace std

#endif  // COMMON3_TYPE_BASIC_DECIMAL_H_
