// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef COMMON_TYPES_BASIC_ARITHMETIC_H_
#define COMMON_TYPES_BASIC_ARITHMETIC_H_

// Project include
#include "common/def_const.h"
#include "common/types/def_const.h"
#include "common/types/hash_util.h"

// C & C++ system include
#include <cstdint>
#include <iostream>
#include <cassert>
#include <limits>
#include <utility>

class BigInt {
  public:
    BigInt() : val_(0) {}
    BigInt(int64_t val) : val_(val) {}
    BigInt(const BigInt& other) : val_(other.val_) {}

    inline BigInt& operator= (const BigInt& other) {
        this->val_ = other.val_;
        return *this;
    }
    inline BigInt operator+(const BigInt& other) const { return val_ + other.val_; }
    inline BigInt operator-(const BigInt& other) const { return val_ - other.val_; }
    inline BigInt operator*(const BigInt& other) const { return val_ * other.val_; }
    inline BigInt operator/(const BigInt& other) const { return val_ / other.val_; }
    inline BigInt operator%(const BigInt& other) const { return val_ % other.val_; }

    inline bool operator==(const BigInt& other) const { return val_ == other.val_; }
    inline bool operator!=(const BigInt& other) const { return val_ != other.val_; }
    inline bool operator< (const BigInt& other) const { return val_ <  other.val_; }
    inline bool operator> (const BigInt& other) const { return val_ >  other.val_; }
    inline bool operator<=(const BigInt& other) const { return val_ <= other.val_; }
    inline bool operator>=(const BigInt& other) const { return val_ >= other.val_; }

    inline BigInt& operator++()   { val_++; return *this; }
    inline BigInt& operator--()   { val_--; return *this; }

    inline int64_t get() const          { return val_; }
    inline BigInt  getValue() const     { return BigInt(val_); }
    inline BigInt  getEval() const      { return BigInt(val_); }

    inline size_t  hash(uint32_t seed)  { return HashUtil::hash(&val_, sizeof(int64_t), seed); }
    static BigInt  getMax()             { return std::numeric_limits<int64_t>::max(); }
    static BigInt  getMin()             { return std::numeric_limits<int64_t>::min(); }

    friend inline std::ostream& operator<<(std::ostream& os, const BigInt& arith) {
        os << arith.val_;
        return os;
    }

  private:
    int64_t val_;
};

class Integer {
  public:
    Integer() : val_(0) {}
    Integer(int32_t val) : val_(val) {}
    Integer(const Integer& other) : val_(other.val_) {}

    inline Integer& operator= (const Integer& other) {
        this->val_ = other.val_;
        return *this;
    }
    inline Integer operator+ (const Integer& other) const { return val_ + other.val_; }
    inline Integer operator- (const Integer& other) const { return val_ - other.val_; }
    inline Integer operator* (const Integer& other) const { return val_ * other.val_; }
    inline Integer operator/ (const Integer& other) const { return val_ / other.val_; }
    inline Integer operator% (const Integer& other) const { return val_ % other.val_; }

    inline bool operator==(const Integer& other) const { return val_ == other.val_; }
    inline bool operator!=(const Integer& other) const { return val_ != other.val_; }
    inline bool operator< (const Integer& other) const { return val_ <  other.val_; }
    inline bool operator> (const Integer& other) const { return val_ >  other.val_; }
    inline bool operator<=(const Integer& other) const { return val_ <= other.val_; }
    inline bool operator>=(const Integer& other) const { return val_ >= other.val_; }

    inline Integer& operator++()   { val_++; return *this; }
    inline Integer& operator--()   { val_--; return *this; }

    inline int32_t get() const          { return val_; }
    inline BigInt  getValue() const     { return BigInt(val_); }
    inline BigInt  getEval() const      { return BigInt(val_); }

    inline size_t  hash(uint32_t seed)  { return HashUtil::hash(&val_, sizeof(int32_t), seed); }
    static Integer getMax()             { return std::numeric_limits<int32_t>::max(); }
    static Integer getMin()             { return std::numeric_limits<int32_t>::min(); }

    friend inline std::ostream& operator<<(std::ostream& os, const Integer& arith) {
        os << arith.val_;
        return os;
    }

  private:
    int32_t val_;
};

class Double {
  public:
    Double() : val_(0.0) {}
    Double(double val) : val_(val) {}
    Double(const Double& other) : val_(other.val_) {}

    inline Double& operator= (const Double& other) {
        this->val_ = other.val_;
        return *this;
    }
    inline Double operator+ (const Double& other) const { return val_ + other.val_; }
    inline Double operator- (const Double& other) const { return val_ - other.val_; }
    inline Double operator* (const Double& other) const { return val_ * other.val_; }
    inline Double operator/ (const Double& other) const { return val_ / other.val_; }

    inline bool operator==(const Double& other) const { return val_ == other.val_; }
    inline bool operator!=(const Double& other) const { return val_ != other.val_; }
    inline bool operator< (const Double& other) const { return val_ <  other.val_; }
    inline bool operator> (const Double& other) const { return val_ >  other.val_; }
    inline bool operator<=(const Double& other) const { return val_ <= other.val_; }
    inline bool operator>=(const Double& other) const { return val_ >= other.val_; }

    inline Double& operator++()   { val_++; return *this; }
    inline Double& operator--()   { val_--; return *this; }

    inline double get() const           { return val_; }
    inline Double getValue() const      { return Double(val_); }
    inline Double getEval() const       { return Double(val_); }

    inline size_t hash(uint32_t seed)   { return HashUtil::hash(&val_, sizeof(double), seed); }
    static Double getMax()              { return std::numeric_limits<double>::max(); }
    static Double getMin()              { return std::numeric_limits<double>::min(); }

    friend inline std::ostream& operator<<(std::ostream& os, const Double& arith) {
        os << arith.val_;
        return os;
    }

  private:
    double val_;
};

class Float {
  public:
    Float() : val_(0.0) {}
    Float(float val) : val_(val) {}
    Float(const Float& other) : val_(other.val_) {}

    inline Float& operator= (const Float& other) {
        this->val_ = other.val_;
        return *this;
    }
    inline Float operator+ (const Float& other) const { return val_ + other.val_; }
    inline Float operator- (const Float& other) const { return val_ - other.val_; }
    inline Float operator* (const Float& other) const { return val_ * other.val_; }
    inline Float operator/ (const Float& other) const { return val_ / other.val_; }

    inline bool operator==(const Float& other) const { return val_ == other.val_; }
    inline bool operator!=(const Float& other) const { return val_ != other.val_; }
    inline bool operator< (const Float& other) const { return val_ <  other.val_; }
    inline bool operator> (const Float& other) const { return val_ >  other.val_; }
    inline bool operator<=(const Float& other) const { return val_ <= other.val_; }
    inline bool operator>=(const Float& other) const { return val_ >= other.val_; }

    inline Float& operator++()   { val_++; return *this; }
    inline Float& operator--()   { val_--; return *this; }

    inline float  get() const           { return val_; }
    inline Double getValue() const      { return Double(val_); }
    inline Double getEval() const       { return Double(val_); }

    inline size_t hash(uint32_t seed)   { return HashUtil::hash(&val_, sizeof(float), seed); }
    static Float  getMax()              { return std::numeric_limits<int32_t>::max(); }
    static Float  getMin()              { return std::numeric_limits<int32_t>::min(); }

    friend inline std::ostream& operator<<(std::ostream& os, const Float& arith) {
        os << arith.val_;
        return os;
    }

  private:
    float val_;
};

#endif  // COMMON_TYPES_BASIC_ARITHMETIC_H_
