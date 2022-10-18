// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef COMMON_TYPES_BASIC_BOOL_H_
#define COMMON_TYPES_BASIC_BOOL_H_

class Bool {
  public:
    Bool() : val_(false) {}
    Bool(bool val) : val_(val) {}
    Bool(const Bool& other) : val_(other.val_) {}

    inline operator bool()  { return val_; }
    inline bool operator!() { return val_; }

    inline bool operator< (const Bool& other) const { return val_ <  other.val_; }
    inline bool operator> (const Bool& other) const { return val_ >  other.val_; }
    inline bool operator||(const Bool& other) const { return val_ || other.val_; }
    inline bool operator&&(const Bool& other) const { return val_ && other.val_; }
    inline bool operator==(const Bool& other) const { return val_ == other.val_; }

    inline bool get() const             { return val_; }
    inline Bool getEval() const         { return *this; }
    inline Bool getValue() const        { return *this; }
    inline size_t hash(uint32_t seed)   { return HashUtil::hash(this, sizeof(Bool), seed); }

  private:
    bool val_;
};

#endif  // COMMON_TYPES_BASIC_BOOL_H_
