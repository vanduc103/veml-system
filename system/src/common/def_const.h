// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef COMMON_DEF_CONST_H_
#define COMMON_DEF_CONST_H_

#include <cstdint>
#include <cassert>
#include <string>
#include <vector>
#include <set>
#include <iostream>
#include <sstream>
#include <exception>

// disallow compiler-generated default ctor, copy ctor & assign operator
#define DISALLOW_COMPILER_GENERATED(TypeName)   \
    TypeName();  \
    TypeName(const TypeName&);  \
    void operator=(const TypeName&)

// disallow compiler-generated copy ctor & assign operator
#define DISALLOW_COPY_AND_ASSIGN(TypeName)  \
    TypeName(const TypeName&);  \
    void operator=(const TypeName&)

// not implemented function or method
#define NOT_IMPL assert(false && "Not implemented")

// wrong result type btw operators
#define WRONG_RESULT_TYPE \
    assert(false && "wrong result type"); \
    return nullptr;

// exception handling
#define RTI_EXCEPTION(err) \
    RTIException e;\
    e.message = err;\
    std::cerr << e.message;\
    throw e;

struct RTIException : public std::exception {
    const char* what() const throw() {
        return message.c_str();
    }
    std::string message;
};

typedef std::pair<int32_t, int32_t> IntPair;
typedef std::vector<int32_t>        IntVec;
typedef std::set<int32_t>           IntSet;

typedef std::pair<int64_t, int64_t> LongPair;
typedef std::vector<int64_t>        LongVec;
typedef std::set<int64_t>           LongSet;

typedef std::pair<float, float>     FloatPair;
typedef std::vector<float>          FloatVec;
typedef std::set<float>             FloatSet;

typedef std::pair<double, double>   DoublePair;
typedef std::vector<double>         DoubleVec;
typedef std::set<double>            DoubleSet;

typedef std::vector<std::string>    StrVec;

template <class T>
std::string vec_to_string(const std::vector<T>& vec) {
    std::stringstream ss;
    ss << "[";
    for (auto& v : vec) {
        ss << v << ", ";
    }
    ss << "]";

    return ss.str();
}

/*
 * Address format
 * Logical Address: uint64_t
 * Physical Address: char* 
 */
typedef uint64_t LogicalPtr;
typedef std::vector<LogicalPtr> LptrVec;
const LogicalPtr LOGICAL_NULL_PTR = 0xFFFFFFFFFFFFFFFF;
typedef char* PhysicalPtr;
typedef std::vector<PhysicalPtr> PptrVec;

/*
 * Address Pair
 */
struct AddressPair {
    AddressPair() {
        lptr_ = LOGICAL_NULL_PTR;
        pptr_ = nullptr;
    }

    AddressPair(LogicalPtr lptr, PhysicalPtr pptr)
        : lptr_(lptr), pptr_(pptr) {}

    AddressPair(const AddressPair& other) { *this = other; }

    AddressPair& operator=(const AddressPair& other) {
        lptr_ = other.lptr_;
        pptr_ = other.pptr_;
        return *this;
    }

    inline bool isNull() {
        return lptr_ == LOGICAL_NULL_PTR
            || pptr_ == nullptr;
    }

    LogicalPtr  lptr_;
    PhysicalPtr pptr_;
};

typedef std::vector<AddressPair> AddressVec;

/*
 * degree of parallelism
 */
const int NUM_PAGE_LIST = 16;  // should be even number
const int PAGE_LIST_FACTOR = 4;
const int NUM_VERSION_HASH_SIZE = 128;

const int NUM_LOG_FILE = 8;
const int NUM_ACTIVE_MAP = 1;

/*
 * graph configuration
 */
const int GRAPH_DELTA_INIT_SIZE = 97;
const int GRAPH_MERGE_INTERVAL = 10000;  // 10 sec
const int NUM_EDGES_FOR_SPLIT = 128 * 1024;
const int NUM_EDGES_FOR_MERGE = NUM_EDGES_FOR_SPLIT/2;

/*
 * mask for bit-wise operation
 */
const uint32_t VALID_MASK[] = {
    0x00000001, 0x00000002, 0x00000004, 0x00000008,
    0x00000010, 0x00000020, 0x00000040, 0x00000080,
    0x00000100, 0x00000200, 0x00000400, 0x00000800,
    0x00001000, 0x00002000, 0x00004000, 0x00008000,
    0x00010000, 0x00020000, 0x00040000, 0x00080000,
    0x00100000, 0x00200000, 0x00400000, 0x00800000,
    0x01000000, 0x02000000, 0x04000000, 0x08000000,
    0x10000000, 0x20000000, 0x40000000, 0x80000000
};

#endif  // COMMON_DEF_CONST_H_
