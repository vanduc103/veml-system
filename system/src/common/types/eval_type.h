// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef COMMON_TYPES_EVAL_TYPE_H_
#define COMMON_TYPES_EVAL_TYPE_H_

// Project include
#include "common/types/def_const.h"
#include "common/types/basic/types.h"
#include "common/types/user_defined/types.h"
#include "common/types/hash_util.h"

#include <variant>

enum EvalType {
    ET_BIGINT,
    ET_DOUBLE,
    ET_DECIMAL,
    ET_STRING,
    ET_BOOL,
    ET_DATE,
    ET_TIME,
    ET_TIMESTAMP,
    ET_INTLIST,
    ET_LONGLIST,
    ET_FLOATLIST,
    ET_DOUBLELIST,
    ET_INTVEC,
    ET_LONGVEC,
    ET_FLOATVEC,
    ET_DOUBLEVEC,
    ET_UDT,
    NUM_ET
};

typedef EvalType ET;

template<class T> inline ET evalTypeOf()        { return ET_UDT;}
template<> inline ET evalTypeOf<BigInt>()       { return ET_BIGINT; }
template<> inline ET evalTypeOf<Double>()       { return ET_DOUBLE; }
template<> inline ET evalTypeOf<Decimal>()      { return ET_DECIMAL; }
template<> inline ET evalTypeOf<String>()       { return ET_STRING; }
template<> inline ET evalTypeOf<Bool>()         { return ET_BOOL; }
template<> inline ET evalTypeOf<Date>()         { return ET_DATE; }
template<> inline ET evalTypeOf<Time>()         { return ET_TIME; }
template<> inline ET evalTypeOf<Timestamp>()    { return ET_TIMESTAMP; }
template<> inline ET evalTypeOf<IntList>()      { return ET_INTLIST; }
template<> inline ET evalTypeOf<LongList>()     { return ET_LONGLIST; }
template<> inline ET evalTypeOf<FloatList>()    { return ET_FLOATLIST; }
template<> inline ET evalTypeOf<DoubleList>()   { return ET_DOUBLELIST; }
template<> inline ET evalTypeOf<IntVec>()       { return ET_INTVEC; }
template<> inline ET evalTypeOf<LongVec>()      { return ET_LONGVEC; }
template<> inline ET evalTypeOf<FloatVec>()     { return ET_FLOATVEC; }
template<> inline ET evalTypeOf<DoubleVec>()    { return ET_DOUBLEVEC; }

template<class T>
inline VT valueTypeOf();
template<> inline VT valueTypeOf<BigInt>()      { return VT_BIGINT; }
template<> inline VT valueTypeOf<Integer>()     { return VT_INT; }
template<> inline VT valueTypeOf<Double>()      { return VT_DOUBLE; }
template<> inline VT valueTypeOf<Float>()       { return VT_FLOAT; }
template<> inline VT valueTypeOf<Decimal>()     { return VT_DECIMAL; }
template<> inline VT valueTypeOf<String>()      { return VT_VARCHAR; }
template<> inline VT valueTypeOf<Bool>()        { return VT_BOOL; }
template<> inline VT valueTypeOf<Date>()        { return VT_DATE; }
template<> inline VT valueTypeOf<Time>()        { return VT_TIME; }
template<> inline VT valueTypeOf<Timestamp>()   { return VT_TIMESTAMP; }
template<> inline VT valueTypeOf<IntList>()     { return VT_INTLIST; }
template<> inline VT valueTypeOf<LongList>()    { return VT_LONGLIST; }
template<> inline VT valueTypeOf<FloatList>()   { return VT_FLOATLIST; }
template<> inline VT valueTypeOf<DoubleList>()  { return VT_DOUBLELIST; }

class EvalValue {
  public:
    typedef std::variant<BigInt,
                        Double,
                        Decimal,
                        String,
                        Bool,
                        Date,
                        Time,
                        Timestamp,
                        PhysicalPtr,
                        IntList,
                        LongList,
                        FloatList,
                        DoubleList,
                        IntVec,
                        LongVec,
                        FloatVec,
                        DoubleVec> Var;
    /**
     * constructors
     */
    EvalValue() : data(BigInt(0)) { type=NUM_ET;}
    EvalValue(const BigInt& v) : data(v) { type=ET_BIGINT; }
    EvalValue(const Double& v) : data(v) { type=ET_DOUBLE; }
    EvalValue(const Decimal& v) : data(v) { type=ET_DOUBLE; }
    EvalValue(const String& v) : data(v) { type=ET_STRING; }
    EvalValue(const Bool& v) : data(v) { type=ET_BOOL; }
    EvalValue(const Time& v) : data(v) { type=ET_TIME; }
    EvalValue(const Date& v) : data(v) { type=ET_DATE; }
    EvalValue(const Timestamp& v) : data(v)   { type=ET_TIMESTAMP; }
    EvalValue(const PhysicalPtr& v) : data(v) { type=ET_UDT; }
    EvalValue(const IntList& v) : data((PhysicalPtr)&v) { type=ET_INTLIST; }
    EvalValue(const LongList& v) : data((PhysicalPtr)&v) { type=ET_LONGLIST; }
    EvalValue(const FloatList& v) : data((PhysicalPtr)&v) { type=ET_FLOATLIST; }
    EvalValue(const DoubleList& v) : data((PhysicalPtr)&v) { type=ET_DOUBLELIST; }
    EvalValue(const IntVec& v) : data(v) { type=ET_INTVEC; }
    EvalValue(const LongVec& v) : data(v) { type=ET_LONGVEC; }
    EvalValue(const FloatVec& v) : data(v) { type=ET_FLOATVEC; }
    EvalValue(const DoubleVec& v) : data(v) { type=ET_DOUBLEVEC; }

    static EvalValue gen_bigint(const BigInt& v)       { return EvalValue(v); }
    static EvalValue gen_double(const Double& v)       { return EvalValue(v); }
    static EvalValue gen_decimal(const Decimal& v)     { return EvalValue(v); }
    static EvalValue gen_string(const String& v)       { return EvalValue(v); }
    static EvalValue gen_bool(const Bool& v)           { return EvalValue(v); }
    static EvalValue gen_time(const Time& v)           { return EvalValue(v); }
    static EvalValue gen_date(const Date& v)           { return EvalValue(v); }
    static EvalValue gen_timestamp(const Timestamp& v) { return EvalValue(v); }
    static EvalValue gen_pptr(const PhysicalPtr& v)    { return EvalValue(v); }
    static EvalValue gen_ilist(const IntList& v)       { return EvalValue(v); }
    static EvalValue gen_llist(const LongList& v)      { return EvalValue(v); }
    static EvalValue gen_flist(const FloatList& v)     { return EvalValue(v); }
    static EvalValue gen_dlist(const DoubleList& v)    { return EvalValue(v); }
    static EvalValue gen_ivec(const IntVec& v)         { return EvalValue(v); }
    static EvalValue gen_lvec(const LongVec& v)        { return EvalValue(v); }
    static EvalValue gen_fvec(const FloatVec& v)       { return EvalValue(v); }
    static EvalValue gen_dvec(const DoubleVec& v)      { return EvalValue(v); }

    EvalValue(const EvalValue& other) : data(other.data), type(other.type) {}
    EvalValue& operator=(const EvalValue& other) {
        type = other.type;
        data = other.data;
        return *this;
    }

    /**
     * getter
     */
    template <typename T>
    T& get() {
        return std::get<T>(data);
    }

    BigInt& getBigInt()     { return get<BigInt>(); }
    Double& getDouble()     { return get<Double>(); }
    Decimal& getDecimal()   { return get<Decimal>(); }
    String& getString()     { return get<String>(); }
    Bool& getBool()         { return get<Bool>(); }
    Date& getDate()         { return get<Date>(); }
    Time& getTime()         { return get<Time>(); }
    Timestamp& getTS()      { return get<Timestamp>(); }
    PhysicalPtr& getPtr()   { return get<PhysicalPtr>(); }
    PhysicalPtr getRefPtr() { return reinterpret_cast<PhysicalPtr>(&data); }
    IntList& getIntList() {
        return *reinterpret_cast<IntList*>(get<PhysicalPtr>());
    }
    LongList& getLongList() {
        return *reinterpret_cast<LongList*>(get<PhysicalPtr>());
    }
    FloatList& getFloatList() {
        return *reinterpret_cast<FloatList*>(get<PhysicalPtr>());
    }
    DoubleList& getDoubleList() {
        return *reinterpret_cast<DoubleList*>(get<PhysicalPtr>());
    }
    IntVec& getIntVec()      { return get<IntVec>(); }
    LongVec& getLongVec()     { return get<LongVec>(); }
    FloatVec& getFloatVec()   { return get<FloatVec>(); }
    DoubleVec& getDoubleVec() { return get<DoubleVec>(); }

    /**
     * const getter
     */
    template <typename T>
    const T& get() const {
        return std::get<T>(data);
    }

    const BigInt& getBigIntConst() const   { return get<BigInt>(); }
    const Double& getDoubleConst() const   { return get<Double>(); }
    const Decimal& getDecimalConst() const { return get<Decimal>(); }
    const String& getStringConst() const   { return get<String>(); }
    const Bool& getBoolConst() const       { return get<Bool>(); }
    const Date& getDateConst() const       { return get<Date>(); }
    const Time& getTimeConst() const       { return get<Time>(); }
    const Timestamp& getTSConst() const    { return get<Timestamp>(); }
    const PhysicalPtr& getPtrConst() const { return get<PhysicalPtr>(); }
    const IntList& getIntListConst() const {
        return *reinterpret_cast<IntList*>(get<PhysicalPtr>());
    }
    const LongList& getLongListConst() const {
        return *reinterpret_cast<LongList*>(get<PhysicalPtr>());
    }
    const FloatList& getFloatListConst() const {
        return *reinterpret_cast<FloatList*>(get<PhysicalPtr>());
    }
    const DoubleList& getDoubleListConst() const {
        return *reinterpret_cast<DoubleList*>(get<PhysicalPtr>());
    }
    const IntVec& getIntVecConst() const       { return get<IntVec>(); }
    const LongVec& getLongVecConst() const     { return get<LongVec>(); }
    const FloatVec& getFloatVecConst() const   { return get<FloatVec>(); }
    const DoubleVec& getDoubleVecConst() const { return get<DoubleVec>(); }

    EvalType getEvalType() const { return type; }
    void setEvalType(EvalType et) { type = et; }

    static const std::string ns_err;

    EvalValue operator+(const EvalValue& other) const {
        assert(type == other.type);
        switch (type) {
            case ET_BIGINT:
                return std::get<BigInt>(data) + std::get<BigInt>(other.data);
            case ET_DOUBLE:
                return std::get<Double>(data) + std::get<Double>(other.data);
            case ET_DECIMAL:
                return std::get<Decimal>(data) + std::get<Decimal>(other.data);
            default: {
                std::string msg = ns_err + "@EvalValue::operator+ ";
                RTI_EXCEPTION(msg);
                return EvalValue();
            }
        }
    }

    EvalValue operator-(const EvalValue& other) const {
        assert(type == other.type);
        switch (type) {
            case ET_BIGINT:
                return std::get<BigInt>(data) - std::get<BigInt>(other.data);
            case ET_DOUBLE:
                return std::get<Double>(data) - std::get<Double>(other.data);
            case ET_DECIMAL:
                return std::get<Decimal>(data) - std::get<Decimal>(other.data);
            default: {
                std::string msg = ns_err + "@EvalValue::operator- ";
                RTI_EXCEPTION(msg);
                return EvalValue();
            }
        }
    }

    EvalValue operator*(const EvalValue& other) const {
        assert(type == other.type);
        switch (type) {
            case ET_BIGINT:
                return std::get<BigInt>(data) * std::get<BigInt>(other.data);
            case ET_DOUBLE:
                return std::get<Double>(data) * std::get<Double>(other.data);
            case ET_DECIMAL:
                return std::get<Decimal>(data) * std::get<Decimal>(other.data);
            default: {
                std::string msg = ns_err + "@EvalValue::operator* ";
                RTI_EXCEPTION(msg);
                return EvalValue();
            }
        }
    }

    EvalValue operator/(const EvalValue& other) const {
        assert(type == other.type);
        switch (type) {
            case ET_BIGINT:
                return std::get<BigInt>(data) / std::get<BigInt>(other.data);
            case ET_DOUBLE:
                return std::get<Double>(data) / std::get<Double>(other.data);
            case ET_DECIMAL:
                return std::get<Decimal>(data) / std::get<Decimal>(other.data);
            default: {
                std::string msg = ns_err + "@EvalValue::operator/ ";
                RTI_EXCEPTION(msg);
                return EvalValue();
            }
        }
    }

    static EvalValue max(const EvalValue& l, const EvalValue& r) {
        if (l < r) return r;
        else return l;
    }

    static EvalValue min(const EvalValue& l, const EvalValue& r) {
        if (l < r) return l;
        else return r;
    }

    bool operator==(const EvalValue& other) const {
        switch (type) {
            case ET_BIGINT:
                return std::get<BigInt>(data) == std::get<BigInt>(other.data);
            case ET_DOUBLE:
                return std::get<Double>(data) == std::get<Double>(other.data);
            case ET_DECIMAL:
                return std::get<Decimal>(data) == std::get<Decimal>(other.data);
            case ET_STRING:
                return std::get<String>(data) == std::get<String>(other.data);
            case ET_BOOL:
                return std::get<Bool>(data) == std::get<Bool>(other.data);
            case ET_DATE:
                return std::get<Date>(data) == std::get<Date>(other.data);
            case ET_TIME:
                return std::get<Time>(data) == std::get<Time>(other.data);
            case ET_TIMESTAMP:
                return std::get<Timestamp>(data)
                            == std::get<Timestamp>(other.data);
            default: {
                std::string msg = ns_err + "@EvalValue::operator==, ";
                RTI_EXCEPTION(msg);
                return false;
            }
        }
    }

    bool operator!=(const EvalValue& other) const {
        return !(*this == other);
    }

    bool operator<(const EvalValue& other) const {
        switch (type) {
            case ET_BIGINT:
                return std::get<BigInt>(data) < std::get<BigInt>(other.data);
            case ET_DOUBLE:
                return std::get<Double>(data) < std::get<Double>(other.data);
            case ET_DECIMAL:
                return std::get<Decimal>(data) < std::get<Decimal>(other.data);
            case ET_STRING:
                return std::get<String>(data) < std::get<String>(other.data);
            case ET_BOOL:
                return std::get<Bool>(data) < std::get<Bool>(other.data);
            case ET_DATE:
                return std::get<Date>(data) < std::get<Date>(other.data);
            case ET_TIME:
                return std::get<Time>(data) < std::get<Time>(other.data);
            case ET_TIMESTAMP:
                return std::get<Timestamp>(data)
                            < std::get<Timestamp>(other.data);
            default: {
                std::string msg = ns_err + "@EvalValue::operator<, ";
                RTI_EXCEPTION(msg);
                return false;
            }
        }
    }

    bool operator<=(const EvalValue& other) const {
        return !((*this) > other);
    }

    bool operator>(const EvalValue& other) const {
        switch (type) {
            case ET_BIGINT:
                return std::get<BigInt>(data) > std::get<BigInt>(other.data);
            case ET_DOUBLE:
                return std::get<Double>(data) > std::get<Double>(other.data);
            case ET_DECIMAL:
                return std::get<Decimal>(data) > std::get<Decimal>(other.data);
            case ET_STRING:
                return std::get<String>(data) > std::get<String>(other.data);
            case ET_BOOL:
                return std::get<Bool>(data) > std::get<Bool>(other.data);
            case ET_DATE:
                return std::get<Date>(data) > std::get<Date>(other.data);
            case ET_TIME:
                return std::get<Time>(data) > std::get<Time>(other.data);
            case ET_TIMESTAMP:
                return std::get<Timestamp>(data)
                            > std::get<Timestamp>(other.data);
            default: {
                std::string msg = ns_err + "@EvalValue::operator>, ";
                RTI_EXCEPTION(msg);
                return false;
            }
        }
    }

    bool operator>=(const EvalValue& other) const {
        return !((*this) < other);
    }

    struct equal_to {
        inline bool operator() (const EvalValue& l, const EvalValue& r) const
        { return l == r; }
    };

    struct not_equal_to {
        inline bool operator() (const EvalValue& l, const EvalValue& r) const
        { return l != r; }
    };

    struct greater {
        inline bool operator() (const EvalValue& l, const EvalValue& r) const
        { return l > r; }
    };

    struct greater_equal {
        inline bool operator() (const EvalValue& l, const EvalValue& r) const
        { return l >= r; }
    };

    struct less {
        inline bool operator() (const EvalValue& l, const EvalValue& r) const
        { return l < r; }
    };

    struct less_equal {
        inline bool operator() (const EvalValue& l, const EvalValue& r) const
        { return l <= r; }
    };

    uint32_t hash(uint32_t seed) const {
        switch (type) {
            case ET_BIGINT:
                return HashUtil::hash(
                        &std::get<BigInt>(data), sizeof(BigInt), seed);
            case ET_DOUBLE:
                return HashUtil::hash(
                        &std::get<Double>(data), sizeof(Double), seed);
            case ET_DECIMAL:
                return HashUtil::hash(
                        &std::get<Decimal>(data), sizeof(Decimal), seed);
            case ET_STRING: {
                String s = std::get<String>(data);
                return HashUtil::hash(s.ptr(), s.size(), seed);
            }
            case ET_BOOL:
                return HashUtil::hash(
                        &std::get<Bool>(data), sizeof(Bool), seed);
            case ET_DATE:
                return HashUtil::hash(
                        &std::get<Date>(data), sizeof(Date), seed);
            case ET_TIME:
                return HashUtil::hash(
                        &std::get<Time>(data), sizeof(Time), seed);
            case ET_TIMESTAMP:
                return HashUtil::hash(
                        &std::get<Timestamp>(data), sizeof(Timestamp), seed);
            default: {
                std::string msg = ns_err + "@EvalValue::hash, ";
                RTI_EXCEPTION(msg);
            }
        }
        return 0;
    }

    struct EvalHash {
		inline std::size_t operator()(const EvalValue& v) const {
			return v.hash(1);
		}
	};

    struct EvalVecHash {
		inline std::size_t operator()(const std::vector<EvalValue>& values) const {
            uint32_t hash_key = 1;
            for (auto& v : values) {
                hash_key = v.hash(hash_key);
            }
            return hash_key;
		}
    };

    std::string to_string() const;
    
  private:
    std::variant<BigInt,
                Double,
                Decimal,
                String,
                Bool,
                Date,
                Time,
                Timestamp,
                PhysicalPtr,
                IntVec,
                LongVec,
                FloatVec,
                DoubleVec> data;
    EvalType type;
};

typedef std::vector<EvalValue> EvalVec;

inline std::ostream& operator<<(std::ostream &os, const EvalValue& v) {
    switch (v.getEvalType()) {
        case ET_BIGINT:
            os << v.getBigIntConst();
            break;
        case ET_DOUBLE:
            os << v.getDoubleConst();
            break;
        case ET_DECIMAL:
            os << v.getDecimalConst();
            break;
        case ET_STRING:
            os << v.getStringConst();
            break;
        case ET_DATE:
            os << v.getDateConst();
            break;
        case ET_TIME:
            os << v.getTimeConst();
            break;
        case ET_TIMESTAMP:
            os << v.getTSConst();
            break;
        default: {
            std::string msg = EvalValue::ns_err + "@EvalValue::operator<<, ";
            RTI_EXCEPTION(msg);
        }
    }
    return os;
}

inline EvalType convertToEvalType(ValueType vt) {
    switch(vt) {
        case VT_BIGINT:
        case VT_INT:
            return ET_BIGINT;
        case VT_DOUBLE:
        case VT_FLOAT:
            return ET_DOUBLE;
        case VT_DECIMAL:
            return ET_DECIMAL;
        case VT_VARCHAR:
        case VT_CHAR:
            return ET_STRING;
        case VT_BOOL:
            return ET_BOOL;
        case VT_DATE:
            return ET_DATE;
        case VT_TIME:
            return ET_TIME;
        case VT_TIMESTAMP:
            return ET_TIMESTAMP;
        case VT_INTLIST:
            return ET_INTLIST;
        case VT_LONGLIST:
            return ET_LONGLIST;
        case VT_FLOATLIST:
            return ET_FLOATLIST;
        case VT_DOUBLELIST:
            return ET_DOUBLELIST;
        case VT_UDT:
            return ET_UDT;
        default:
            std::string msg = EvalValue::ns_err + "@EvalValue::convertToEvalType, ";
            RTI_EXCEPTION(msg);
            return NUM_ET;
    }
}

template <> IntList& EvalValue::get();
template <> LongList& EvalValue::get();
template <> FloatList& EvalValue::get();
template <> DoubleList& EvalValue::get();
template <> const IntList& EvalValue::get() const;
template <> const LongList& EvalValue::get() const;
template <> const FloatList& EvalValue::get() const;
template <> const DoubleList& EvalValue::get() const;

#endif  // COMMON_TYPES_EVAL_TYPE_H_
