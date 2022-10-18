// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef COMMON_TYPES_ELEMENT_TYPE_H_
#define COMMON_TYPES_ELEMENT_TYPE_H_

// Project include
#include "common/types/eval_type.h"
#include "common/def_const.h"

namespace storage {
    class PagedArrayVar;
}

/*
 * for fixed size element
 */
template <class T, class EvalType = T>
class FixedSizeElement {
  public:
    typedef T           stor_type;
    typedef EvalType    eval_type;

    FixedSizeElement(eval_type val) : val_(val.get()) {}

    inline EvalValue    getEval()       { return EvalValue(val_.getEval()); }
    inline eval_type    getValue()      { return val_.getValue(); }
    inline stor_type&   getRefValue()   { return val_; }

    static ValueType    getValueType()  { return valueTypeOf<stor_type>(); }

  private:
    stor_type           val_;
};

/*
 * for variable size element
 */
template <class T>
class VarSizeElement {
  public:
    typedef T stor_type;
    typedef T eval_type;

    /*
     * constructors for embedded
     */
    VarSizeElement(eval_type val, uint16_t size) : size_(size), flag_(F_EMBEDDED) {
        assert(size_ <= MAX_EMBEDDED_SIZE);
        memcpy(val_, val.ptr(), size_);
    }
    VarSizeElement(const EvalValue& val, uint16_t size)
        : VarSizeElement(val.get<eval_type>(), size) {}

    /*
     * constructors for offset
     */
    VarSizeElement(eval_type val, uint16_t size, uint32_t offset)
        : size_(size), flag_(F_OFFSET) {
        assert(size_ > MAX_EMBEDDED_SIZE);

        *reinterpret_cast<uint32_t*>(val_) = offset;

        PhysicalPtr vslot = reinterpret_cast<PhysicalPtr>(this) + offset;
        memcpy(vslot, val.ptr(), size_);
    }
    VarSizeElement(const EvalValue& val, uint16_t size, uint32_t offset)
        : VarSizeElement(val.get<eval_type>(), size, offset) {}

    /*
     * constructors for var-slot
     */
    VarSizeElement(eval_type val, uint32_t var_pa_id, AddressPair var_slot);
    VarSizeElement(const EvalValue& val, uint32_t var_pa_id, AddressPair var_slot)
        : VarSizeElement(val.get<eval_type>(), var_pa_id, var_slot) {}

    void release(storage::PagedArrayVar* pa);

    /*
     * getters
     */
    EvalValue           getEval() const    { return EvalValue(getValue()); }
    eval_type           getValue() const;
    stor_type&          getRefValue();
    static ValueType    getValueType()     { return VT_VARCHAR; }

  private:
    uint16_t    size_;
    char        val_[MAX_EMBEDDED_SIZE];
    enum : int8_t {
        F_EMBEDDED,
        F_VSLOT,
        F_OFFSET
    } flag_;
};

/*
 * for embedded element
 */
template <class T>
class EmbeddedElement {
  public:
    typedef T stor_type;
    typedef T eval_type;

    EmbeddedElement(eval_type val) : size_(val.size()) {
        assert(val.size() <= 510 && "too long value");
        memcpy(val_, val.ptr(), size_);
    }
    EmbeddedElement(eval_type val, uint32_t type_size) : size_(val.size()) {
        assert(size_ <= type_size);
        assert(val.size() <= 510 && "too long value");
        memcpy(val_, val.ptr(), size_);
    }
    EmbeddedElement(const EvalValue& val, uint32_t type_size)
        : EmbeddedElement(val.get<eval_type>(), type_size) {}

    inline eval_type getEval() const    { return eval_type(val_, size_); }
    inline eval_type getValue() const   { return eval_type(val_, size_); }
    static ValueType getValueType()     { return VT_CHAR; }

  private:
    uint16_t     size_;
    char        val_[510];
};

#endif  // COMMON_TYPES_ELEMENT_TYPE_H_
