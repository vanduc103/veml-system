// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

// Related include
#include "common/types/element_type.h"

// Project include
#include "storage/catalog.h"
#include "storage/base/paged_array_var.h"

// C & C++ system include
#include <limits>

using namespace storage;

template <class T>
VarSizeElement<T>::VarSizeElement(eval_type val, uint32_t var_pa_id, AddressPair var_slot) {
    if (val.size() >= std::numeric_limits<uint16_t>::max()) {
        std::string err = "too long value @VarSizeElement, ";
        RTI_EXCEPTION(err);
    }

    size_ = val.size();
    flag_ = F_VSLOT;

    memcpy(val_, &var_pa_id, 4);
    memcpy(val_ + 4, &var_slot.lptr_, 8);
    memcpy(var_slot.pptr_, val.ptr(), size_);
}

template <class T>
void VarSizeElement<T>::release(PagedArrayVar* pa) {
    if (flag_ == F_VSLOT) {
       LogicalPtr& lptr = *reinterpret_cast<LogicalPtr*>(val_ + 4);
       pa->erase(lptr);
    }
}

template <class T>
typename VarSizeElement<T>::eval_type VarSizeElement<T>::getValue() const {
    switch (flag_) {
        case F_EMBEDDED:
        {
            return eval_type(val_, size_);
        }
        case F_VSLOT:
        {
            uint32_t var_pa_id = *reinterpret_cast<const uint32_t*>(val_);
            PagedArrayVar* var_pa = reinterpret_cast<PagedArrayVar*>(
                    Metadata::getPagedArray(var_pa_id));

            const LogicalPtr& lptr = *reinterpret_cast<const LogicalPtr*>(val_ + 4);
            AddressPair var_slot = var_pa->getVarSlot(lptr);
            return eval_type(var_slot.pptr_, size_);
        }
        case F_OFFSET:
        {
            uint32_t offset = *reinterpret_cast<const uint32_t*>(val_);
            return eval_type(reinterpret_cast<const char*>(this) + offset, size_);
        }
        default:
        {
            std::string err = "invalid flag @VarSizeElement, ";
            RTI_EXCEPTION(err);
            return eval_type(val_, size_);
        }
    }
}

template <class T>
typename VarSizeElement<T>::stor_type& VarSizeElement<T>::getRefValue() {
    switch (flag_) {
        case F_EMBEDDED:
        {
            stor_type* ref = new stor_type(val_, size_);
            return *ref;
        }
        case F_VSLOT:
        {
            uint32_t var_pa_id = *reinterpret_cast<const uint32_t*>(val_);
            PagedArrayVar* var_pa = reinterpret_cast<PagedArrayVar*>(
                    Metadata::getPagedArray(var_pa_id));

            const LogicalPtr& lptr = *reinterpret_cast<const LogicalPtr*>(val_ + 4);
            AddressPair var_slot = var_pa->getVarSlot(lptr);
            stor_type* ref = new stor_type(var_slot.pptr_, size_);
            return *ref;
        }
        case F_OFFSET:
        {
            uint32_t offset = *reinterpret_cast<const uint32_t*>(val_);
            stor_type* ref = new stor_type(reinterpret_cast<const char*>(this) + offset, size_);
            return *ref;
        }
        default:
        {
            std::string msg = "invalid flag @VarSizeElement, ";
            RTI_EXCEPTION(msg);
            stor_type* ref = new stor_type(val_, size_);
            return *ref;
        }
    }
}

template class VarSizeElement<String>;
