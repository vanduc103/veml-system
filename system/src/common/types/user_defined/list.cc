// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

// Related include
#include "common/types/user_defined/list.h"

// Project include
#include "storage/catalog.h"
#include "storage/base/paged_array_var.h"

//#include <torch/torch.h>

using namespace storage;

uint32_t ListWrapper::createVarPA() {
    PagedArrayVar* pa = new PagedArrayVar();
    return Metadata::addNewPagedArray(pa);
}

PagedArrayVar* ListWrapper::getVarPA(uint32_t var_id) {
        return reinterpret_cast<PagedArrayVar*>(
                Metadata::getPagedArray(var_id));
}

AddressPair ListWrapper::getVarSlot(PagedArrayVar* pa, LogicalPtr lptr) {
    return pa->getVarSlot(lptr);
}

AddressPair ListWrapper::allocateVarSlot(PagedArrayVar* pa, uint32_t size) {
    return pa->allocateVarSlot(size);
}

void ListWrapper::erase(PagedArrayVar* pa, LogicalPtr lptr) {
    pa->erase(lptr);
}

template <>
List<int32_t>::List(const EvalValue& other)
    : List(other.getIntListConst()) {}

template <>
List<int32_t>::List(const EvalValue& other, uint32_t id)
    : List(other.getIntListConst(), id) {}

template <>
List<int64_t>::List(const EvalValue& other)
    : List(other.getLongListConst()) {}

template <>
List<int64_t>::List(const EvalValue& other, uint32_t id)
    : List(other.getLongListConst(), id) {}

template <>
List<float>::List(const EvalValue& other)
    : List(other.getFloatListConst()) {}
template <>
List<float>::List(const EvalValue& other, uint32_t id)
    : List(other.getFloatListConst(), id) {}

template <>
List<double>::List(const EvalValue& other)
    : List(other.getDoubleListConst()) {}
template <>
List<double>::List(const EvalValue& other, uint32_t id)
    : List(other.getDoubleListConst(), id) {}
