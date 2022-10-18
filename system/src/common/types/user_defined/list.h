// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef COMMON_TYPES_USER_DEFINED_LIST_H_
#define COMMON_TYPES_USER_DEFINED_LIST_H_

// Project include
#include "common/def_const.h"
#include "common/types/def_const.h"
#include "concurrency/mutex_lock.h"

// C & C++ system inclue
#include <atomic>

//#include <torch/torch.h>

class EvalValue;

namespace storage {
    class PagedArrayVar;
}

class ListWrapper {
  public:
    static uint32_t createVarPA();
    static storage::PagedArrayVar* getVarPA(uint32_t var_id);
    static AddressPair getVarSlot(
            storage::PagedArrayVar* pa, LogicalPtr lptr);
    static AddressPair allocateVarSlot(
            storage::PagedArrayVar* pa, uint32_t size);
    static void erase(
            storage::PagedArrayVar* pa, LogicalPtr lptr);
};

template <class T>
class List {
  public:
    static const uint32_t INIT_CAPACITY = 4;
    static const uint16_t ITEM_SIZE = sizeof(T);
    static const uint32_t PAGE_SIZE = 4 * 1024;
    static const uint32_t HALF_SIZE = (PAGE_SIZE/ITEM_SIZE)/2;
    static const uint32_t ITEM_PER_PAGE
                            = (PAGE_SIZE - sizeof(LogicalPtr))/ITEM_SIZE;
    static const uint16_t SLOT_SIZE
                            = INIT_CAPACITY * ITEM_SIZE + sizeof(uint32_t);

    // constructors
    List();
    List(const List<T>& other);
    List(const List<T>& other, uint32_t id);
    List(const EvalValue& other);
    List(const EvalValue& other, uint32_t id);
    List& operator=(const List& other);

    // modifiers
    uint32_t push_back(T v);
    void insert(const std::vector<T>& vec);
    void release();

    // accessors
    T& operator[](uint32_t idx);
    void copyTo(std::vector<T>& to);

    // getters
    uint32_t size() const;
    uint32_t capacity() const;

    List& get()      { return *this; }
    List& getEval()  { return *this; }
    List& getValue() { return *this; }

    T* begin();
    T* end();

    std::string to_string() const;

    //torch::Tensor toTensor();

    // null api for paged_array operation
    bool operator==(const List<T>& other) { return false; }
    bool operator< (const List<T>& other) { return false; }
    bool operator> (const List<T>& other) { return false; }
    bool operator<=(const List<T>& other) { return false; }
    bool operator>=(const List<T>& other) { return false; }

  private:
    void resize(uint32_t size);
    void collect(std::vector<T>& vec) const;

    PhysicalPtr getSlot(uint32_t idx) const;
    LogicalPtr& getHead();
    LogicalPtr& getTail();
    LogicalPtr  getHeadConst() const;
    LogicalPtr  getTailConst() const;

    uint32_t& var_id();
    uint32_t var_id_const() const;

    char val_[SLOT_SIZE];
    std::atomic<uint32_t> size_;
    std::atomic<uint32_t> capacity_;
    Mutex mx_resize_;
};

#include "common/types/user_defined/list.inl"

#endif  // COMMON_TYPES_USER_DEFINED_LIST_H_
