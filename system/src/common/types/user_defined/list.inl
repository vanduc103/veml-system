// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

template <class T>
inline List<T>::List() : size_(0), capacity_(INIT_CAPACITY) {
    memset(val_, 0xff, SLOT_SIZE);
}

template <class T>
inline List<T>::List(const List<T>& other) {
    size_ = other.size_.load();
    capacity_ = other.capacity_.load();
    memcpy(val_, other.val_, SLOT_SIZE);
}

template <class T>
inline List<T>::List(const List<T>& other, uint32_t id) {
    memset(val_, 0xff, SLOT_SIZE);
    var_id() = id;
    size_.store(other.size_);
    capacity_.store(other.capacity_);

    if (capacity_ >= ITEM_PER_PAGE) {
        // copy all varpage-list
        storage::PagedArrayVar* pa
            = ListWrapper::getVarPA(var_id());

        // start from head page
        LogicalPtr* ptr = &getHead();
        for (uint32_t idx = 0; idx < capacity_; idx+=ITEM_PER_PAGE) {
            // allocate new varpage
            AddressPair vslot = ListWrapper::allocateVarSlot(pa, PAGE_SIZE);

            // link new varpage to varpage-list
            *ptr = vslot.lptr_;
            
            // assign slot for pointer to next page
            ptr = reinterpret_cast<LogicalPtr*>(vslot.pptr_);
            *ptr = LOGICAL_NULL_PTR;

            // copy elements
            memcpy(vslot.pptr_+sizeof(LogicalPtr),
                    other.getSlot(idx), ITEM_PER_PAGE * ITEM_SIZE);

            // set current varpage as tail of varpage-list
            getTail() = vslot.lptr_;
        }
    } else if (capacity_ > INIT_CAPACITY) {
        // copy a var slot
        storage::PagedArrayVar* pa = ListWrapper::getVarPA(var_id());
        AddressPair vslot = ListWrapper::allocateVarSlot(
                                    pa, capacity_ * ITEM_SIZE);
        memcpy(vslot.pptr_, other.getSlot(0), capacity_ *ITEM_SIZE);

        getHead() = vslot.lptr_;
        getTail() = vslot.lptr_;
    } else if (size_ > 0) {
        memcpy(val_, other.val_, SLOT_SIZE);
        var_id() = id;
    }
}

template <class  T>
inline List<T>& List<T>::operator=(const List<T>& other) {
    size_ = other.size_.load();
    capacity_ = other.capacity_.load();
    memcpy(val_, other.val_, SLOT_SIZE);

    return *this;
}

template <class T>
inline uint32_t List<T>::push_back(T v) {
    mx_resize_.shared_lock();
    uint32_t idx = size_.fetch_add(1);
    uint32_t cap = capacity_.load();

    if (idx == cap) {
        mx_resize_.upgrade_lock();
        resize(idx);
        mx_resize_.downgrade_lock();
    } else {
        mx_resize_.shared_unlock();
        while (idx >= capacity_.load()) {
            // wait for resize
            __SPIN;
        }
        mx_resize_.shared_lock();
    }

    (*this)[idx] = v;

    mx_resize_.shared_unlock();

    return idx;
}

template <class T>
inline void List<T>::insert(const std::vector<T>& vec) {
    size_t size = vec.size();
    if (size <= INIT_CAPACITY) {
        for (auto v : vec) {
            this->push_back(v);
        }
    } else if (size <= HALF_SIZE) {
        storage::PagedArrayVar* pa = ListWrapper::getVarPA(var_id());
        // allocate var slot
        uint32_t capacity = INIT_CAPACITY;
        for ( ; size > capacity; capacity*=2) {}
        AddressPair vslot
            = ListWrapper::allocateVarSlot(pa, capacity * ITEM_SIZE);

        // set var slot
        getHead() = vslot.lptr_;
        getTail() = vslot.lptr_;
        
        // copy elements
        memcpy(vslot.pptr_, vec.data(), size * ITEM_SIZE);

        // update size/capacity
        size_.fetch_add(size);
        capacity_.store(capacity);
    } else {
        storage::PagedArrayVar* pa = ListWrapper::getVarPA(var_id());
        size_t allocated = 0;
        capacity_.store(0);
        while (allocated < size) {
            // allocate new page
            AddressPair vslot = ListWrapper::allocateVarSlot(pa, PAGE_SIZE);
            *reinterpret_cast<LogicalPtr*>(vslot.pptr_) = LOGICAL_NULL_PTR;

            // link varpage list
            if (allocated == 0) {
                getHead() = vslot.lptr_;
                getTail() = vslot.lptr_;
            } else {
                AddressPair old_tail = ListWrapper::getVarSlot(pa, getTail());
                *reinterpret_cast<LogicalPtr*>(old_tail.pptr_) = vslot.lptr_;
                getTail() = vslot.lptr_;
            }

            // copy elements
            size_t new_items = (size-allocated) < ITEM_PER_PAGE?
                                    (size-allocated) : ITEM_PER_PAGE;
            memcpy(vslot.pptr_ + sizeof(LogicalPtr),
                    vec.data()+allocated, new_items * ITEM_SIZE);

            // update size/capacity
            size_.fetch_add(new_items);
            capacity_.fetch_add(ITEM_PER_PAGE);
            allocated += new_items;
        }
    }
}

template <class T>
inline void List<T>::release() {
    // nothing to do
    if (var_id() == 0xffffffff || capacity_.load() == INIT_CAPACITY) {
        return;
    }

    storage::PagedArrayVar* pa = ListWrapper::getVarPA(var_id());
    if (capacity_.load() < ITEM_PER_PAGE) {
        ListWrapper::erase(pa, getHead());
    } else {
        LogicalPtr lptr = getHead();
        while (lptr != LOGICAL_NULL_PTR) {
            AddressPair slot = ListWrapper::getVarSlot(pa, lptr);
            lptr = *reinterpret_cast<LogicalPtr*>(slot.pptr_);
            ListWrapper::erase(pa, slot.lptr_);
        }
    }
}

template <class T>
inline T& List<T>::operator[](uint32_t idx) {
    ReadLock read_loack(mx_resize_);
    return *reinterpret_cast<T*>(getSlot(idx));
}

template <class T>
inline void List<T>::copyTo(std::vector<T>& to) {
    ReadLock read_loack(mx_resize_);
    collect(to);
}

template <class T>
inline uint32_t List<T>::size() const {
    return size_.load();
}

template <class T>
inline uint32_t List<T>::capacity() const {
    return capacity_.load();
}

template <class T>
inline T* List<T>::begin() {
    reinterpret_cast<T*>(this->getSlot(0));
}

template <class T>
inline T* List<T>::end() {
    reinterpret_cast<T*>(this->getSlot(size_.load()));
}

template <class T>
inline std::string List<T>::to_string() const {
    std::vector<T> vec;
    collect(vec);

    std::stringstream ss;
    ss << "[";
    for (auto v : vec) {
        ss << v << ", ";
    }
    ss << "]";

    return ss.str();
}

/**
template <class T>
inline torch::Tensor List<T>::toTensor() {
    ReadLock read_loack(mx_resize_);
    uint32_t size = size_.load();
    uint32_t capacity = capacity_.load();

    torch::TensorOptions options;
    if (std::is_same<T, int32_t>::value) {
        options = torch::TensorOptions().dtype(torch::kInt32);
    } else if (std::is_same<T, int64_t>::value) {
        options = torch::TensorOptions().dtype(torch::kInt64);
    } else if (std::is_same<T, float>::value) {
        options = torch::TensorOptions().dtype(torch::kFloat32);
    } else if (std::is_same<T, double>::value) {
        options = torch::TensorOptions().dtype(torch::kFloat64);
    }

    if (size == 0) return torch::empty({0,0}, options);
    if (capacity >= ITEM_PER_PAGE) {
        std::vector<torch::Tensor> tensors;
        for (size_t idx = 0; idx < capacity; idx += ITEM_PER_PAGE) {
            size_t upperend = size < idx + ITEM_PER_PAGE?
                                size : idx + ITEM_PER_PAGE;
            T* start = (T*)(this->getSlot(idx));
            T* end = (T*)(this->getSlot(upperend-1));
            uint32_t dim = end - start + 1;
            torch::Tensor t = torch::from_blob(start, {dim}, options);
            tensors.push_back(t);
        }
        if(tensors.size() == 1){
            return tensors[0];
        }
        return at::cat(at::TensorList(tensors), 0);
    }
    return torch::from_blob((T*)(this->getSlot(0)), {size}, options);
}*/

template <class T>
inline void List<T>::resize(uint32_t size) {
    uint32_t new_size = 0;
    if (var_id() == 0xffffffff) {
        // if has no PAVar, generate one
        var_id() = ListWrapper::createVarPA();
    }
    storage::PagedArrayVar* pa = ListWrapper::getVarPA(var_id());

    if (size >= HALF_SIZE) {
        // allocate new page
        AddressPair vslot = ListWrapper::allocateVarSlot(
                                    pa, PAGE_SIZE);
        *reinterpret_cast<LogicalPtr*>(vslot.pptr_) = LOGICAL_NULL_PTR;

        if (size == HALF_SIZE) {
            // copy elements and erase previous varslot
            memcpy(vslot.pptr_ + sizeof(LogicalPtr),
                    getSlot(0), HALF_SIZE * ITEM_SIZE);
            ListWrapper::erase(pa, getHead());

            // set new varpage
            getHead() = vslot.lptr_;
            getTail() = vslot.lptr_;
            new_size = ITEM_PER_PAGE;
        } else {
            // link varpage list
            AddressPair old_tail = ListWrapper::getVarSlot(pa, getTail());
            *reinterpret_cast<LogicalPtr*>(old_tail.pptr_) = vslot.lptr_;

            // set new var page
            getTail() = vslot.lptr_;
            new_size = size + ITEM_PER_PAGE;
        }
    } else {
        if (var_id() == 0xffffffff) {
            // if has no PAVar, generate one
            var_id() = ListWrapper::createVarPA();
        }

        // allocate new varslot
        new_size = size << 1;
        AddressPair vslot = ListWrapper::allocateVarSlot(pa, new_size*ITEM_SIZE);

        // copy elements and erase previous varslot
        memcpy(vslot.pptr_, getSlot(0), size * ITEM_SIZE);
        if (size != INIT_CAPACITY)
            ListWrapper::erase(pa, getHead());

        // set new varslot
        getHead() = vslot.lptr_;
        getTail() = vslot.lptr_;
    }

    capacity_.store(new_size);
}

template <class T>
inline void List<T>::collect(std::vector<T>& vec) const {
    uint32_t size = size_.load();
    uint32_t capacity = capacity_.load();

    if (size == 0) return;

    vec.reserve(vec.size() + size);
    if (capacity >= ITEM_PER_PAGE) {
        for (size_t idx = 0; idx < capacity; idx += ITEM_PER_PAGE) {
            size_t upperend = size < idx + ITEM_PER_PAGE?
                                size : idx + ITEM_PER_PAGE;
            std::copy((T*)(this->getSlot(idx)),
                    (T*)(this->getSlot(upperend-1)) + 1,
                    std::back_inserter(vec));
        }
    } else {
        vec.insert(vec.end(),
                (T*)(this->getSlot(0)),
                (T*)(this->getSlot(size)));
    }
}

template <class T>
inline PhysicalPtr List<T>::getSlot(uint32_t idx) const {
    if (capacity_.load() == INIT_CAPACITY) {
        return (const PhysicalPtr)
            (val_ + sizeof(uint32_t) + idx * ITEM_SIZE);
    } else if (capacity_.load() <= HALF_SIZE) {
        storage::PagedArrayVar* pa = ListWrapper::getVarPA(var_id_const());
        AddressPair vslot = ListWrapper::getVarSlot(pa, getHeadConst());
        return (const PhysicalPtr)(vslot.pptr_ + idx * ITEM_SIZE);
    } else {
        int page_idx = idx / ITEM_PER_PAGE;
        idx %= ITEM_PER_PAGE;

        storage::PagedArrayVar* pa = ListWrapper::getVarPA(var_id_const());

        AddressPair vslot(getHeadConst(), nullptr);
        while (page_idx >= 0 && vslot.lptr_ != LOGICAL_NULL_PTR) {
            vslot = ListWrapper::getVarSlot(pa, vslot.lptr_);
            vslot.lptr_ = *reinterpret_cast<LogicalPtr*>(vslot.pptr_);
            page_idx -= 1;
        }

        return (const PhysicalPtr)
            (vslot.pptr_ + sizeof(LogicalPtr) + idx * ITEM_SIZE);
    }
}

template <class T>
inline LogicalPtr& List<T>::getHead() {
    return *reinterpret_cast<LogicalPtr*>(
                val_ + sizeof(uint32_t));
}

template <class T>
inline LogicalPtr& List<T>::getTail() {
    return *reinterpret_cast<LogicalPtr*>(
                val_ + sizeof(uint32_t) + sizeof(LogicalPtr));
}

template <class T>
inline LogicalPtr List<T>::getHeadConst() const {
    return *reinterpret_cast<const LogicalPtr*>(
                val_ + sizeof(uint32_t));
}

template <class T>
inline LogicalPtr List<T>::getTailConst() const {
    return *reinterpret_cast<const LogicalPtr*>(
                val_ + sizeof(uint32_t) + sizeof(LogicalPtr));
}

template <class T>
inline uint32_t& List<T>::var_id() {
    return *reinterpret_cast<uint32_t*>(val_);
}

template <class T>
inline uint32_t List<T>::var_id_const() const {
    return *reinterpret_cast<const uint32_t*>(val_);
}

template <> List<int32_t>::List(const EvalValue& other);
template <> List<int32_t>::List(const EvalValue& other, uint32_t id);
template <> List<int64_t>::List(const EvalValue& other);
template <> List<int64_t>::List(const EvalValue& other, uint32_t id);
template <> List<float>::List(const EvalValue& other);
template <> List<float>::List(const EvalValue& other, uint32_t id);
template <> List<double>::List(const EvalValue& other);
template <> List<double>::List(const EvalValue& other, uint32_t id);

typedef List<int32_t> IntList;
typedef List<int64_t> LongList;
typedef List<float>   FloatList;
typedef List<double>  DoubleList;
