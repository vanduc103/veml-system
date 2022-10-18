// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

// C & C++ system include
#include <fstream>

// Other include
#include <boost/serialization/binary_object.hpp>

namespace storage {

inline PABase::iterator& PABase::iterator::operator++() {
    if (is_ended_) {
        return *this;
    }

    while (true) {
        ++(*sgmt_itr_);

        if (!(*sgmt_itr_)) {
            if (++sgmt_idx_ == sgmts_.size()) {
                setEnd();
            } else {
                delete sgmt_itr_;
                sgmt_itr_ = sgmts_[sgmt_idx_]->begin(trans_);
                if (!(*sgmt_itr_))
                    continue;
            }
            return *this;
        }
        return *this;
    }
    assert(false && "it can't be reached");
}

inline void PABase::iterator::nextPage() {
    if (is_ended_) {
        return;
    }

    while (true) {
        sgmt_itr_->nextPage();

        if (!(*sgmt_itr_)) {
            if (++sgmt_idx_ == sgmts_.size()) {
                setEnd();
            } else {
                delete sgmt_itr_;
                sgmt_itr_ = sgmts_[sgmt_idx_]->begin(trans_);
                if (!(sgmt_itr_))
                    continue;
            }
            return;
        }
        return;
    }
    assert(false && "it can't be reached");
}

inline PABase::iterator&
PABase::iterator::operator=(const PABase::iterator& itr) {
    sgmts_.clear();
    sgmts_ = itr.sgmts_;
    sgmt_itr_ = (itr.sgmt_itr_) ? new Segment::iterator(*itr.sgmt_itr_) : nullptr;
    sgmt_idx_ = itr.sgmt_idx_;
    is_ended_ = itr.is_ended_;
    trans_ = itr.trans_;

    return *this;
}

inline bool PABase::iterator::isValid() const {
    return sgmt_itr_->isValid();
}

inline LogicalPtr PABase::iterator::getLptr() const {
    return (**sgmt_itr_).lptr_;
}

inline PhysicalPtr PABase::iterator::getPptr() const {
    return (**sgmt_itr_).pptr_;
}

inline AddressPair PABase::iterator::operator*() const {
    return **sgmt_itr_;
}

inline Page* PABase::iterator::getPage() const {
    return sgmt_itr_->getPage();
}

inline AddressPair PABase::insert(EvalValue& val) {
    return insertSlot(val.getRefPtr());
}

inline AddressPair PABase::insert(EvalValue& val, LogicalPtr idx) {
    return insertSlot(val.getRefPtr(), idx);
}

inline AddressPair PABase::insert(Transaction& trans, EvalValue& val) {
    return insertSlot(trans, val.getRefPtr());
}

inline AddressPair PABase::insert(
        Transaction& trans, EvalValue& val, LogicalPtr idx) {
    return insertSlot(trans, val.getRefPtr(), idx);
}

inline bool PABase::erase(LogicalPtr idx) {
    SegmentLptr sgmt_id = Segment::calculateSgmtId(idx);
    if (sgmt_map_.size() <= sgmt_id || sgmt_map_[sgmt_id] == nullptr) {
        std::string err("invalid index @paged_array_base::erase, ");
        RTI_EXCEPTION(err)
    }
    Segment* sgmt = getSegment(sgmt_id);

    uint32_t offset = idx - sgmt->getStartIndex();
    bool was_full = false;
    bool success = sgmt->releaseSlot(offset, was_full);

    if (success && was_full) {
        checkReleaseSlot(sgmt, sgmt_id);
    }

    return success;
}

inline bool PABase::erase(Transaction& trans, LogicalPtr idx) {
    SegmentLptr sgmt_id = Segment::calculateSgmtId(idx);
    if (sgmt_map_.size() <= sgmt_id || sgmt_map_[sgmt_id] == nullptr) {
        return false;
    }
    Segment* sgmt = getSegment(sgmt_id);

    Version* version = trans.createVersion(
                            Version::DELETE, trans.getTid(),
                            trans.getStartTs(), id_, idx, nullptr, 0);

    uint32_t offset = idx - sgmt->getStartIndex();
    sgmt->insertNewVersion(offset, version);

    return true;
}

inline bool PABase::update(
        Transaction& trans, LogicalPtr idx, EvalValue& val) {
    SegmentLptr sgmt_id = Segment::calculateSgmtId(idx);
    if (sgmt_map_.size() <= sgmt_id || sgmt_map_[sgmt_id] == nullptr) {
        return false;
    }
    Segment* sgmt = getSegment(sgmt_id);
    Version* version = trans.createVersion(Version::UPDATE, trans.getTid(),
                        trans.getStartTs(), getId(), idx, val.getPtr(), slot_width_);

    uint32_t offset = idx - sgmt->getStartIndex();
    sgmt->insertNewVersion(offset, version);

    return true;
}

inline AddressPair PABase::getSlot(LogicalPtr idx, bool force) {
    SegmentLptr sgmt_id = Segment::calculateSgmtId(idx);
    if (sgmt_id >= sgmt_map_.size()) {
        std::string err("invalid idx @PABase::getSlot");
        RTI_EXCEPTION(err);
        return AddressPair();
    }

    Segment* sgmt = getSegment(sgmt_id);
    AddressPair slot;

    if (sgmt != nullptr) {
        uint32_t offset = idx - sgmt->getStartIndex();
        slot = sgmt->getSlot(offset, force);

        if (slot.pptr_ != nullptr)
            return slot;
        else {
            slot = sgmt->getSlot(offset, force);
            std::string err("sgmt->getSlot failed @paged_array_base::getSlot, ");
            err.append(", paid: ");
            err.append(std::to_string(id_));
            err.append(", idx: ");
            err.append(std::to_string(idx));
            err.append(", offset: ");
            err.append(std::to_string(offset));
            err.append(", sgmt_id: ");
            err.append(std::to_string(sgmt_id));
            err.append(", sgmt_map size: ");
            err.append(std::to_string(sgmt_map_.size()));
            err.append("\n");
            RTI_EXCEPTION(err);
        }

    } else {
        std::string err("invalid segment @paged_array_base::getSlot, ");
        RTI_EXCEPTION(err);
    }

    return {LOGICAL_NULL_PTR, nullptr};
}

inline AddressPair PABase::getSlot(
        Transaction& trans, LogicalPtr idx, bool force) {
    SegmentLptr sgmt_id = Segment::calculateSgmtId(idx);
    if (sgmt_id >= sgmt_map_.size()) {
        std::string err("invalid index @paged_array_base::getSlot, ");
        RTI_EXCEPTION(err);
    }

    Segment* sgmt = getSegment(sgmt_id);
    AddressPair slot;

    if (sgmt != nullptr) {
        uint32_t offset = idx - sgmt->getStartIndex();
        slot = sgmt->getSlot(trans, offset, force);

        if (slot.pptr_ != nullptr)
            return slot;
    } else {
        std::string err("invalid segment @paged_array_base::getSlot, ");
        RTI_EXCEPTION(err);
    }

    return {LOGICAL_NULL_PTR, nullptr};
}

inline bool PABase::isValid(LogicalPtr idx) {
    SegmentLptr sgmt_id = Segment::calculateSgmtId(idx);
    if (sgmt_id >= sgmt_map_.size())  return false;

    Segment* sgmt = getSegment(sgmt_id);
    if (sgmt) {
        uint32_t offset = idx - sgmt->getStartIndex();
        return sgmt->isValid(offset);
    }

    return false;
}

inline EvalValue PABase::getValue(
        LogicalPtr idx, ValueType vt, int32_t offset) {
    AddressPair slot = this->getSlot(idx);

    if (slot.pptr_ != nullptr) {
        PhysicalPtr val = slot.pptr_ + offset;
        VALUE_TYPE_EXECUTION(vt,
                return reinterpret_cast<TYPE*>(val)->getEval());
    } else {
        return EvalValue();
    }

    return EvalValue();
}

inline EvalValue PABase::getValueFromSlot(
        PhysicalPtr slot, const FieldInfo* fi) {
    PhysicalPtr val = slot + fi->getOffset();
    VALUE_TYPE_EXECUTION(fi->getValueType(),
            return reinterpret_cast<TYPE*>(val)->getEval());

    return EvalValue();
}

inline EvalValue PABase::getValueFromSlot(
        PhysicalPtr slot, ValueType vt) {
    VALUE_TYPE_EXECUTION(vt,
            return reinterpret_cast<TYPE*>(slot)->getEval());

    return EvalValue();
}

inline Segment* PABase::getSegment(SegmentLptr sgmt_id) const {
    if (sgmt_id == NULL_SGMT_LPTR) return nullptr;
    if (sgmt_id >= sgmt_map_.size()) return nullptr;
    return sgmt_map_[sgmt_id];
}


/*
 * PagedArray<T>
 */
template <class T>
PagedArray<T>::PagedArray(bool is_metadata, uint16_t slot_width)
    : PABase(is_metadata, slot_width == 0? sizeof(T) : slot_width),
    var_slots_(nullptr) {}

template <class T>
PagedArray<T>::PagedArray(uint32_t id, bool is_metadata, uint16_t slot_width)
    : PABase(id, is_metadata, slot_width == 0? sizeof(T) : slot_width),
    var_slots_(nullptr) {}

template <class T>
AddressPair PagedArray<T>::insert(typename T::eval_type val) {
    return insertSlot(reinterpret_cast<PhysicalPtr>(&val));
}

template <class T>
AddressPair PagedArray<T>::insert(typename T::eval_type val, LogicalPtr idx) {
    return insertSlot(reinterpret_cast<PhysicalPtr>(&val), idx);
}

template <class T>
AddressPair PagedArray<T>::insert(Transaction& trans, typename T::eval_type val) {
    return insertSlot(trans, reinterpret_cast<PhysicalPtr>(&val));
}

template <class T>
AddressPair PagedArray<T>::insert(Transaction& trans, typename T::eval_type val, LogicalPtr idx) {
    return insertSlot(trans, reinterpret_cast<PhysicalPtr>(&val), idx);
}

template <class T>
typename T::stor_type& PagedArray<T>::operator[](LogicalPtr idx) {
    AddressPair slot = getSlot(idx);
    if (slot.pptr_ == nullptr) {
        std::string err("wrong index for paged array @PagedArray<T>::operator[], ");
        RTI_EXCEPTION(err);
    }

    return reinterpret_cast<T*>(slot.pptr_)->getRefValue();
}

template <class T>
EvalValue PagedArray<T>::getValue(LogicalPtr idx) {
    AddressPair slot = getSlot(idx);
    if (slot.pptr_ == nullptr) {
        std::string err("wrong index for paged array @PagedArray<T>::getValue, ");
        RTI_EXCEPTION(err);
    }

    return reinterpret_cast<T*>(slot.pptr_)->getEval();
}

template <class T>
void PagedArray<T>::setValue(LogicalPtr idx, eval val) {
    AddressPair slot = getSlot(idx, true);
    memcpy(slot.pptr_, reinterpret_cast<PhysicalPtr>(&val), slot_width_);
}

template <class T>
void PagedArray<T>::copyTo(LongVec& to, LogicalPtr start, LogicalPtr end) {
    if (end == LOGICAL_NULL_PTR) {
        end = this->getNumElement();
    }

    while (start < end) {
        SegmentLptr sgmt_id = Segment::calculateSgmtId(start);
        if (sgmt_id >= sgmt_map_.size()) return;
        Segment* sgmt = getSegment(sgmt_id);

        PageLptr page_lptr = (start - sgmt->getStartIndex())
                                / sgmt->getSlotsPerPage();
        while (start < end && page_lptr < sgmt->getNumDataPages()) {
            Page* page = sgmt->getPage(page_lptr++);
            uint64_t page_start = page->getStartLaddr();

            uint64_t start_offset = start - page_start;
            uint64_t num = end > page_start + page->getMaxNumSlots()?
                            page->getMaxNumSlots() - start_offset : end-start;

            AddressPair slot;
            page->getSlot(slot, start_offset, -1, true);
            int64_t* start_ptr = reinterpret_cast<int64_t*>(slot.pptr_);
            int64_t* end_ptr = start_ptr + num;

            std::copy(start_ptr, end_ptr, std::back_inserter(to));
            start += num;
        }
    }
}

/*
 * iterator
 */
template <class T>
typename PagedArray<T>::iterator
PagedArray<T>::begin(Transaction* trans) const {
    SgmtLptrVec sgmt_ids = getAllSgmtIds();
    return begin(sgmt_ids, trans);
}

template <class T>
typename PagedArray<T>::iterator
PagedArray<T>::begin(LogicalPtr lptr, Transaction* trans) const {
    SegmentLptr sgmt_id = Segment::calculateSgmtId(lptr);
    SgmtLptrVec sgmt_ids;
    sgmt_ids.reserve(sgmt_map_.size()-sgmt_id);
    for ( ; sgmt_id < sgmt_map_.size(); sgmt_id++) {
        sgmt_ids.push_back(sgmt_id);
    }

    PABase::iterator itr = begin(sgmt_ids, trans);
    itr.setLptr(lptr);
    return itr;
}

template <class T>
typename PagedArray<T>::iterator
PagedArray<T>::begin(SgmtLptrVec& sgmt_ids, Transaction* trans) const {
    SegmentVec sgmts;
    sgmts.reserve(sgmt_ids.size());
    for (auto id : sgmt_ids) {
        sgmts.push_back(getSegment(id));
    }

    return iterator(sgmts, trans);
}

template <class T>
void PagedArray<T>::deserialize(SegmentLptr id,
        boost::archive::binary_iarchive& ia) {
    // deserialize header pages
    Segment::Archive ar(ia);

    PhysicalPtr ptr = nullptr;
    if (posix_memalign(reinterpret_cast<void**>(&ptr), PAGE_SIZE,
                (ar.num_header_pages_ + ar.num_data_pages_) * PAGE_SIZE)) {
        assert(0);
    }

    Segment* sgmt = new (ptr) Segment(ar);

    // deserialize data pages
    ia >> boost::serialization::make_binary_object(
            ptr + (ar.num_header_pages_ * PAGE_SIZE),
            ar.num_data_pages_ * PAGE_SIZE);

    sgmt_map_.insert(id, sgmt);
}

template <class T>
size_t PagedArray<T>::getMemoryUsage() {
    size_t size = PABase::getMemoryUsage();
    if (var_slots_) {
        size += ((PABase*)var_slots_)->getMemoryUsage();
    }

    return size;
}

/*
 * template specialization for Varchar_T
 */
template <>
PagedArray<Varchar_T>::PagedArray(bool is_metadata, uint16_t slot_width);
template <>
PagedArray<Varchar_T>::PagedArray(uint32_t id, bool is_metadata, uint16_t slot_width);
template <>
AddressPair PagedArray<Varchar_T>::insert(String val);
template <>
AddressPair PagedArray<Varchar_T>::insert(String val, LogicalPtr idx);

/*
 * template specialization for Tuple_T
 */
template <>
AddressPair PagedArray<Tuple_T>::insert(Tuple val);
template <>
AddressPair PagedArray<Tuple_T>::insert(Tuple val, LogicalPtr idx);
template <>
AddressPair PagedArray<Tuple_T>::insert(Transaction& trans, Tuple val);
template <>
AddressPair PagedArray<Tuple_T>::insert(Transaction& trans, Tuple val, LogicalPtr idx);

}  // namespace storage
