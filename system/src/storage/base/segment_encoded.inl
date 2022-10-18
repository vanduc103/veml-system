// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

namespace storage {

/*
 * DeltaDict
 */
template <class T>
inline int DeltaDict<T>::getVid(typename T::eval_type value, bool insert) {
    int vid = -1;
    if (!value_map_.find(value, vid) && insert) {
        vid = value_id_.fetch_add(1);
        value_map_.insert(value, vid);

        WriteLock write_lock(mx_dict_);
        while ((int)values_.size() <= vid) values_.push_back(value);
        values_[vid] = value;
    }
    return vid;
}

template <class T>
inline typename T::eval_type DeltaDict<T>::getValue(int vid) {
    if (vid >= value_id_.load()) {
        std::string err("wrong value id for segment_encoded, ");
        RTI_EXCEPTION(err);
    }

    ReadLock read_lock(mx_dict_);
    return values_[vid];
}

template <class T>
inline PhysicalPtr DeltaDict<T>::getValuePtr(int vid) {
    if (vid >= value_id_.load()) {
        std::string err("wrong value id for segment_encoded, ");
        RTI_EXCEPTION(err);
    }

    ReadLock read_lock(mx_dict_);
    return reinterpret_cast<PhysicalPtr>(&(values_[vid]));
}

template <class T>
std::size_t DeltaDict<T>::size() {
    return value_id_.load();
}

template <class T>
std::size_t DeltaDict<T>::getMemoryUsage() {
    size_t dict = value_map_.getMemoryUsage();
    size_t value = size() * sizeof(eval);

    return dict + value;
}

template <class T>
void DeltaDict<T>::getStaticDict(std::vector<int>& vids,
        std::vector<typename T::eval_type>& values) {
    values.clear();
    vids.clear();
    vids.reserve(size());

    ReadLock read_lock(mx_dict_);
    auto itr = value_map_.begin();
    int new_vid = 0;
    for ( ; itr; ++itr) {
        values.push_back(itr.first());
        vids[itr.second()] = new_vid++;
    }
}

template <class T>
void DeltaDict<T>::clear() {
    WriteLock write_lock(mx_dict_);
    values_.clear();
    value_id_.store(0);
}

/*
 * SegmentEncoded<T>::iterator
 */
template <class T>
SegmentEncoded<T>::iterator::iterator(Segment* sgmt, Transaction* trans)
    : Segment::iterator(sgmt, trans) {

    bit_length_ = sgmt->getBitLength();
    mask_ = sgmt_->getMask();
}

template <class T>
typename SegmentEncoded<T>::iterator&
SegmentEncoded<T>::iterator::operator=(const SegmentEncoded<T>::iterator& itr) {
    sgmt_ = itr.sgmt_;
    page_ = itr.page_;

    max_num_page_ = itr.max_num_page_;
    page_idx_ = itr.page_idx_;
    slot_idx_ = itr.slot_idx_;
    last_slots_ = itr.last_slots_;

    page_start_idx_ = itr.page_start_idx_;
    slot_ = itr.slot_;
    slot_width_ = itr.slot_width_;

    is_ended_ = itr.is_ended_;
    trans_ = itr.trans_;

    bit_length_ = itr.bit_length_;
    mask_ = itr.mask_;

    return *this;
}

template <class T>
inline typename SegmentEncoded<T>::iterator&
SegmentEncoded<T>::iterator::operator++() {
    ++slot_idx_;
    slot_ += slot_width_;

    if (slot_idx_ >= last_slots_) {
        bool next_exist = getNextPage();
        if (!next_exist) setEnd();
    }

    return *this;
}

template <class T>
int SegmentEncoded<T>::iterator::getVid() const {
    if (sgmt_->getSegmentType() == STATIC) {
        unsigned offset = slot_idx_ * bit_length_;
        unsigned pos = offset & 31;
        offset >>= 5;

        AddressPair slot;
        page_->getSlot(slot, offset, -1, true);
        uint64_t vid_data = *reinterpret_cast<uint64_t*>(slot.pptr_);

        return (int)((vid_data >> pos) & mask_);
    } else {
        int32_t vid = *reinterpret_cast<int32_t*>(slot_);
        return vid;
    }
}

template <class T>
typename T::eval_type SegmentEncoded<T>::iterator::getValue() const {
    int vid = this->getVid();
    return reinterpret_cast<SegmentEncoded<T>*>(sgmt_)->getValueFromVid(vid);
}

template <class T>
void SegmentEncoded<T>::iterator::init() {
    Segment::iterator::init();
    bit_length_ = sgmt_->getBitLength();
    mask_ = sgmt_->getMask();
    page_idx_ = -1;
    is_ended_ = !getNextPage();
}

template <class T>
void SegmentEncoded<T>::iterator::setLptr(LogicalPtr lptr) {
    uint64_t offset = lptr - sgmt_->getStartIndex();
    SegmentEncoded<T>* sgmt = reinterpret_cast<SegmentEncoded<T>*>(sgmt_);
    page_idx_ = offset / sgmt->getSlotsPerPage();
    page_ = sgmt->getPage(page_idx_);
    slot_idx_ = offset % sgmt->getSlotsPerPage(); 
    last_slots_ = page_->getNumSlots();

    page_start_idx_ = page_->getStartLaddr();
    AddressPair slot;
    page_->getSlot(slot, slot_idx_, -1, true);
    slot_ = slot.pptr_;
    slot_width_ = page_->getSlotWidth();

    bit_length_ = sgmt->getBitLength();
    mask_ = sgmt->getMask();
}


template <class T>
bool SegmentEncoded<T>::iterator::getNextPage() {
    if (Segment::iterator::getNextPage()) {
        last_slots_ = page_->getNumSlots();
        return true;
    }
    return false;
}

/*
 * constructor
 */
template <class T>
SegmentEncoded<T>::SegmentEncoded(
            Type sgmt_type, Page::Type page_type, uint32_t pa_id,
            uint32_t num_header_pages, uint32_t num_data_pages,
            uint32_t num_slots, uint32_t slots_per_page, uint16_t slot_width,
            uint64_t start_index, SegmentLptr sgmt_lptr, uint32_t dict_offset,
            uint32_t bit_length, uint32_t mask, uint32_t var_offset, uint32_t var_size,
            uint32_t dict_size, PhysicalPtr delta_dict) 
    : Segment(sgmt_type, page_type, pa_id, num_header_pages,
            num_data_pages, num_slots, slots_per_page, slot_width,
            start_index, sgmt_lptr, dict_offset) {
    bit_length_ = bit_length;
    mask_ = mask;
    var_offset_ = var_offset;
    delta_dict_ = delta_dict;
    var_size_ = var_size;
    dict_size_ = dict_size;
}

template <class T>
inline void SegmentEncoded<T>::insert(uint32_t offset, typename T::eval_type value) {
    if (sgmt_type_ == STATIC) {
        assert(false && "wrong encoded segment type");
    }
    DeltaDict<T>* dict = reinterpret_cast<DeltaDict<T>*>(delta_dict_);
    int vid = dict->getVid(value, true);

    AddressPair slot;
    if (!assignSlot(slot, offset, false)) {
        std::string err("invalid offset @SegmentEncoded<T>::insert");
        RTI_EXCEPTION(err);
    }

    *reinterpret_cast<int32_t*>(slot.pptr_) = vid;
}

template <class T>
inline void SegmentEncoded<T>::insert(
        Transaction& trans, uint32_t offset, typename T::eval_type value) {
    if (sgmt_type_ == STATIC) {
        assert(false && "wrong encoded segment type");
    }
    DeltaDict<T>* dict = reinterpret_cast<DeltaDict<T>*>(delta_dict_);
    int vid = dict->getVid(value, true);

    AddressPair slot;
    if (!assignSlot(slot, offset, false)) {
        std::string err("invalid offset @SegmentEncoded<T>::insert");
        RTI_EXCEPTION(err);
    }

    /*
     * TODO: create version
     */

    *reinterpret_cast<int32_t*>(slot.pptr_) = vid;
}

template <class T>
Segment::iterator* SegmentEncoded<T>::begin(Transaction* trans) {
    return new SegmentEncoded<T>::iterator(this, trans);
}

template <class T>
inline int SegmentEncoded<T>::readVid(uint32_t offset) {
    PageLptr page_lptr = offset / slots_per_page_;
    offset = offset % slots_per_page_;
    Page* page = getPage(page_lptr);

    if (sgmt_type_ == STATIC) {
        unsigned slot_idx = offset * bit_length_;
        unsigned pos = slot_idx & 31;
        slot_idx >>= 5;

        AddressPair slot;
        page->getSlot(slot, slot_idx, -1, true);
        uint64_t vid_data = *reinterpret_cast<uint64_t*>(slot.pptr_);

        return (int)((vid_data >> pos) & mask_);
    } else {
        AddressPair slot;
        page->getSlot(slot, offset, -1, true);
        int vid = *reinterpret_cast<int32_t*>(slot.pptr_);
        return vid;
    }
}

/*
 * binary search interface
 */
template <class T>
inline int SegmentEncoded<T>::exact_search(const EvalValue& val) {
    bool exact = false;
    int res = lower_bound(val, exact);

    if (exact) {
        return res;
    } else {
        return -1;
    }
}

template <class T>
int SegmentEncoded<T>::lower_bound(const EvalValue& val, bool& exact) {
    eval v = val.get<eval>();
    T* dict = this->getDictPtr(0);
    int low = 0;
    int high = dict_size_ - 1;

    while (low < high) {
        int mid = low + (high - low) / 2;
        if (dict[mid].getValue() < v)
            low = mid + 1;
        else
            high = mid - 1;
    }

    if (dict[low].getValue() == v) exact = true;
    
    return low;
}

/*
 * get memory usage
 */
template <class T>
std::size_t SegmentEncoded<T>::getMemoryUsage() {
    std::size_t size = PAGE_SIZE * (num_header_pages_ + num_data_pages_);
    if (sgmt_type_ == DYNAMIC) {
        DeltaDict<T>* dict =
            reinterpret_cast<DeltaDict<T>*>(delta_dict_);
        size += dict->getMemoryUsage();
    }

    return size;
}

/*
 * protected getter
 */
template <class T>
PhysicalPtr SegmentEncoded<T>::getDictPtr(int vid) {
    if (sgmt_type_ == STATIC) {
        PhysicalPtr dict = reinterpret_cast<PhysicalPtr>(this) + dictionary_offset_;
        T* dict_T = reinterpret_cast<T*>(dict);
        return reinterpret_cast<PhysicalPtr>(&(dict_T[vid]));
    } else {
        DeltaDict<T>* dict =
            reinterpret_cast<DeltaDict<T>*>(delta_dict_);
        return dict->getValuePtr(vid);
    } 
}
        

template <class T>
typename T::eval_type SegmentEncoded<T>::getValueFromVid(int vid) {
    if (sgmt_type_ == STATIC) {
        PhysicalPtr dict = reinterpret_cast<PhysicalPtr>(this) + dictionary_offset_;
        T* dict_T = reinterpret_cast<T*>(dict);
        return dict_T[vid].getValue();
    } else {
        DeltaDict<T>* dict =
            reinterpret_cast<DeltaDict<T>*>(delta_dict_);
        return dict->getValue(vid);
    }
}

/*
 * generate static segment
 */
template <class T>
void SegmentEncoded<T>::writeEmbeddedDict(std::vector<eval>& values) {
    if (sgmt_type_ == DYNAMIC) {
        std::string err("wrong segment type for encoding");
        RTI_EXCEPTION(err);
    }

    T* dict = reinterpret_cast<T*>(getDictPtr(0));

    auto itr = values.begin();
    unsigned idx = 0;
    for ( ; itr != values.end(); itr++, idx++) {
        new (&(dict[idx])) T(*itr);
    }
}

template <class T>
void SegmentEncoded<T>::writeVidSequence(std::vector<int>& vids, std::vector<int>& ordered_vids) {
    if (sgmt_type_ == DYNAMIC) {
        std::string err("wrong segment type for encoding");
        RTI_EXCEPTION(err);
    }

    Page* page = nullptr;
    int slot_idx = 0;
    for (unsigned i = 0; i < vids.size(); i++, slot_idx++) {
        if (i % slots_per_page_ == 0) {
            // finalize previous page
            if (page != nullptr) {
                page->setNumSlots(slot_idx);
            }

            // move to next page
            int page_idx = i / slots_per_page_;
            page = getPage(page_idx);
            slot_idx = 0;
        }
        int vid = ordered_vids[vids[i]];

        unsigned offset = slot_idx * bit_length_;
        unsigned pos = offset & 31;
        offset >>= 5;

        AddressPair slot;
        page->getSlot(slot, offset, -1, true);
        uint64_t& vid_data = *reinterpret_cast<uint64_t*>(
                                reinterpret_cast<uint32_t*>(slot.pptr_));

        vid_data &= ~(((uint64_t)mask_) << pos);
        vid_data |= ((uint64_t)vid) << pos;
    }

    if (page)
        page->setNumSlots(slot_idx);
}

}  // namespace storage
