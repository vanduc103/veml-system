// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

// Related include
#include "storage/util/bit_vector.h"

namespace storage {

BitVector::iterator::iterator(
        BitVector* bv, const SegmentVec& sgmts, Transaction* trans)
    : paged_array_base::iterator(sgmts, trans) {
    if (sgmts_.size() == 0) {
        is_ended_ = true;
        return;
    }

    size_ = bv->getSize();
    count_ = 0;

    page_idx_ = 0;
    slot_idx_ = 0;
    bit_idx_ = 0;

    page_ = sgmts_[sgmt_idx_]->getPage(page_idx_);

    num_pages_ = sgmts_[sgmt_idx_]->getNumDataPages();
    num_slots_ = page_->getNumSlots();
    is_ended_ = false;
}

BitVector::iterator& BitVector::iterator::operator++() {
    bit_idx_++;
    slot_idx_++;
    count_++;
    if (count_ == size_) {
        setEnd();
        return * this;
    }

    if (bit_idx_ >= 32) {
        bit_idx_ = 0;

        if (slot_idx_ >= num_slots_) {
            slot_idx_ = 0;
            page_idx_++;

            if (page_idx_ >= num_pages_) {
                page_idx_ = 0;
                sgmt_idx_++;

                if (sgmt_idx_ == sgmts_.size()) {
                    setEnd();
                    return *this;
                }

                num_pages_ = sgmts_[sgmt_idx_]->getNumDataPages();
            }

            page_ = sgmts_[sgmt_idx_]->getPage(page_idx_);
            num_slots_ = page_->getNumSlots();
        }
    }

    return *this;
}

bool BitVector::iterator::operator*() const {
    AddressPair slot;
    page_->getSlot(slot, slot_idx_/32, -1, true);
    uint32_t& encoded_slot = *reinterpret_cast<uint32_t*>(slot.pptr_);
    return (encoded_slot & VALID_MASK[bit_idx_]) != 0;
}

void BitVector::iterator::init() {
    paged_array_base::iterator::init();

    count_ = 0;
    page_idx_ = 0;
    slot_idx_ = 0;
    bit_idx_ = 0;

    page_ = sgmts_[sgmt_idx_]->getPage(page_idx_);

    num_pages_ = sgmts_[sgmt_idx_]->getNumDataPages();
    num_slots_ = page_->getNumSlots();
    is_ended_ = false;
}

EvalValue BitVector::getValue(LogicalPtr idx) {
    return Bool(isValid(idx));
}

uint64_t BitVector::assignLptr() {
    SegmentLptr sgmt_lptr = sgmt_map_.getNfSgmtHeader();
    Segment* sgmt = nullptr;
    AddressPair slot;
    bool allocated = false;

    while (!allocated) {
        while (sgmt_lptr == NULL_SGMT_LPTR) {
            sgmt_lptr = allocateBVSegment(false);
        }

        sgmt = getSegment(sgmt_lptr);
        while (sgmt && !allocated) {
            if (!sgmt->allocateSlot(slot, 0, false)) {
                sgmt_lptr = sgmt->getNextNfSegment();
                sgmt = getSegment(sgmt_lptr);
            } else {
                allocated = true;
            }
        }

        if (!allocated) {
            sgmt_lptr = allocateBVSegment(false);
        }
    }

    checkAllocatedSgmt(sgmt, sgmt_lptr);
    setValid(slot.lptr_);

    return slot.lptr_;
}

bool BitVector::isValid(LogicalPtr idx) {
    if (idx >= size_.load())
        return false;

    SegmentLptr sgmt_id = Segment::calculateSgmtId(idx);
    if (sgmt_id >= sgmt_map_.size() || sgmt_map_[sgmt_id] == nullptr)
        return false;

    uint32_t& slot
        = *reinterpret_cast<uint32_t*>(getBitSlot(idx).pptr_);

    uint64_t bit_idx = idx % 32;
    return (slot & VALID_MASK[bit_idx]) != 0 ;
}

void BitVector::setValid(LogicalPtr idx, bool force) {
    SegmentLptr sgmt_id = Segment::calculateSgmtId(idx);
    if (sgmt_id >= sgmt_map_.size() || sgmt_map_[sgmt_id] == nullptr) {
        if (!force) {
            std::string err("invalid index @BitVector::setValid, ");
            RTI_EXCEPTION(err);
            return;
        } else {
            allocateBVSegment(sgmt_id, false);
        }
    }

    uint32_t& slot
        = *reinterpret_cast<uint32_t*>(getBitSlot(idx, force).pptr_);

    uint64_t bit_idx = idx % 32;
    slot = slot | VALID_MASK[bit_idx];

    if (idx >= size_.load())
        size_.store(idx + 1);
}

void BitVector::setInvalid(LogicalPtr idx, bool force) {
    SegmentLptr sgmt_id = Segment::calculateSgmtId(idx);
    if (sgmt_id >= sgmt_map_.size() || sgmt_map_[sgmt_id] == nullptr) {
        if (!force) {
            assert("false" && "requested index exceeds bit vector's size");
            return;
        } else {
            allocateBVSegment(sgmt_id, false);
        }
    }

    uint32_t& slot
        = *reinterpret_cast<uint32_t*>(getBitSlot(idx, force).pptr_);

    uint64_t bit_idx = idx % 32;
    slot = slot & ~(VALID_MASK[bit_idx]);

    if (idx >= size_.load())
        size_.store(idx + 1);
}

void BitVector::serialize(boost::archive::binary_oarchive& oa) const {
    // TODO: impl
}

void BitVector::deserialize(SegmentLptr id,
        boost::archive::binary_iarchive& ia) {
    // TODO: impl
}

void BitVector::reserve(uint64_t size, bool dirty) {
    while (size > capacity_)
        resize(dirty);
}

BitVector::iterator BitVector::begin(Transaction* trans) {
    SgmtLptrVec sgmt_ids = this->getAllSgmtIds();
    return begin(sgmt_ids, trans);
}

BitVector::iterator BitVector::begin(
        SgmtLptrVec& sgmt_ids, Transaction* trans) {
    SegmentVec sgmts;
    for (auto sgmt_id : sgmt_ids) {
        Segment* sgmt = getSegment(sgmt_id);
        if (sgmt != nullptr)
            sgmts.push_back(sgmt);
    }
    return BitVector::iterator(this, sgmts, trans);
}

SegmentLptr BitVector::allocateBVSegment(bool dirty) {
    if (next_sgmt_id_ >= sgmt_map_.size()) {
        return allocateBVSegment(next_sgmt_id_++, dirty);
    }

    while (next_sgmt_id_ < sgmt_map_.size()
            && sgmt_map_[next_sgmt_id_] != nullptr)
        next_sgmt_id_++;

    return allocateBVSegment(next_sgmt_id_, dirty);
}

SegmentLptr BitVector::allocateBVSegment(uint64_t sgmt_id, bool dirty) {
    uint64_t num_slots;
    uint64_t start_index = 0;

    if (sgmt_id == 0) {
        num_slots = BASE_SUB_SGMT_SIZE;
    } else if (sgmt_id < MAX_SUB_SGMT) {
        num_slots = (uint64_t)BASE_SUB_SGMT_SIZE << (sgmt_id - 1);
        start_index = num_slots;
    } else {
        num_slots = BASE_SGMT_SIZE;
        start_index = BASE_SGMT_SIZE * (sgmt_id + 1 - MAX_SUB_SGMT);
    }

    uint32_t slots_per_page = (Page::getDataSize() / 4 * 4) * 8;
    uint32_t num_data_pages
        = (num_slots + slots_per_page - 1) / slots_per_page;
    if (num_slots <= slots_per_page) {
        num_data_pages = 1;
        slots_per_page = num_slots;
    }

    PhysicalPtr ptr = nullptr;
    if (posix_memalign(reinterpret_cast<void**>(&ptr),
            PAGE_SIZE, PAGE_SIZE * (num_data_pages + 1 /* for header*/))) {
        assert(0);
    }

    Segment* sgmt = new (ptr) Segment(
            Segment::DYNAMIC, Page::Type::ENCODED, id_,
            1 /* num_header_page */, num_data_pages, num_slots,
            slots_per_page, slot_width_, start_index, sgmt_id,
            Segment::HEADER_SIZE, dirty);

    for (uint64_t i = sgmt_map_.size(); i <= sgmt_id; ++i) {
        sgmt_map_.push_back(nullptr);
    }
    sgmt_map_[sgmt_id] = sgmt;

    SegmentLptr header_lptr = sgmt_map_.getNfSgmtHeader();
    if (header_lptr == NULL_SGMT_LPTR) {
        sgmt_map_.getNfSgmtHeader() = sgmt_id;
        sgmt_map_.getNfSgmtTail() = sgmt_id;
    } else {
        Segment* header = getSegment(header_lptr);
        sgmt->setNextNfSegment(header_lptr);
        header->setPrevNfSegment(sgmt_id);
        sgmt_map_.getNfSgmtHeader() = sgmt_id;
    }

    if (capacity_ < start_index + num_slots)
        capacity_ = start_index + num_slots;

    if (dirty && size_ < start_index + num_slots)
        size_.store(start_index + num_slots);

    return SegmentLptr(sgmt_id);
}

AddressPair BitVector::getBitSlot(uint64_t idx, bool set) {
    SegmentLptr sgmt_id = Segment::calculateSgmtId(idx);
    assert(sgmt_map_.size() > sgmt_id);

    Segment* sgmt = getSegment(sgmt_id);
    AddressPair slot;

    if (sgmt != nullptr) {
        uint64_t offset = idx - sgmt->getStartIndex();
        uint64_t slot_idx = offset / 32;

        slot = sgmt->getSlot(slot_idx, true);

        if (slot.pptr_ != nullptr && set) {
            sgmt->setNumSlots(offset, true);
        }
        return slot;
    }
    return {LOGICAL_NULL_PTR, nullptr};
}

void BitVector::resize(bool dirty) {
    allocateBVSegment(dirty);
}

}  // namespace stroage
