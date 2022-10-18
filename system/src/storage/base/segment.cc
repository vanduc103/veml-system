// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

// Related include
#include "storage/base/segment.h"

// Project include
#include "storage/catalog.h"
#include "storage/base/paged_array.h"

// C & C++ system include
#include <cassert>
#include <fstream>

// Other include
#include <boost/serialization/binary_object.hpp>

namespace storage {

/*
 * Segment::iterator
 */
Segment::iterator::iterator(Segment* sgmt, Transaction* trans)
    : sgmt_(sgmt), page_(nullptr), max_num_page_(sgmt->getNumDataPages()),
    page_idx_(-1), slot_idx_(0), last_slots_(0), trans_(trans) {
    is_ended_ = !getNextPage();
}

Segment::iterator& Segment::iterator::operator=(const Segment::iterator& itr) {
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

    return *this;
}

void Segment::iterator::init() {
    page_ = nullptr;
    page_idx_ = -1;
    slot_idx_ = 0;
    last_slots_ = 0;
    is_ended_ = !getNextPage();
}

void Segment::iterator::setLptr(LogicalPtr lptr) {
    uint64_t offset = lptr - sgmt_->getStartIndex();
    page_idx_ = offset / sgmt_->getSlotsPerPage(); 
    page_ = sgmt_->getPage(page_idx_);
    slot_idx_ = offset % sgmt_->getSlotsPerPage();
    last_slots_ = page_->getLastSlot();

    page_start_idx_ = page_->getStartLaddr();
    AddressPair slot;
    page_->getSlot(slot, slot_idx_);
    slot_ = slot.pptr_;
    slot_width_ = page_->getSlotWidth();
}

bool Segment::iterator::getNextPage() {
    assert(slot_idx_ >= last_slots_);

    while (true) {
        ++page_idx_;
        if (page_idx_ >= max_num_page_) {
            return false;
        }

        page_ = sgmt_->getPage(page_idx_);
        if (page_->isEmpty()) continue;

        slot_idx_ = -1;

        if (page_->getType() == Page::ENCODED) {
            last_slots_ = page_->getNumSlots();
        } else {
            last_slots_ = page_->getLastSlot();
        }
        page_start_idx_ = page_->getStartLaddr();
        slot_width_ = page_->getSlotWidth();

        do {
            AddressPair slot;
            if (page_->getType() == Page::ENCODED) {
                page_->getSlot(slot, ++slot_idx_, -1, true);
                slot_ = slot.pptr_;
            } else {
                page_->getSlot(slot, ++slot_idx_);
                slot_ = slot.pptr_;
            }
        } while (!slot_ && slot_idx_ < last_slots_);

        if (!slot_) continue;

        return true;
    }
}

Segment::Archive::Archive(boost::archive::binary_iarchive& ia) {
    ia >> sgmt_type_ >> page_type_ >> pa_id_
        >> num_header_pages_ >> num_data_pages_
        >> max_num_slots_ >> slots_per_page_
        >> slot_width_ >> full_flag_;

    for (unsigned i = 0; i < NUM_PAGE_LIST; i++) {
        PageLptr header, tail;
        uint32_t num_nf;
        ia >> header >> tail >> num_nf;
        nf_page_header_.push_back(header);
        nf_page_tail_.push_back(tail);
        num_nf_pages_.push_back(num_nf);
    }
    
    ia >> start_index_ >> sgmt_lptr_ >> prev_nf_segment_ >> next_nf_segment_;
    ia >> dictionary_offset_ >> bit_length_ >> mask_
        >> var_offset_ >> var_size_ >> dict_size_;
}

/*
 * constructors
 */
Segment::Segment(Type sgmt_type, Page::Type page_type, uint32_t pa_id,
        uint32_t num_header_pages, uint32_t num_data_pages,
        uint32_t max_num_slots, uint32_t slots_per_page, uint16_t slot_width,
        uint64_t start_index, SegmentLptr sgmt_lptr,
        uint32_t dict_offset, bool dirty)
    : sgmt_type_(sgmt_type), page_type_(page_type), pa_id_(pa_id),
    num_header_pages_(num_header_pages), num_data_pages_(num_data_pages),
    max_num_slots_(max_num_slots), slots_per_page_(slots_per_page),
    slot_width_(slot_width), full_flag_(0), start_index_(start_index), sgmt_lptr_(sgmt_lptr),
    prev_nf_segment_(NULL_SGMT_LPTR), next_nf_segment_(NULL_SGMT_LPTR),
    dictionary_offset_(dict_offset), bit_length_(0), mask_(0), 
    var_offset_(0), var_size_(0), delta_dict_(nullptr) {

    // initialize pages
    unsigned i;
    for (i = 0; i < num_data_pages - 1; ++i) {
        Page* page = getPage(i);
        new (page) Page(page_type,
                start_index + i * slots_per_page, /* start_index */
                slots_per_page, /* max_num_slots */
                slot_width,
                i - 1, /* prev_page */
                i + 1 /* next_page */,
                dirty);
    }

    // init last page
    Page* page = getPage(i);
    new (page) Page(page_type,
            start_index + i * slots_per_page, /* start_index */
            max_num_slots - i * slots_per_page, /* max_num_slots */
            slot_width,
            i - 1, /* prev_page */
            NULL_PAGE_LPTR /* next_page */,
            dirty);

    if (multiPageList()) {
        uint32_t page_list_size = num_data_pages / NUM_PAGE_LIST;
        for (unsigned p = 0; p < NUM_PAGE_LIST; p++) {
            nf_page_header_[p] = p * page_list_size;
            nf_page_tail_[p] = (p + 1) * page_list_size - 1;

            num_nf_pages_[p] = page_list_size;
            getPage(p * page_list_size)->setPrevPage(NULL_PAGE_LPTR);
            if (p != NUM_PAGE_LIST - 1)
                getPage((p + 1) * page_list_size -1)->setNextPage(NULL_PAGE_LPTR);
        }
        num_nf_pages_[NUM_PAGE_LIST - 1]
            = num_data_pages - (page_list_size * (NUM_PAGE_LIST - 1));
        nf_page_tail_[NUM_PAGE_LIST - 1] = num_data_pages - 1;
    } else {
        nf_page_header_[0] = 0;
        nf_page_tail_[0] = num_data_pages -1;
        num_nf_pages_[0] = num_data_pages;
        for (unsigned p = 1; p < NUM_PAGE_LIST; p++) {
            nf_page_header_[p] = NULL_PAGE_LPTR;
            nf_page_tail_[p] = NULL_PAGE_LPTR;
            num_nf_pages_[p] = 0;
        }
    }
}

Segment::Segment(Segment::Archive& ar)
    : sgmt_type_(ar.sgmt_type_), page_type_(ar.page_type_), pa_id_(ar.pa_id_),
    num_header_pages_(ar.num_header_pages_), num_data_pages_(ar.num_data_pages_),
    max_num_slots_(ar.max_num_slots_), slots_per_page_(ar.slots_per_page_),
    slot_width_(ar.slot_width_), full_flag_(ar.full_flag_),
    start_index_(ar.start_index_), sgmt_lptr_(ar.sgmt_lptr_),
    prev_nf_segment_(ar.prev_nf_segment_), next_nf_segment_(ar.next_nf_segment_),
    dictionary_offset_(ar.dictionary_offset_),
    bit_length_(ar.bit_length_), mask_(ar.mask_),
    var_offset_(ar.var_offset_), var_size_(ar.var_size_), dict_size_(ar.dict_size_),
    delta_dict_(nullptr) {
    for (unsigned i = 0; i < NUM_PAGE_LIST; i++) {
        nf_page_header_[i] = ar.nf_page_header_[i];
        nf_page_tail_[i] = ar.nf_page_tail_[i];
        num_nf_pages_[i] = ar.num_nf_pages_[i];
    }
}

/*
 * allocate/release/access slot
 */
bool Segment::allocateSlot(AddressPair& slot, uint32_t list_id, bool is_trans) {
    list_id = (multiPageList())? list_id : 0;

    PageLptr page_lptr = nf_page_header_[list_id];
    Page* page = getPage(page_lptr);

    while (page) {
        slot = page->allocateSlot(is_trans);

        if (!slot.isNull()) {
            return checkAllocatedSlot(page, page_lptr, list_id);
        } else {
            page_lptr = page->getNextPage();
            page = getPage(page_lptr);
        }

        if (page == nullptr && num_nf_pages_[list_id] > 0) {
            page_lptr = nf_page_header_[list_id];
            page = getPage(page_lptr);
        }
    }

    return false;
}

bool Segment::assignSlot(AddressPair& slot, uint32_t offset, bool is_trans) {
    int num_slots_per_page = Page::getMaxNumSlots(slot_width_);

    PageLptr page_idx = offset / num_slots_per_page;
    int offset_in_page = offset % num_slots_per_page;
    assert(page_idx < getNumDataPages());

    Page* page = getPage(page_idx);
    slot = page->allocateSlot(offset_in_page, is_trans);

    if (slot.lptr_ == LOGICAL_NULL_PTR) return false;

    if (multiPageList()) {
        uint32_t list_id = getListId(page_idx);

        assert(list_id < NUM_PAGE_LIST);
        return checkAllocatedSlot(page, page_idx, list_id);
    } else {
        return checkAllocatedSlot(page, page_idx, 0);
    }
    return true;
}

bool Segment::releaseSlot(uint32_t offset, bool& was_full_sgmt) {
    int num_slots_per_page = Page::getMaxNumSlots(slot_width_);

    PageLptr page_id = offset / num_slots_per_page;
    int offset_in_page = offset % num_slots_per_page;
    assert(page_id < getNumDataPages());

    Page* page = getPage(page_id);
    bool was_full_page;
    if (!page->releaseSlot(offset_in_page, was_full_page))
        return false;

    int32_t list_id = 0;
    if (multiPageList()) {
        list_id = num_data_pages_ % NUM_PAGE_LIST == 0?
                page_id / (num_data_pages_/NUM_PAGE_LIST)
                : page_id / (num_data_pages_/NUM_PAGE_LIST + 1);
    }

    if (was_full_page)
        return checkReleasedSlot(page, page_id, was_full_sgmt, list_id);

    return true;
}

/*
 * public, MVCC management
 */
void Segment::insertNewVersion(uint32_t offset, Version* version) {
    PageLptr page_idx = offset / slots_per_page_;
    int offset_in_page = offset % slots_per_page_;
    assert(page_idx < getNumDataPages());

    // set version flag
    Page* page = getPage(page_idx);
    setVersionFlag(page, offset_in_page, true);

    // enroll version
    version_map_.insertNewVersion(offset, version);
}

void Segment::consolidateVersion(uint32_t offset) {
    Version* version = version_map_.getVersion(offset);
    if (version) {
        consolidate(version, offset);
    }
}

/*
 * public getter/setter
 */
uint64_t Segment::getStartIndex(int32_t page_id) const {
    Page* page = getPage(page_id);
    return page->getLptr(0);
}

int32_t Segment::getNumSlots(int32_t page_id) const {
    Page* page = getPage(page_id);
    return page->getNumSlots();
}

int32_t Segment::getListId(int32_t page_id) const {
    if (multiPageList()) {
        int32_t page_list_size = num_data_pages_ / NUM_PAGE_LIST;
        int32_t list_id = page_id / page_list_size;
        if (list_id >= NUM_PAGE_LIST)
            list_id = NUM_PAGE_LIST - 1;

        return list_id;
    } else {
        return 0;
    }
}

uint64_t Segment::getNumElement() {
    uint64_t num = 0;
    for (unsigned i = 0; i < num_data_pages_; i++) {
        Page* page = getPage(i);
        num += page->getNumSlots();
    }

    return num;
}

void Segment::setNumSlots(uint32_t offset, bool for_bits) {
    int num_slots_per_page = Page::getMaxNumSlots(getPage(0)->getSlotWidth());
    if (for_bits)
        num_slots_per_page *= 32;

    PageLptr page_idx = offset / num_slots_per_page;
    unsigned offset_in_page = offset % num_slots_per_page;
    assert(page_idx < getNumDataPages());

    Page* page = getPage(page_idx);
    if (offset_in_page >= page->getNumSlots()) {
        page->setNumSlots(offset_in_page + 1);
    }
}

bool Segment::getFullFlag() {
    ReadLock read_lock(mx_full_flag_);
    return full_flag_ != 0;
}

void Segment::setFullFlag(bool set) {
    WriteLock write_lock(mx_full_flag_);
    full_flag_ = set;
}

bool Segment::isFull() {
    for (unsigned i = 0; i < NUM_PAGE_LIST; i++) {
        if (nf_page_header_[i] != NULL_PAGE_LPTR)
            return false;
    }
    return true;
}

bool Segment::isFull(uint32_t list_id) {
    return nf_page_header_[list_id] == NULL_PAGE_LPTR;
}

bool Segment::isVersioned(uint32_t offset) {
    int num_slots_per_page = Page::getMaxNumSlots(getPage(0)->getSlotWidth());

    PageLptr page_idx = offset / num_slots_per_page;
    uint32_t offset_in_page = offset % num_slots_per_page;
    assert(page_idx < getNumDataPages());

    Page* page = getPage(page_idx);
    return page->isVersioned(offset_in_page);
}

/*
 * get iterator
 */
Segment::iterator* Segment::begin(Transaction* trans) {
    return new iterator(this, trans);
}

void Segment::serialize(boost::archive::binary_oarchive& oa) {
    // serialize segment header
    oa << sgmt_type_ << page_type_ << pa_id_
        << num_header_pages_ << num_data_pages_
        << max_num_slots_ << slots_per_page_
        << slot_width_ << full_flag_;

    for (unsigned i = 0 ; i < NUM_PAGE_LIST; i++) {
        oa << nf_page_header_[i] << nf_page_tail_[i] << num_nf_pages_[i];
    }

    oa << start_index_ << sgmt_lptr_ << prev_nf_segment_ << next_nf_segment_;
    oa << dictionary_offset_ << bit_length_ << mask_
        << var_offset_ << var_size_ << dict_size_;

    // serialize segment data pages
    oa << boost::serialization::make_binary_object(
            (PhysicalPtr)this + num_header_pages_ * PAGE_SIZE,
            num_data_pages_ * PAGE_SIZE);
}

/*
 * get memory usage
 */
std::size_t Segment::getMemoryUsage() {
    return PAGE_SIZE * (num_header_pages_ + num_data_pages_);
}

/*
 * private util functions
 */
bool Segment::checkAllocatedSlot(Page* page, PageLptr page_lptr, int32_t list_id) {
    if (page) {
        if (page->isFull() && !page->getFullFlag()) {
            WriteLock write_lock(mx_nf_page_list_);
            Page* prev = getPage(page->getPrevPage());
            Page* next = getPage(page->getNextPage());

            if (nf_page_header_[list_id] == page_lptr)
                nf_page_header_[list_id] = page->getNextPage();
            if (nf_page_tail_[list_id] == page_lptr)
                nf_page_tail_[list_id] = page->getPrevPage();

            if (prev)
                prev->setNextPage(page->getNextPage());

            if (next)
                next->setPrevPage(page->getPrevPage());

            page->setPrevPage(NULL_PAGE_LPTR);
            page->setNextPage(NULL_PAGE_LPTR);
            page->setFullFlag();
            num_nf_pages_[list_id]--;
        }
        return true;
    }
    return false;
}

bool Segment::checkReleasedSlot(Page* page,
        PageLptr page_id, bool& was_full_sgmt, int32_t list_id) {
    WriteLock write_lock(mx_nf_page_list_);
    num_nf_pages_[list_id]++;
    page->setFullFlag(false);
    if (nf_page_header_[list_id] == NULL_PAGE_LPTR) {
        nf_page_header_[list_id] = page_id;
        nf_page_tail_[list_id] = page_id;
        was_full_sgmt = true;
    } else {
        Page* tail = getPage(nf_page_tail_[list_id]);
        if (tail) {
            tail->setNextPage(page_id);
            page->setPrevPage(nf_page_tail_[list_id]);
        }

        page->setNextPage(NULL_PAGE_LPTR);
        nf_page_tail_[list_id] = page_id;
        was_full_sgmt = (full_flag_ == 1);
    }

    setFullFlag(false);
    return true;
}

void Segment::finalize() {
    for (unsigned i = 0; i < NUM_PAGE_LIST; i++) {
        nf_page_header_[i] = nf_page_tail_[i] = NULL_PAGE_LPTR;
    }
}

/*
 * private, MVCC management
 */
void Segment::consolidate(Version* version, uint32_t offset) {
    PageLptr page_idx = offset / slots_per_page_;
    int32_t offset_in_page = offset % slots_per_page_;
    Page* page = getPage(page_idx);

    // get page lock
    page->getLock();

    // get closed version
    bool has_insert_version = false;
    Version* closed = nullptr;
    closed = version->getClosed(closed, has_insert_version);
    
    if (closed != nullptr) {
        if (closed->getCts() == -2) {
            // all versions are aborted
            if (has_insert_version) {
                release(page, page_idx, offset_in_page);
            }
        }
        else {
            switch (closed->getType())
            {
                case Version::INSERT:
                {
                    break;
                }
                case Version::DELETE:
                {
                    release(page, page_idx, offset_in_page);
                    break;
                }
                case Version::UPDATE:
                {
                    EvalValue eval = closed->getImage();
                    PhysicalPtr image = eval.getPtr();

                    page->updateSlot(offset_in_page, image);
                    delete[] image;
                    break;
                }
                case Version::LIST:
                {
                    assert(0);
                }
            }
        }

        // clear version list
        version->clearVersionList();
        if (version->getCts() != -1) {
            version_map_.removeVersion(offset);
            page->setVersionFlag(offset_in_page, false);
        }
    }

    page->getUnLock();
}

void Segment::release(Page* page, PageLptr page_id, int offset_in_page) {
    bool was_full_page, was_full_sgmt;
    page->release(offset_in_page, was_full_page);

    if (was_full_page) {
        int32_t list_id = 0;
        if (multiPageList()) {
            list_id = num_data_pages_ % NUM_PAGE_LIST == 0?
                    page_id / (num_data_pages_/NUM_PAGE_LIST)
                    : page_id / (num_data_pages_/NUM_PAGE_LIST + 1);
        }

        checkReleasedSlot(page, page_id, was_full_sgmt, list_id);

        if (was_full_sgmt) {
            PABase* pa = Metadata::getPagedArray(pa_id_);
            pa->checkReleaseSlot(this, sgmt_lptr_);
        }
    }
}

/*
 * private getter/setter
 */
AddressPair Segment::getVersion(int64_t start_ts,
        int64_t current_ts, uint32_t offset, AddressPair slot) {
    Version* version = version_map_.getVersion(offset);
    if (version == nullptr) {
        return slot;
    }

    bool has_insert_version = false;
    version = version->getLatest(start_ts, current_ts, version, has_insert_version);
    if (version) {
        switch (version->getType())
        {
            case Version::UPDATE:
            {
                EvalValue image = version->getImage();
                slot = {slot.lptr_, image.getPtr()};
                break;
            }
            case Version::DELETE:
                slot = AddressPair();
            case Version::INSERT:
                break;
            default:
                assert(0);
        }
    } else if (has_insert_version) {
        slot = AddressPair();
    }

    return slot;
}

bool Segment::getVersionFlag(uint32_t offset) {
    int num_slots_per_page = Page::getMaxNumSlots(getPage(0)->getSlotWidth());

    PageLptr page_idx = offset / num_slots_per_page;
    int offset_in_page = offset % num_slots_per_page;
    assert(page_idx < getNumDataPages());

    Page* page = getPage(page_idx);
    return page->isVersioned(offset_in_page);
}

void Segment::setVersionFlag(Page* page, int32_t offset_in_page, bool flag) {
    page->getLock();
    page->setVersionFlag(offset_in_page, flag);
    page->getUnLock();
}

} // namespace storage
