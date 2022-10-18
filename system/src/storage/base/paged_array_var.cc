// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

// Related include
#include "storage/base/paged_array_var.h"

// Project include
#include "storage/base/segment_var.h"

// C & C++ system include
#include <fstream>

// Other include
#include <boost/serialization/binary_object.hpp>

namespace storage {

/*
 * constructor
 */
PagedArrayVar::PagedArrayVar() : PABase(false, 0) {
    for (int i = 0; i < NUM_VAR_PAGE_CATEGORY; ++i) {
        nf_var_page_header_[i] = LOGICAL_NULL_PTR;
        nf_var_page_tail_[i] = LOGICAL_NULL_PTR;
    }
}

PagedArrayVar::PagedArrayVar(uint32_t pa_id) : PABase(pa_id, false, 0) {
    for (int i = 0; i < NUM_VAR_PAGE_CATEGORY; ++i) {
        nf_var_page_header_[i] = LOGICAL_NULL_PTR;
        nf_var_page_tail_[i] = LOGICAL_NULL_PTR;
    }
}

/*
 * get/allocate var-slot
 */
AddressPair PagedArrayVar::allocateVarSlot(uint16_t width) {
    // calculate category_id
    int cid = 0;
    uint16_t upper_bound = 16;
    for (; cid < NUM_VAR_PAGE_CATEGORY
            && upper_bound < width; ++cid, upper_bound *= 2) {}

    if (width <= 0 || width > upper_bound) {
        std::string err("invalid width for var-slot @PagedArrayVar::allocateVarSlot, ");
        RTI_EXCEPTION(err);
    }

    // request var-slot
    AddressPair slot = AddressPair();
    LogicalPtr page_lptr = LOGICAL_NULL_PTR;
    Page* page = nullptr;
    bool allocated = false;

    while (!allocated) {
        // get var page
        page_lptr = nf_var_page_header_[cid];
        while (page_lptr == LOGICAL_NULL_PTR) {
            // category is empty, allocate page from nf_segment
            page_lptr = allocateVarPage(cid, upper_bound);
        }

        // get slot from page
        uint64_t sgmt_id = getVarSgmtIdFromLptr(page_lptr);
        SegmentVar* sgmt = reinterpret_cast<SegmentVar*>(getSegment(sgmt_id));
        if (sgmt != nullptr) {
            PageLptr page_idx = getVarPageIdFromLptr(page_lptr);
            page = sgmt->getPage(page_idx);
            slot = page->allocateSlot(false);

            if (!slot.isNull()) {
                allocated = true;
            }
        }
    }

    checkAllocatedVarSlot(page, page_lptr, cid);
    return slot;
}

bool PagedArrayVar::erase(LogicalPtr idx) {
    SegmentLptr sgmt_id = getVarSgmtIdFromLptr(idx);
    if (sgmt_id >= sgmt_map_.size()) {
        std::string err("wrong index @PagedArrayVar, ");
        RTI_EXCEPTION(err);
    }

    SegmentVar* sgmt = reinterpret_cast<SegmentVar*>(getSegment(sgmt_id));
    if (sgmt != nullptr) {
        uint32_t offset = getVarOffsetFromLptr(idx);
        uint32_t page_idx = getVarPageIdFromLptr(idx);
        Page* page = sgmt->getPage(page_idx);

        if (sgmt->releaseSlot(page, page_idx, offset)) {
            // if page was full, enroll nf_var_page list
            int cid = std::log2(page->getSlotWidth()) - 4;
            checkReleasedVarSlot(page, page->getStartLaddr(), cid);
        }
        return true;
    }

    std::string err("wrong index, null slot @PagedArrayVar::erase, ");
    RTI_EXCEPTION(err);
    return false;
}

AddressPair PagedArrayVar::getVarSlot(LogicalPtr idx) {
    SegmentLptr sgmt_id = getVarSgmtIdFromLptr(idx);
    if (sgmt_id >= sgmt_map_.size()) {
        std::string err("wrong index @PagedArrayVar, ");
        RTI_EXCEPTION(err);
    }

    SegmentVar* sgmt = reinterpret_cast<SegmentVar*>(getSegment(sgmt_id));
    AddressPair slot;

    if (sgmt != nullptr) {
        uint32_t offset = getVarOffsetFromLptr(idx);
        slot = sgmt->getVarSlot(offset);

        if (slot.pptr_ != nullptr)
            return slot;
    }

    std::string err("wrong index, null slot @PagedArrayVar::getVarSlot, ");
    RTI_EXCEPTION(err);
    return {LOGICAL_NULL_PTR, nullptr};
}

EvalValue PagedArrayVar::getValue(LogicalPtr idx) {
    return String(getVarSlot(idx).pptr_);
}

void PagedArrayVar::serialize(boost::archive::binary_oarchive& oa) const {
    PABase::serialize(oa);
    for (int i = 0; i < NUM_VAR_PAGE_CATEGORY; i++) {
        oa << nf_var_page_header_[i];
        oa << nf_var_page_tail_[i];
    }
}

void PagedArrayVar::deserialize(boost::archive::binary_iarchive& ia) {
    PABase::deserialize(ia);
    for (int i = 0; i < NUM_VAR_PAGE_CATEGORY; i++) {
        ia >> nf_var_page_header_[i];
        ia >> nf_var_page_tail_[i];
    }
}

void PagedArrayVar::deserialize(SegmentLptr id,
        boost::archive::binary_iarchive& ia) {
    // deserialize header pages
    Segment::Archive ar(ia);

    PhysicalPtr ptr = nullptr;
    if (posix_memalign(reinterpret_cast<void**>(&ptr), PAGE_SIZE,
                (ar.num_header_pages_ + ar.num_data_pages_) * PAGE_SIZE)) {
        assert(0);
    }

    SegmentVar* sgmt = new (ptr) SegmentVar(ar);

    // deserialize data pages
    ia >> boost::serialization::make_binary_object(
            ptr + (ar.num_header_pages_ * PAGE_SIZE),
            ar.num_data_pages_ * PAGE_SIZE);

    sgmt_map_.insert(id, sgmt);
}

size_t PagedArrayVar::getMemoryUsage() {
    size_t size = PABase::getMemoryUsage();
    size += 2 * sizeof(LogicalPtr) * NUM_VAR_PAGE_CATEGORY;

    return size;
}

LogicalPtr PagedArrayVar::allocateVarPage(int cid, uint16_t width) {
    SegmentLptr sgmt_lptr = sgmt_map_.getNfSgmtHeader();
    SegmentVar* sgmt = nullptr;

    LogicalPtr l_page = LOGICAL_NULL_PTR;
    Page* p_page = nullptr;

    bool allocated = false;
    while (!allocated) {
        while (sgmt_lptr == NULL_SGMT_LPTR) {
            sgmt_lptr = allocateVarSegment();

            if (sgmt_lptr == NULL_SGMT_LPTR)
                sgmt_lptr = sgmt_map_.getNfSgmtHeader();
        }

        sgmt = reinterpret_cast<SegmentVar*>(getSegment(sgmt_lptr));
        while (sgmt && !allocated) {
            l_page = sgmt->allocateVarPage(width, p_page);

            if (l_page == LOGICAL_NULL_PTR) {
                sgmt_lptr = sgmt->getNextNfSegment();
                sgmt = reinterpret_cast<SegmentVar*>(getSegment(sgmt_lptr));
            } else {
                allocated = true;
            }
        }

        if (!allocated) {
            sgmt_lptr = sgmt_map_.getNfSgmtHeader();
        }
    }

    {
        WriteLock write_lock(mx_sgmt_list_);
        Page* tail = getVarPage(nf_var_page_tail_[cid]);
        if (tail) {
            p_page->setPrevVarPage(nf_var_page_tail_[cid]);
            tail->setNextVarPage(l_page);
        } else {
            nf_var_page_header_[cid] = nf_var_page_tail_[cid] = l_page;
        }
    }

    checkAllocatedSgmt(sgmt, sgmt_lptr);
    return l_page;
}

SegmentLptr PagedArrayVar::allocateVarSegment() {
    WriteLock write_lock(mx_sgmt_list_, try_to_lock);
    if (!write_lock.owns_lock())
        return NULL_SGMT_LPTR;

    uint64_t sgmt_id = next_sgmt_id_++;
    LogicalPtr start_idx = sgmt_id << NUM_BITS_BYTE_OFFSET;

    int size = 0;
    if (sgmt_id == 0) {
        size = BASE_VAR_SUB_SGMT_SIZE;
    } else if (sgmt_id < MAX_SUB_SGMT) {
        size = (uint64_t)BASE_VAR_SUB_SGMT_SIZE << (sgmt_id - 1);
    } else {
        size = BASE_VAR_SGMT_SIZE;
    }
    uint32_t num_data_pages = size / PAGE_SIZE - 1 /* reserve for header page */;

    // allocate mem from OS
    SegmentVar* new_sgmt = nullptr;
    PhysicalPtr ptr = nullptr;
    if (posix_memalign(reinterpret_cast<void**>(&ptr), PAGE_SIZE, size)) {
        assert(0);
    }

    // allocate var sgmt
    new_sgmt = new (ptr) SegmentVar(id_, num_data_pages, start_idx, sgmt_id);

    sgmt_map_.push_back(nullptr);
    sgmt_map_[sgmt_id] = new_sgmt;

    // update not-full segment list
    SegmentLptr header_lptr = sgmt_map_.getNfSgmtHeader();
    if (header_lptr == NULL_SGMT_LPTR) {
        sgmt_map_.getNfSgmtHeader() = sgmt_id;
        sgmt_map_.getNfSgmtTail() = sgmt_id;
    } else {
        Segment* header = getSegment(header_lptr);
        header->setPrevNfSegment(sgmt_id);
        new_sgmt->setNextNfSegment(header_lptr);
        sgmt_map_.getNfSgmtHeader() = sgmt_id;
    } 

    return sgmt_id;
}

SegmentLptr PagedArrayVar::getVarSgmtIdFromLptr(LogicalPtr lptr) {
    return (lptr & MASK_VAR_SGMT_ID_GETTER) >> NUM_BITS_BYTE_OFFSET;
}

uint32_t PagedArrayVar::getVarOffsetFromLptr(LogicalPtr lptr) {
    return lptr & MASK_BYTE_OFFSET_GETTER;
}

PageLptr PagedArrayVar::getVarPageIdFromLptr(LogicalPtr lptr) {
    uint64_t offset = getVarOffsetFromLptr(lptr);
    return (offset / PAGE_SIZE) - 1;
}

Page* PagedArrayVar::getVarPage(LogicalPtr lptr) {
    if (lptr == LOGICAL_NULL_PTR)
        return nullptr;

    SegmentLptr sgmt_id = getVarSgmtIdFromLptr(lptr);
    Segment* sgmt = getSegment(sgmt_id);

    uint32_t page_id = getVarPageIdFromLptr(lptr);
    return sgmt->getPage(page_id);
}

bool PagedArrayVar::checkAllocatedVarSlot(
        Page* page, LogicalPtr page_lptr, int cid) {
    if (page) {
        WriteLock write_lock(mx_sgmt_list_);
        if (page->isFull() && !page->getFullFlag()) {
            Page* prev = getVarPage(page->getPrevVarPage());
            Page* next = getVarPage(page->getNextVarPage());

            if (nf_var_page_header_[cid] == page_lptr)
                nf_var_page_header_[cid] = page->getNextVarPage();

            if (nf_var_page_tail_[cid] == page_lptr)
                nf_var_page_tail_[cid] = page->getPrevVarPage();

            if (prev)
                prev->setNextVarPage(page->getNextVarPage());

            if (next)
                next->setPrevVarPage(page->getPrevVarPage());

            page->setPrevVarPage(LOGICAL_NULL_PTR);
            page->setNextVarPage(LOGICAL_NULL_PTR);
            page->setFullFlag();
        }
        return true;
    }
    return false;
}

bool PagedArrayVar::checkReleasedVarSlot(
        Page* page, LogicalPtr page_lptr, int cid) {
    if (page) {
        WriteLock write_lock(mx_sgmt_list_);
        if (!page->isFull() && page->getFullFlag()) {
            if (nf_var_page_tail_[cid] != LOGICAL_NULL_PTR) {
                Page* tail = getVarPage(nf_var_page_tail_[cid]);
                tail->setNextVarPage(page_lptr);
                page->setPrevVarPage(nf_var_page_tail_[cid]);
            }
            nf_var_page_tail_[cid] = page_lptr;

            if (nf_var_page_header_[cid] == LOGICAL_NULL_PTR) {
                nf_var_page_header_[cid] = page_lptr;
            }

            page->setFullFlag(false);
            return true;
        }
    }
    return false;
}

}   // namespace storage
