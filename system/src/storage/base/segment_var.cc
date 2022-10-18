// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

// Related include
#include "storage/base/segment_var.h"

namespace storage {

LogicalPtr SegmentVar::allocateVarPage(uint16_t width, Page* & page) {
    PageLptr page_idx = NULL_PAGE_LPTR;
    LogicalPtr lptr = LOGICAL_NULL_PTR;

    WriteLock write_lock(mx_nf_page_list_);
    page_idx = nf_page_header_[0];

    if (page_idx != NULL_PAGE_LPTR) {
        page = getPage(page_idx);
        PageLptr next_page = page->getNextPage();

        uint64_t offset = reinterpret_cast<char*>(page) - reinterpret_cast<char*>(this);
        lptr = start_index_ + offset;

        new (page) Page(Page::VAR, lptr, Page::getMaxNumSlots(width), width, 0, 0);
        page->setPrevVarPage(LOGICAL_NULL_PTR);
        page->setNextVarPage(LOGICAL_NULL_PTR);

        // remove from nf_page_list in sgmt
        nf_page_header_[0] = next_page;
        num_nf_pages_[0]--;
    }

    return lptr;
}

AddressPair SegmentVar::getVarSlot(uint32_t offset) {
    PhysicalPtr pptr = reinterpret_cast<PhysicalPtr>(
            reinterpret_cast<char*>(this) + offset);

    return {start_index_ + offset, pptr};
}

bool SegmentVar::releaseSlot(Page* page, PageLptr page_idx, uint32_t offset) {
    uint32_t offset_from_page = offset - (PAGE_SIZE * (page_idx + 1));

    bool was_full_page = false;
    if (!page->releaseVarSlot(offset_from_page, was_full_page)) {
        std::string err("wrong index @SegmentVar::releaseSlot");
        RTI_EXCEPTION(err);
    }

    return was_full_page;
}

}  // namespace storage
