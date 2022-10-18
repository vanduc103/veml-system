// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef STORAGE_BASE_SEGMENT_VAR_H_
#define STORAGE_BASE_SEGMENT_VAR_H_

// Project include
#include "storage/base/segment.h"

namespace storage {

/**
 * @brief segment for var-size data
 * @details each page in SegmentVar can have different size of var-slot
 * @author Ilju Lee, ijlee@kdb.snu.ac.kr
 */
class SegmentVar : public Segment {
  public:
    /**
     * @brief SegmentVar constructor
     * @param uint32_t pa_id: id of paged_array that contains this segment
     * @param uint32_t num_data_pages: the number of data page
     * @param LogicalPtr start_index: logical address of segment header
     * @param SegmentLptr sgmt_lptr: the order of this segment in paged_array
     */
    SegmentVar(uint32_t pa_id, uint32_t num_data_pages,
            LogicalPtr start_index, SegmentLptr sgmt_lptr)
        : Segment(VAR, Page::VAR, pa_id,
                1 /* num_header_pages */, num_data_pages,
                0 /* num_slots */, 0 /* slots_per_page */,
                0 /* slot_width */, start_index, sgmt_lptr,
                Segment::HEADER_SIZE, Segment::HEADER_SIZE) {
        assert((uint64_t)sgmt_lptr_ << NUM_BITS_BYTE_OFFSET == start_index_);
    }

    /**
     * @brief SegmentVar constructor for recovery process
     */
    SegmentVar(Segment::Archive& ar) : Segment(ar) {}

    /**
     * @brief allocate an empty var-page
     * @param uint16_t width: byte size of slot in allocated page
     * @param Page* page: allocated page
     */
    LogicalPtr allocateVarPage(uint16_t width, Page* & page);

    /**
     * @brief release var slot
     * @param Page* page: page that contains slot to release
     * @param PageLptr page_idx: page-id in segment
     * @param uint32_t offset: byte offset from segment
     */
    bool releaseSlot(Page* page, PageLptr page_idx, uint32_t offset);

    /**
     * @brief get var-slot
     * @param uint32_t offset: byte offset from segment
     */
    AddressPair getVarSlot(uint32_t offset);
};

}  // namespace storage

#endif  // STORAGE_BASE_SEGMENT_VAR_H_
