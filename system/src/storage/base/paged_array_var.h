// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef STORAGE_BASE_PAGED_ARRAY_VAR_H_
#define STORAGE_BASE_PAGED_ARRAY_VAR_H_

// Related include
#include "storage/base/paged_array.h"

namespace storage {

class Page;

/**
 * @brief paged_array's variation for var-size data
 * @details not used alone, provide a var-size slot to PagedArray<T>
 * @author Ilju Lee, ijlee@kdb.snu.ac.kr
 */
class PagedArrayVar : public PABase {
  public:
    /**
     * @brief PagedArrayVar constructor
     */
    PagedArrayVar();

    /**
     * @brief PagedArrayVar constructor
     * @details constructor for recovery
     */
    PagedArrayVar(uint32_t pa_id);

    /**
     * @brief allocate new var-slot for requested width
     */
    AddressPair allocateVarSlot(uint16_t width);

    /**
     * @brief erase a var-slot
     */
    bool erase(LogicalPtr idx);

    /**
     * @brief get slot
     */
    AddressPair getVarSlot(LogicalPtr idx);

    /**
     * @brief get value as String
     */
    EvalValue getValue(LogicalPtr idx);

    /**
     * @brief serialize paged_array to binary
     */
    void serialize(boost::archive::binary_oarchive& oa) const;

    /**
     * @brief deserialize paged_array from binary
     */
    void deserialize(boost::archive::binary_iarchive& ia);
    void deserialize(SegmentLptr id,
                boost::archive::binary_iarchive& ia);

    /**
     * @brief get byte size of memory consumption
     */
    size_t getMemoryUsage();

  protected:
    /**
     * @brief allocate new var-page
     * @param int cid: category id for requested slot width
     * @param uint16_t width: byte size of request slot width
     */
    LogicalPtr allocateVarPage(int cid, uint16_t width);

    /**
     * @breif create new SegmentVar
     */
    SegmentLptr allocateVarSegment();

  private:
    /**
     * @brief calculate segment-id from logical address of var-slot
     */
    static SegmentLptr getVarSgmtIdFromLptr(LogicalPtr lptr);

    /**
     * @brief calculate page-id from logical address of var-slot
     */
    static PageLptr getVarPageIdFromLptr(LogicalPtr lptr);

    /**
     * @brief calculate byte offset from logical address of var-slot
     */
    static uint32_t getVarOffsetFromLptr(LogicalPtr lptr);

    /**
     * @brief get page from segment
     */
    Page* getVarPage(LogicalPtr lptr);

    /**
     * @brief check segment that contained newly allocated slot
     * @details if segment becomes full, revise not-full segment list
     */
    bool  checkAllocatedVarSlot(Page* page, LogicalPtr page_lptr, int cid);

    /**
     * @brief check segment that contains slot released
     * @details if segment was full, revise not-full segment list
     */
    bool  checkReleasedVarSlot(Page* page, LogicalPtr page_lptr, int cid);

    LogicalPtr nf_var_page_header_[NUM_VAR_PAGE_CATEGORY];
    LogicalPtr nf_var_page_tail_[NUM_VAR_PAGE_CATEGORY];
};

}  // namespace storage

#endif  // STORAGE_BASE_PAGED_ARRAY_VAR_H_
