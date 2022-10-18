// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef STORAGE_UTIL_SEGMENT_MAP_H_
#define STORAGE_UTIL_SEGMENT_MAP_H_

// Project include
#include "storage/def_const.h"
#include "storage/util/page_allocator.h"

// Other include
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>

namespace storage {

class Segment;

/**
 * @brief segment map for paged_array
 * @details implemented as list of page(16KB)
 * @details handle concurrent read/write of key-value pair
 * @details manage full(not full) segment list
 * @author Ilju Lee, ijlee@kdb.snu.ac.kr
 */
class SegmentMap {
  public:
    /**
     * @brief SegmentMap constructor
     */
    SegmentMap() : page_(nullptr) { clear(); }

    /**
     * @brief SegmentMap destructor
     */
    ~SegmentMap() { deallocate(); }

    /**
     * @brief get header of not-full segment list
     * @return first not full semgnet
     */
    SegmentLptr& getNfSgmtHeader() { return nf_sgmt_header_; }

    /**
     * @brief get tail of not-full segment list
     * @return last not full semgnet
     */
    SegmentLptr& getNfSgmtTail() { return nf_sgmt_tail_; }

    /**
     * @breif insert a new segment
     * @param uint32_t idx: index of segment in segment map
     * @param Segment* sgmt: segment to insert
     */
    void insert(uint32_t idx, Segment* sgmt) {
        WriteLock write_lock(mx_sgmt_map_);

        resize(idx + 1);
        if (idx >= size_) size_ = idx + 1;

        (*this)[idx] = sgmt;
    }

    /**
     * @brief insert segment to end of segment map
     * @param Segment* sgmt: segment to insert
     */
    void push_back(Segment* sgmt) {
        insert(size_, sgmt);
    }

    /**
     * @brief clear segment map
     */
    void clear() {
        WriteLock write_lock(mx_sgmt_map_);
        deallocate();

        getNfSgmtHeader() = NULL_SGMT_LPTR;
        getNfSgmtTail() = NULL_SGMT_LPTR;
        size_ = 0;
        capacity_ = 0;
    }

    /**
     * @brief resize the capacity of segment map
     * @param uint32_t size: size of requested capacity
     */
    void resize(uint32_t size) {
        while (size > capacity_) {
            ListItem* last = page_;
            while (last && last->getNext() != nullptr) last = last->getNext();

            ListItem* new_page = new ListItem();
            if (last) {
                last->setNext(new_page);
            } else {
                page_ = new_page;
            }

            capacity_ += ListItem::ITEM_SIZE / sizeof(Segment*);
        }
    }

    /**
     * @brief deallocate all memory for SegmentMap (not segment)
     */
    void deallocate() {
        ListItem* page = page_;
        while (page) {
            ListItem* prev = page;
            page = page->getNext();
            delete prev;
        }

        page_ = nullptr;
    }

    /**
     * @brief get memory consumpation of this segment map
     */
    size_t getMemoryUsage() {
        size_t usage = 0;
        ListItem* page = page_;
        while (page) {
            page = page->getNext();
            usage += sizeof(ListItem);
        }

        return usage;
    }

    /**
     * @brief get segment from segment map
     * @param unsigned idx: index of segment
     * @return segment
     */
    Segment*& operator[](unsigned idx) const {
        if (idx >= size_) { assert(false && "wrong index for segment_map"); }

        uint32_t num_item_per_page = ListItem::ITEM_SIZE / sizeof(Segment*);
        uint32_t page_idx = idx / num_item_per_page;
        uint32_t offset = idx % num_item_per_page;

        ListItem* page = page_;
        for (unsigned i = 0; i < page_idx; i++) page = page->getNext();

        return *(reinterpret_cast<Segment**>(page->get()) + offset);
    }

    /**
     * @brief current size of segment map
     * @return size
     */
    uint32_t size() const { return size_; }

    /**
     * @brief serialize segment map to binary
     */
    void serialize(boost::archive::binary_oarchive& oa) const;

    /**
     * @brief deserialize segment map from binary
     */
    void deserialize(boost::archive::binary_iarchive& ia);

  private:
    Mutex       mx_sgmt_map_;

    SegmentLptr nf_sgmt_header_;
    SegmentLptr nf_sgmt_tail_;;

    ListItem*   page_;
    
    uint16_t    capacity_;
    uint16_t    size_;
};

}  // namespace storage

#endif  // namespace storage
