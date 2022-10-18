// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef STORAGE_BASE_SEGMENT_H_
#define STORAGE_BASE_SEGMENT_H_

// Project include
#include "storage/base/page.h"
#include "storage/util/version_hash_map.h"
#include "common/types/types.h"
#include "concurrency/transaction.h"

// Other include
#include <boost/static_assert.hpp>

class Version;

namespace storage {

class RowTable;
class ColumnTable;

/**
 * @brief memory units that make up a paged-arry
 * @details contains multiple pages, consists of a single chunk of memory
 * @author Ilju Lee, ijlee@kdb.snu.ac.kr
 */
class Segment {
  protected:
    /**
     * @brief segment iterator
     * @detail used to iterate over single segment
     */
    class iterator {
      public:
        /**
         * @brief create null iterator
         */
        iterator() : sgmt_(nullptr), is_ended_(true) {}

        /**
         * @brief Segment::iterator constructor
         * @param Segment* sgmt: segment to iterate
         * @Transaction* trans: tx that call iterator, null if not transactional
         */
        iterator(Segment* sgmt, Transaction* trans);
        
        /**
         * @brief Segment::iterator copy constructor
         */
        iterator(const iterator& itr) : sgmt_(itr.sgmt_), page_(itr.page_),
            max_num_page_(itr.max_num_page_), page_idx_(itr.page_idx_),
            slot_idx_(itr.slot_idx_), last_slots_(itr.last_slots_),
            page_start_idx_(itr.page_start_idx_), slot_(itr.slot_),
            slot_width_(itr.slot_width_), is_ended_(itr.is_ended_),
            trans_(itr.trans_) {}

        /**
         * @brief Segment::iterator destructor
         */
        virtual ~iterator() {}

        /**
         * @brief Segment::iterator copy operator
         */
        iterator& operator=(const iterator& itr);

        /**
         * @brief move to next slot
         */
        iterator& operator++();

        /**
         * @brief move to next page
         */
        void nextPage();

        /**
         * @breif get current slot
         */
        AddressPair operator*() const;

        /**
         * @brief get current page
         */
        Page* getPage() const;

        /**
         * @brief check that current slot is valid
         */
        bool isValid() const;

        /**
         * @brief check the iteration is finished
         */
        operator bool() { return !is_ended_; }

        /**
         * @brief check the iteration is finished
         */
        bool isEnd() { return is_ended_; }

        /**
         * @brief set flag for end of iteration
         */
        void setEnd() { is_ended_ = true; }

        /**
         * @brief init iterator
         */
        virtual void init();

        /**
         * @brief change current slot
         * @details used to start the iteration from the middle of the segment
         * @param LogicalPtr lptr: index of new current slot
         */
        void setLptr(LogicalPtr lptr);

      protected:
        /**
         * @brief move to next page of the segment
         */
        virtual bool getNextPage();

        Segment*    sgmt_;
        Page*       page_;

        uint32_t    max_num_page_;
        PageLptr    page_idx_;
        uint16_t    slot_idx_;
        uint16_t    last_slots_;

        LogicalPtr  page_start_idx_;
        PhysicalPtr slot_;
        uint16_t    slot_width_;

        bool        is_ended_;

        Transaction* trans_;
    };

  public:
    /**
     * @brief an enum type of segment
     * @li NORMAL: segment of PagedArray
     * @li STATIC: read-only segment of PagedArrayEncoded
     * @li DYNAMIC: updatable segment of PagedArrayEncoded
     * @li VAR: segment of PagedArrayVar
     */
    enum Type { NORMAL, STATIC, DYNAMIC, VAR };

    class Archive {
      public:
        Archive(boost::archive::binary_iarchive& ia);
        Segment::Type sgmt_type_;
        Page::Type page_type_;
        uint32_t pa_id_;
        uint32_t num_header_pages_;
        uint32_t num_data_pages_;
        uint32_t max_num_slots_;
        uint32_t slots_per_page_;
        uint16_t slot_width_;
        uint16_t full_flag_;
        std::vector<PageLptr> nf_page_header_;
        std::vector<PageLptr> nf_page_tail_;
        std::vector<uint32_t> num_nf_pages_;
        LogicalPtr start_index_;
        SegmentLptr sgmt_lptr_;
        SegmentLptr prev_nf_segment_;
        SegmentLptr next_nf_segment_;
        uint32_t dictionary_offset_;
        uint32_t bit_length_;
        uint32_t mask_;
        uint32_t var_offset_;
        uint32_t var_size_;
        uint32_t dict_size_;
    };

    /**
     * @brief Segment constructor
     * @param Type type: type of segment
     * @param Page::Type type: type of page
     * @param uint32_t pa_id: id of paged_array that contains this segment
     * @param uint32_t num_header_pages: the number of header page
     * @param uint32_t num_data_pages: the number of data page
     * @param uint32_t num_slots: the number of slots in this segment
     * @param uint32_t slots_per_page: the number of slots in page
     * @param uint16_t slot_width: byte size of the slot
     * @param LogicalPtr start_index: logical address of the first slot
     * @param SegmentLptr sgmt_lptr: the order of this segment in paged_array
     * @param uint32_t dict_offset: byte position of ditionary in this segment
     * @param bool dirty: if true, set all slots with 0XFF (used for BitVector impl)
     */
    Segment(Type sgmt_type, Page::Type page_type, uint32_t pa_id,
            uint32_t num_header_pages, uint32_t num_data_pages,
            uint32_t num_slots, uint32_t slots_per_page, uint16_t slot_width,
            LogicalPtr start_index, SegmentLptr sgmt_lptr,
            uint32_t dict_offset, bool dirty = false);
    /**
     * @brief Segment constructor for recovery process
     */
    Segment(Archive& ar);

    /**
     * @brief Segment destructor
     */
    ~Segment() {}

    /**
     * @brief allocate any unused slot
     * @param AddressPair slot: allocated slot
     * @param uint32_t list_id: id of page-lit in this segment
     * @param bool is_trans: true if it is transactional operation
     * @reutrn true if success, false if this segment is full
     */
    bool allocateSlot(AddressPair& slot, uint32_t list_id, bool is_trans);

    /**
     * @brief allocate designated slot
     * @param AddressPair slot: allocated slot
     * @param uint32_t offset: offset of designated slot
     * @param bool is_trans: true if it is transactional operation
     * @reutrn true if success, false if the slot is in use
     */
    bool assignSlot(AddressPair& slot, uint32_t offset, bool is_trans);

    /**
     * @brief release slot
     * @param uint32_t offset: offset of slot to release
     * @param bool was_full_sgmt: set true if the segment was full
     * @return true if succss, false if the slot is not valid
     */
    bool releaseSlot(uint32_t offset, bool& was_full_sgmt);

    /**
     * @brief get slot
     * @param uint32_t offset: slot's offset from start_index
     * @param bool force: if true, get slot regardness valud flag
     * @return slot
     */
    AddressPair getSlot(uint32_t offset, bool force);

    /**
     * @brief get slot, transactional operation
     * @param uint32_t offset: slot's offset from start_index
     * @param bool force: if true, get slot regardness valud flag
     * @return slot
     */
    AddressPair getSlot(Transaction& trans, uint32_t offet, bool force);

    /**
     * @brief check a slot is valid
     */
    bool isValid(uint32_t offset);

    /**
     * @brief insert new version to version-map in segment
     * @param uint32_t offset: slot's offset from start_index
     * @param Version* version: version to insert
     */
    void insertNewVersion(uint32_t offset, Version* version);

    /**
     * @brief consolidate version of a slot
     * @param uint32_t offset: slot's offset from start_index
     */
    void consolidateVersion(uint32_t offset);

    /**
     * @brief get type of page in this segment
     */
    Page::Type getPageType() const { return page_type_; }

    /**
     * @brief get type of this segment
     */
    Type getSegmentType() const { return sgmt_type_; }

    /**
     * @brief get the number of header-page in this segment
     */
    uint32_t getNumHeaderPages() const { return num_header_pages_; }

    /**
     * @brief get the number of data-page in this segment
     */
    uint32_t getNumDataPages() const { return num_data_pages_; }

    /**
     * @brief get index of this segment in paged-array
     */
    SegmentLptr getSegmentLptr() const { return sgmt_lptr_; }

    /**
     * @brief get starting index of this segment in paged-array
     */
    uint64_t getStartIndex() const { return start_index_; }

    /**
     * @brief get starting index of specific page in this segment
     * @param int32_t page_id: id of page
     */
    uint64_t getStartIndex(int32_t page_id) const;

    /**
     * @brief get capcity of this segment
     */
    uint32_t getNumSlots() const { return max_num_slots_; }

    /**
     * @brief get the number of current valid slot of page
     * @param int32_t page_id: id of page
     */
    int32_t getNumSlots(int32_t page_id) const;

    /**
     * @brief get byte size of slot in this segment
     */
    uint16_t getSlotWidth() const { return slot_width_; }

    /**
     * @brief get encoded bit-length of slot in this segment(STATIC only)
     */
    uint32_t getBitLength() const { return bit_length_; }

    /**
     * @brief get encoding mask of slot in this segment(STATIC only)
     */
    uint32_t getMask() const { return mask_; }

    /**
     * @brief get the capcity of page in this segment
     */
    uint32_t getSlotsPerPage() const { return slots_per_page_; }

    /**
     * @brief get physical pointer of dictionary (PagedArrayEncoded only)
     */
    PhysicalPtr getDict() const { return delta_dict_; }

    /**
     * @brief get the number of current valid slots in this segment
     */
    uint64_t getNumElement();

    /**
     * @brief set number of slots in page (PagedArrayEncoded only)
     * @details if the slot has the highest index in its page, set page's num slots
     * @param uint32_t offset: slot's offset from start_index
     * @param bool for_bits, true if it is BitVector
     */
    void setNumSlots(uint32_t offset, bool for_bits = false);

    /**
     * @brief get id of page-list which contains the page
     */
    int32_t getListId(int page_id) const;

    /**
     * @brief check whether segment is full (require lock)
     */
    bool getFullFlag();

    /**
     * @brief set the full flag of the segment (require lock)
     */
    void setFullFlag(bool set = true);

    /**
     * @brief check whether segment is full
     */
    bool isFull();

    /**
     * @brief check whether specific page-list in segment is full
     * @param uint32_t list_id: id of page-list
     */
    bool isFull(uint32_t list_id);

    /**
     * @brief check a slot has version
     * @param uint32_t offset: slot's offset from start_index
     */
    bool isVersioned(uint32_t offset);

    /**
     * @brief create a iterator of this segment
     * @param Transaction* trans: tx, null if it is not transactional operation
     */
    virtual iterator* begin(Transaction* trans);

    /**
     * @brief get prev segment in not-full segment list
     */
    SegmentLptr getPrevNfSegment() const { return prev_nf_segment_; }

    /**
     * @brief get next segment in not-full segment list
     */
    SegmentLptr getNextNfSegment() const { return next_nf_segment_; }
    
    /**
     * @brief set next segment in not-full segment list
     */
    void setPrevNfSegment(SegmentLptr segment) { prev_nf_segment_ = segment; }

    /**
     * @brief set next segment in not-full segment list
     */
    void setNextNfSegment(SegmentLptr segment) { next_nf_segment_ = segment; }

    /**
     * @brief serialize this segment
     */
    void serialize(boost::archive::binary_oarchive& oa);

    /**
     * @brief get memory consumpation of this segment
     */
    virtual std::size_t getMemoryUsage();

    /**
     * @brief calculatte segment id that contains slot idx
     */
    static SegmentLptr calculateSgmtId(LogicalPtr idx);
    
    /**
     * @brief calculate segment's starting index and the number of slots
     */
    static void getSgmtStat(SegmentLptr sgmt_id, LogicalPtr& start_idx, LogicalPtr& num_slots);

    friend class paged_array_base;
    template <class T> friend class PagedArrayEncoded;
    friend class PagedArrayVar;
    friend class BitVector;

 protected:
    DISALLOW_COMPILER_GENERATED(Segment);

    static const uint16_t HEADER_SIZE =
        sizeof(uint64_t)                    // vf_table_
        + sizeof(Type)                      // sgmt_type_
        + sizeof(Page::Type)                // page_type_
        + sizeof(uint32_t)                  // paged_array_id_
        + sizeof(uint32_t)                  // dummy1
        + sizeof(uint32_t)                  // num_header_pages_
        + sizeof(uint32_t)                  // num_data_pages_

        + sizeof(uint32_t)                  // max_num_slots_
        + sizeof(uint32_t)                  // slots_per_page_
        + sizeof(uint16_t)                  // slot_width_
        + sizeof(uint16_t)                  // full_flag_

        + sizeof(PageLptr) * NUM_PAGE_LIST  // nf_page_header_
        + sizeof(PageLptr) * NUM_PAGE_LIST  // nf_page_tail_
        + sizeof(uint32_t) * NUM_PAGE_LIST  // num_nf_pages_
        + sizeof(uint64_t)                  // start_index_
        + sizeof(SegmentLptr)               // sgmt_lptr_
        + sizeof(SegmentLptr)               // prev_nf_segment_
        + sizeof(SegmentLptr)               // next_nf_segment_
        + sizeof(uint32_t)                  // dictionary_offset_
        + sizeof(uint32_t) * 5              // for encoded segment
        + sizeof(PhysicalPtr)               // for encoded delta
        + sizeof(Mutex) * 2                 // mx_full_flag_, mx_nf_page_list_
        + sizeof(VersionHashMap);           // version_map_

  private:
    /**
     * @brief check page that contains allocated slot
     * @details called from allocatedSlot/assignSlot.
     *  if allocated page became full, revise not-full page list
     */
    bool checkAllocatedSlot(Page* page, PageLptr page_id, int32_t list_id);

    /**
     * @brief check page that contains released slot
     * @details called from releaseSlot.
     *  if released page was full, revise not-full page list
     */
    bool checkReleasedSlot(Page* page, PageLptr page_id, bool& was_full_sgmt, int32_t list_id);

    /**
     * @brief adjust the segment so that it can no longer allocate slots (for dbg)
     */
    void finalize();

    /**
     * @breif check that the segment has multiple page-list
     */
    inline bool multiPageList() const {
        return (num_data_pages_ > NUM_PAGE_LIST * PAGE_LIST_FACTOR)
            && (page_type_ == Page::FIXED);
    }

    /**
     * @brief consolidate a slot's versions
     * @param uint32_t offset: slot's offset from start_index
     */
    void consolidate(Version* version , uint32_t offset);

    /**
     * @brief release a slot during version consolidate
     * @details when the insertion was aborted or the slot has DELETE version
     */
    void release(Page* page, PageLptr page_id, int32_t offset_in_page);

    template<class T> friend class PagedArray;
  protected:
    /**
     * @brief get a page from the segment
     */
    Page* getPage(PageLptr page_lptr) const;

    /**
     * @brief get the version of a slot
     * @param int64_t start_ts: start timestamp of transaction
     * @param int64_t start_ts: current timestamp of the statement in tx
     * @param uint32_t offset: slot's offset from start_index
     * @param AddressPair slot: slot in page
     */
    AddressPair getVersion(int64_t start_ts, int64_t curr_ts, uint32_t offset, AddressPair slot);

    /**
     * @brief check a slot has version
     * @param uint32_t offset: slot's offset from start_index
     * @return true if the slot has version
     */
    bool getVersionFlag(uint32_t offset);

    /**
     * @brief set a slot has version
     */
    void setVersionFlag(Page* page, int32_t offset_in_page, bool flag);

    /*
     * member variables
     */
    Type                sgmt_type_;
    Page::Type          page_type_;
    uint32_t            pa_id_;
    uint32_t            dummy1;

    const uint32_t      num_header_pages_;
    const uint32_t      num_data_pages_;

    uint32_t            max_num_slots_;
    uint32_t            slots_per_page_;

    uint16_t            slot_width_;
    uint16_t            full_flag_;

    PageLptr            nf_page_header_[NUM_PAGE_LIST];
    PageLptr            nf_page_tail_[NUM_PAGE_LIST];
    uint32_t            num_nf_pages_[NUM_PAGE_LIST];

    const LogicalPtr    start_index_;
    const SegmentLptr   sgmt_lptr_;

    SegmentLptr         prev_nf_segment_;
    SegmentLptr         next_nf_segment_;

    uint32_t            dictionary_offset_;

    // for encoded segment
    uint32_t            bit_length_;
    uint32_t            mask_;
    uint32_t            var_offset_;
    uint32_t            var_size_;
    uint32_t            dict_size_;

    // for encoded delta
    PhysicalPtr         delta_dict_;

    // Mutex
    Mutex               mx_full_flag_;
    Mutex               mx_nf_page_list_;

    VersionHashMap      version_map_;
};

/*
 * inline functions
 */
inline Segment::iterator& Segment::iterator::operator++() {
    do {
        ++slot_idx_;
        slot_ += slot_width_;
    } while (slot_idx_ < last_slots_ && (!page_->isUsed(slot_idx_)));

    if (slot_idx_ >= last_slots_) {
        bool next_exist = getNextPage();
        if (!next_exist) setEnd();
    }

    return *this;
}

inline void Segment::iterator::nextPage() {
    slot_idx_ = last_slots_;
    is_ended_ = !getNextPage();
}

inline AddressPair Segment::iterator::operator*() const {
    if (trans_) {
        AddressPair slot;
        if (page_->getSlot(slot, slot_idx_, 0, false)) {
            return sgmt_->getVersion(trans_->getStartTs(), trans_->getTs(), slot_idx_, slot);
        }
        return slot;
    } else {
        return AddressPair(page_start_idx_ + slot_idx_, slot_);
    }
}

inline Page* Segment::iterator::getPage() const {
    return page_;
}

inline bool Segment::iterator::isValid() const {
    return page_->isUsed(slot_idx_);
}

inline AddressPair Segment::getSlot(uint32_t offset, bool force) {
    int num_slots_per_page = Page::getMaxNumSlots(slot_width_);

    PageLptr page_idx = offset / num_slots_per_page;
    int offset_in_page = offset % num_slots_per_page;
    Page* page = getPage(page_idx);

    AddressPair slot;
    page->getSlot(slot, offset_in_page, -1, force);

    return slot;
}

inline AddressPair Segment::getSlot(Transaction& trans, uint32_t offset, bool force) {
    int num_slots_per_page = Page::getMaxNumSlots(slot_width_);

    PageLptr page_idx = offset / num_slots_per_page;
    int offset_in_page = offset % num_slots_per_page;
    Page* page = getPage(page_idx);

    AddressPair slot;
    while (slot.pptr_ == nullptr) {
        if (page->getSlot(slot, offset_in_page, 0, force)) {
            slot = getVersion(trans.getStartTs(), trans.getTs(), offset, slot);
        }
    }

    return slot;
}

inline bool Segment::isValid(uint32_t offset) {
    int num_slots_per_page = Page::getMaxNumSlots(slot_width_);

    PageLptr page_idx = offset / num_slots_per_page;
    int offset_in_page = offset % num_slots_per_page;

    Page* page = getPage(page_idx);
    return page->isUsed(offset_in_page);
}

inline SegmentLptr Segment::calculateSgmtId(LogicalPtr idx) {
    if (!idx) return 0;
    uint32_t i = __builtin_clzl(idx);
    if (i > 54) return 0;
    else if (i < 44) {
        return 10 + (idx >> 20);
    } else {
        return 54 - i; //i == __builtin_clzl(index);
    }
}

inline void Segment::getSgmtStat(SegmentLptr sgmt_id,
        LogicalPtr& start_idx, LogicalPtr& num_slots) {
    if (sgmt_id == 0) {
        num_slots = BASE_SUB_SGMT_SIZE;
        start_idx = 0;
    } else if (sgmt_id < MAX_SUB_SGMT) {
        num_slots = (uint64_t)BASE_SUB_SGMT_SIZE << (sgmt_id - 1);
        start_idx = num_slots;
    } else {
        num_slots = BASE_SGMT_SIZE;
        start_idx = BASE_SGMT_SIZE * (sgmt_id + 1 - MAX_SUB_SGMT);
    }
}

inline Page* Segment::getPage(PageLptr lptr) const {
    if (lptr == NULL_PAGE_LPTR) {
        return NULL;
    }
    if (lptr >= num_data_pages_) {
        std::string err("page lptr error, larger than max, ");
        RTI_EXCEPTION(err);
    }

    return (Page*)this + num_header_pages_ + lptr;
}

}  // namespace storage

#endif  // STORAGE_BASE_SEGMENT_H_
