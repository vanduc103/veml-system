// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef STORAGE_BASE_PAGE_H_
#define STORAGE_BASE_PAGE_H_

// Project include
#include "storage/def_const.h"
#include "concurrency/mutex_lock.h"

// Other include
#include <boost/static_assert.hpp>

namespace storage {

/**
 * @brief base data structure that contains slots for data
 * @details minimum size of slot is 4 byte.
 * @author Ilju Lee, ijlee@kdb.snu.ac.kr
 */
class Page {
  public:
    /**
     * @brief an enum type of page
     * @li FIXED: page of normal PagedArray
     * @li VAR: page of PagedArrayVar
     * @li ENCODED: page of PagedArrayEncoded
     */
    enum Type { FIXED, VAR, ENCODED };

    /**
     * @brief Page constructor
     * @param Type type: a type of page
     * @param LogicalPtr start_laddr: logical address of the first slot
     * @param uint32_t max_num_slots: the capacity of th page
     * @param uint16_t slot_width: byte size of the slot
     * @param PageLptr prev: previous not-full page
     * @param PageLptr next: next not-full page
     * @param bool dirty: if true, set all slots with 0XFF (used for BitVector impl)
     */
    Page(Type type, LogicalPtr start_laddr,
            uint32_t max_num_slots, uint16_t slot_width,
            PageLptr prev, PageLptr next, bool dirty = false);

    /**
     * @brief get slot from page
     * @param AddressPair slot: returned slot, (null if the valid flag is false)
     * @param uint32_t offset: offset from start_laddr_ (FIXED=index_offset, VAR=byte_offset)
     * @param int64_t ts: transaction timestamp (-1 if it is not transactional operation)
     * @param bool force: if true, get slot regardless valid flag
     * @return true if it has version
     */
    bool getSlot(AddressPair& slot, uint32_t offset, int64_t ts = -1, bool force = false);

    /**
     * @brief get slot without acquiring lock
     * @param uint32_t offset: offset from start_laddr_ (FIXED=index_offset, VAR=byte_offset)
     * @return slot
     */
    AddressPair getSlotNoLock(uint32_t offset);

    /**
     * @brief get logical address of the offset
     * @param uint32_t offset: offset from start_laddr_ (FIXED=index_offset, VAR=byte_offset)
     * @return logical address
     */
    LogicalPtr getLptr(uint32_t offset) { return start_laddr_ + offset; }

    /**
     * @brief allocate any unused slot
     * @param bool is_trans: true if it is transactional operation
     * @return allocated slot (null if the paged has lock or page is full)
     */
    AddressPair allocateSlot(bool is_trans);

    /**
     * @brief allocate designated slot
     * @param int offset: offset of designated slot
     * @param bool is_trans: true if it is transactional operation
     * @return allocated slot
     * @throws exception if designated slot is in use
     */
    AddressPair allocateSlot(int offset, bool is_trans);

    /**
     * @brief relase slot of FIXED page
     * @param int offset: offset of slot to release
     * @param bool was_full: set true if the page was full
     * @return false if the slot is not valid
     */
    bool releaseSlot(uint32_t offset, bool& was_full);

    /**
     * @brief relase slot of VAR page
     * @param int offset: offset of slot to release
     * @param bool was_full: set true if the page was full
     * @return false if the slot is not valid
     */
    bool releaseVarSlot(uint32_t offset, bool& was_full);

    /**
     * @brief update slot
     * @param int offset: offset of slot to update
     * @param PhysicalPtr pptr: new value
     */
    void updateSlot(uint32_t offset, PhysicalPtr pptr);

    /**
     * @brief check whether the page is empty
     * @return true if the page is empty
     */
    inline bool isEmpty() const { return num_slots_ == 0; }

    /**
     * @brief check whether the page is full
     * @return true if the page is full
     */
    inline bool isFull() const { return num_slots_ == max_num_slots_; }

    /**
     * @brief check the page is full (require lock)
     * @return true if the page is full
     */
    bool getFullFlag();

    /**
     * @brief set the full flag of the page (require lock)
     * @param bool set: flag
     */
    void setFullFlag(bool set = true);

    /**
     * @brief check a slot is valid
     * @param uint32_t offset: offset of the slot
     * @return true if the slot is valid
     */
    bool isUsed(uint32_t idx) const;

    /**
     * @brief check a slot has version
     * @param uint32_t offset: offset of the slot
     * @return true if the slot has version
     */
    bool isVersioned(uint32_t idx) const;

    /**
     * @brief get type of the page
     */
    Type getType() const { return type_; }

    /**
     * @brief get prev not-full page
     * @details this page should be in the not-full page list of Semgent
     */
    PageLptr getPrevPage() const { return prev_page_.fixed; }

    /**
     * @brief get next not-full page
     * @details this page should be in the not-full page list of Semgent
     */
    PageLptr getNextPage() const { return next_page_.fixed; }

    /**
     * @brief get next empty page
     * @details this page should be in the empty page list of Semgent
     */
    PageLptr getNextEmptyPage() const { return next_page_.fixed; }

    /**
     * @brief get prev empty varpage
     * @details this page should be in the empty page list of SemgentVar
     */
    LogicalPtr getPrevVarPage() const { return prev_page_.var; }

    /**
     * @brief get next empty varpage
     * @details this page should be in the empty page list of SemgentVar
     */
    LogicalPtr getNextVarPage() const { return next_page_.var; }

    /**
     * @brief get starting logical address of the page
     */
    LogicalPtr getStartLaddr() const { return start_laddr_; }

    /**
     * @brief get capacity of the page
     */
    uint32_t getMaxNumSlots() const { return max_num_slots_; }

    /**
     * @brief get current number of valid slots
     */
    uint32_t getNumSlots() const { return num_slots_; }

    /**
     * @brief get slot width of the page
     */
    uint16_t getSlotWidth() const { return slot_width_; }

    /**
     * @brief get highest index of valid slot
     */
    uint32_t getLastSlot() const;

    /**
     * @brief get byte size of page (except for header)
     */
    static uint16_t getDataSize() { return DATA_SIZE; }

    /**
     * @brief calculaute capacity of the page for the given slot_width
     * @throws if slot_width is greater than size of page
     */
    static uint32_t getMaxNumSlots(uint16_t slot_width);

    /**
     * @brief set previous page in not-full page list
     */
    void setPrevPage(PageLptr prev) { prev_page_.fixed = prev; }

    /**
     * @brief set next page in not-full page list
     */
    void setNextPage(PageLptr next) { next_page_.fixed = next; }

    /**
     * @brief set previous varpage in empty page list of SegmentVar
     */
    void setPrevVarPage(LogicalPtr prev) { prev_page_.var = prev; } 

    /**
     * @brief set next varpage in empty page list of SegmentVar
     */
    void setNextVarPage(LogicalPtr next) { next_page_.var = next; }

    /**
     * @brief set version flag of slot
     */
    void setVersionFlag(uint32_t offset, bool flag);

    /**
     * @brief init page
     */
    void init();

    friend class Segment;
    template <class T>
    friend class SegmentEncoded;

 protected:
    DISALLOW_COMPILER_GENERATED(Page);
    
    /**
     * @brief union data type for page list
     * @details if the page is FIXED type, use 4B integer(PageLptr), else 8B LogicalPtr
     */
    union NextPage {
        PageLptr    fixed;
        LogicalPtr  var;
        NextPage(PageLptr fixed) : fixed(fixed) {}
        NextPage(LogicalPtr var) : var(var) {}
    };

    static const uint16_t DATA_SIZE = PAGE_SIZE
        - sizeof(LogicalPtr)        // start_laddr_
        - sizeof(Type)              // type_
        - sizeof(uint32_t)          // max_num_slots_
        - sizeof(uint32_t)          // num_slots_
        - sizeof(uint16_t)          // slot_width_
        - sizeof(uint16_t)          // full_flag_
        - sizeof(NextPage)          // prev_nf_page_
        - sizeof(NextPage)          // next_nf_page_
        - sizeof(uint64_t) * 60     // used_flag_
        - sizeof(uint64_t) * 60     // version_flag_
        - sizeof(Mutex)             // mx_full_flag_
        - sizeof(Mutex);            // mx_page_lock_

    /**
     * @brief allocate slot
     * @details this is called from public function, allocateSlot
     * @param int idx: index of used_flag array
     * @param int offset: offset in page
     * @param bool is_trans: true if it is transactional operation
     * @return allocated slot
     */
    AddressPair allocate(int idx, int offset, bool is_trans);

    /**
     * @brief release slot
     * @details this is called from public releaseSlot
     * @param uint32_t offset: offset in page
     * @param bool was_full: set true if the page was full
     */
    bool release(uint32_t offset, bool& was_full);

    /**
     * @brief request page lock
     */
    void getLock() { mx_page_lock_.lock(); }
    
    /**
     * @brief release page lock
     */
    void getUnLock() { mx_page_lock_.unlock(); }
    
    /**
     * @brief set the number of valid slots
     * @details this is called from append-only paged array(encoded, bitvector)
     */
    void setNumSlots(uint32_t num_slots) { num_slots_ = num_slots; }

    /**
     * member variables
     */
    const LogicalPtr start_laddr_;
    const Type type_;

    uint32_t max_num_slots_;
    uint32_t num_slots_;

    uint16_t slot_width_;
    uint16_t full_flag_;

    NextPage prev_page_;
    NextPage next_page_;
    uint64_t used_flag_[60];    // 3840 bit
    uint64_t version_flag_[60]; // 3840 bit

    Mutex mx_full_flag_;
    Mutex mx_page_lock_;

    char slot_[DATA_SIZE];
};

BOOST_STATIC_ASSERT(sizeof(Page) == PAGE_SIZE);

inline bool Page::getSlot(AddressPair& slot, uint32_t offset, int64_t ts, bool force) {
    // if the type is VAR, offset is byte addressing
    // else, offset is index of paged array
    LogicalPtr lptr = type_ == VAR?
        start_laddr_ + (slot_ + offset * slot_width_
                - reinterpret_cast<char*>(const_cast<Page*>(this)))
        : start_laddr_ + offset;

    ReadLock read_lock(mx_page_lock_);
    if (!isUsed(offset) && !force) {
        // access to empty slot
        return false;
    }

    slot.lptr_ = lptr;
    if (ts >= 0 && isVersioned(offset)) {
        // slot has version, only return logical address
        return true;
    }

    // slot does not have version or access to slot w/o timestamp
    slot.pptr_ = const_cast<char*>(slot_) + offset * slot_width_;
    return false;
}

inline AddressPair Page::getSlotNoLock(uint32_t offset) {
    // if the type is VAR, offset is byte addressing
    // else, offset is index of paged array
    LogicalPtr lptr = type_ == VAR?
        start_laddr_ + (slot_ + offset * slot_width_
                - reinterpret_cast<char*>(const_cast<Page*>(this)))
        : start_laddr_ + offset;

    // slot does not have version or access to slot w/o timestamp
    return AddressPair(lptr, const_cast<char*>(slot_) + offset * slot_width_);
}

inline bool Page::isUsed(uint32_t idx) const {
    assert(idx < max_num_slots_);

    int word_idx = idx / 64;
    int word_offset = idx % 64;

    return (used_flag_[word_idx] & (1l << word_offset)) != 0;
}

inline bool Page::isVersioned(uint32_t idx) const {
    assert(idx < max_num_slots_);

    int word_idx = idx / 64;
    int word_offset = idx % 64;

    return (version_flag_[word_idx] & (1l << word_offset)) != 0;
}

inline uint32_t Page::getMaxNumSlots(uint16_t slot_width) {
    if (slot_width > DATA_SIZE) {
        std::string err("slot_width error, larger than max, ");
        RTI_EXCEPTION(err);
    }

    return DATA_SIZE / slot_width;
}

}  // namespace storage

#endif  // STORAGE_BASE_PAGE_H_
