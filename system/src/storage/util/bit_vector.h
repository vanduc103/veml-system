// Copyright 2019 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef STORAGE_UTIL_BIT_VECTOR_H_
#define STORAGE_UTIL_BIT_VECTOR_H_

// Related include
#include "storage/base/paged_array.h"

namespace storage {

/**
 * @brief paged_array's variation, vector of boolean flag
 * @details byte width of slot is 4 which contains 32 bit flags
 * @author Ilju Lee, ijlee@kdb.snu.ac.kr
 */
class BitVector : public PABase {
  public:
    /**
     * @brief BitVector iterator
     */
    class iterator : public PABase::iterator {
      public:
        /**
         * @brief BitVector::iterator constructor
         * @param BitVector bv: bit-vector to iterator
         * @param SegmentVec sgmts: ids of segment to iterator
         * @Transaction* trans: tx that call iterator, null if not transactional
         */
        iterator(BitVector* bv, const SegmentVec& sgmts, Transaction* trans);

        /**
         * @brief BitVector::iterator copy constructor
         * @param iterator itr: iterator to copy
         */
        iterator(const iterator& itr) : PABase::iterator(itr) {
            *this = itr;
        }

        /**
         * @brief BitVector copy operator
         * @param iterator itr: iterator to copy
         */
        iterator& operator=(const iterator& itr) {
            sgmts_.clear();
            sgmts_ = itr.sgmts_;
            sgmt_idx_ = itr.sgmt_idx_;

            size_ = itr.size_;
            count_ = itr.count_;
            page_ = itr.page_;

            page_idx_ = itr.page_idx_;
            slot_idx_ = itr.slot_idx_;
            bit_idx_ = itr.bit_idx_;

            num_pages_ = itr.num_pages_;
            num_slots_ = itr.num_slots_;
            is_ended_ = itr.is_ended_;

            return *this;
        }

        /**
         * @brief move to next flag
         */
        iterator& operator++();

        /**
         * @brief get current slot
         * @return slot
         */
        bool operator*() const;

        /**
         * @brief check current flag is true
         * @return current flag
         */
        bool isValid() const { return *(*this); }

        /**
         * @brief get logical address of current flag
         * @return logical address of flag
         */
        LogicalPtr getLptr() const { return page_->getLptr(slot_idx_); }

        EvalValue getValue() { return Bool(isValid()); }

        /**
         * @brief init iterator
         */
        void init();

      private:
        uint64_t    size_;
        uint64_t    count_;

        Page*       page_;

        uint32_t    page_idx_;
        uint32_t    slot_idx_;
        uint32_t    bit_idx_;

        uint32_t    num_pages_;
        uint32_t    num_slots_;
        bool        is_ended_;
    };

    /**
     * @brief BitVector constructor
     */
    BitVector() : paged_array_base(false, 4), size_(0), capacity_(0) {}

    /**
     * @brief BitVector constructor
     * @details constructor for recovery
     * @param uint32_t pa_id: id of this paged_array
     */
    BitVector(uint32_t pa_id)
        : paged_array_base(false, 4), size_(0), capacity_(0) {}

    /**
     * @brief get value from BitVector
     * @return eval value Bool
     */
    EvalValue getValue(LogicalPtr idx);

    /**
     * @brief assign a new flag
     * @return logical address of new flag
     */
    LogicalPtr assignLptr();

    /**
     * @brief check a flag is true
     * @param LogicalPtr logical address of flag
     * @return value of boolean flag
     */
    bool isValid(LogicalPtr idx);

    /**
     * @brief set a value of flag to true
     * @param LogicalPtr logical address of flag
     * @param bool force: if true, set flag regardless validity
     */
    void setValid(LogicalPtr idx, bool force = false);

    /**
     * @brief set a value of flag to flag
     * @param LogicalPtr logical address of flag
     * @param bool force: if true, set flag regardless validity
     */
    void setInvalid(LogicalPtr idx, bool force = false);

    /**
     * @brief get current capacity of bit-vector
     * @return capacity of bit-vector
     */
    uint64_t getCapacity() { return capacity_; }

    /**
     * @brief get the current number of flags in bit-vector
     * @return the number of flags
     */
    uint64_t getSize() { return size_.load(); }

    /**
     * @brief check it is an encoded paged_array
     * @return true
     */
    bool isEncoded() { return true; }

    /**
     * @brief serialize bit-vector to binary
     */
    void serialize(boost::archive::binary_oarchive& oa) const;

    /**
     * @brief deserialize a segment from binary
     */
    void deserialize(SegmentLptr id, boost::archive::binary_iarchive& ia);

    /**
     * @brief reserve paged_array's capacity
     * @param uint64_t size: size to reserve
     * @param bool dirty: if true, set all slots with 0XFF
     */
    void reserve(uint64_t size, bool dirty);

    /**
     * @brief create an iterator for all segments
     * @param Transaction trans: null if not transactional operation
     */
    iterator begin(Transaction* trans = nullptr);

    /**
     * @brief create an iterator for designated segments
     * @param SgmtLptrVec sgmt_ids: ids of segment to iterate
     * @param Transaction trans: null if not transactional operation
     */
    iterator begin(SgmtLptrVec& sgmt_ids, Transaction* trans = nullptr);

  protected:
    /**
     * @brief allocate new segment of BitVector
     * @param bool dirty: if true, set all slots with 0XFF
     * @return segment-id of created 
     */
    SegmentLptr allocateBVSegment(bool dirty);

    /**
     * @brief allocate new segment for specified sgmt-id
     * @param SegmentLptr sgmt_id: id of new segment
     * @param bool dirty: if true, set all slots with 0XFF
     * @return segment-id of created 
     */
    SegmentLptr allocateBVSegment(uint64_t sgmt_id, bool dirty);

    /**
     * @brief get slot contains flag
     * @param LogicalPtr idx: index of flag
     * @param bool set: if true, set the slot is valid
     * @return address of slot
     */
    AddressPair getBitSlot(LogicalPtr idx, bool set = false);

    /**
     * @brief resize of the BitVector
     * @param bool dirty: if true, set all slots with 0XFF
     */
    void resize(bool dirty);

    /*
     * member variables
     */
    std::atomic<uint64_t> size_;
    uint64_t capacity_;
};

/**
 * @brief wrapper of BitVector which is used by processing module
 * @details a BitVector can be divided by multiple BVWrapper
 * @author Ilju Lee, ijlee@kdb.snu.ac.kr
 */
class BVWrapper {
  public:
    /**
     * @brief create a new BVWrapper
     */
    BVWrapper() : bv_(nullptr) {}

    /**
     * @brief BVWrapper constructor
     * @details create a wrapper for whole bit-vector
     * @param BitVector bv: bit-vector
     */
    BVWrapper(BitVector* bv) : bv_(bv), sgmt_ids_(bv->getAllSgmtIds()) {}

    /**
     * @brief BVWrapper constructor
     * @details create a wrapper for specific segments in bit-vector
     * @param SgmtLptrVec sgmt_ids: ids of segment to make wrapper
     * @param BitVector bv: bit-vector
     */
    BVWrapper(BitVector* bv, const SgmtLptrVec& sgmt_ids)
        : bv_(bv), sgmt_ids_(sgmt_ids) {}

    /**
     * @brief begin iterator
     * @param Transaction trans: null if not transactional operation
     * @return iterator
     */
    BitVector::iterator begin(Transaction* trans) {
        return bv_->begin(sgmt_ids_, trans);
    }

  private:
    BitVector* bv_;
    SgmtLptrVec sgmt_ids_;
};

}  // namespace storage

#endif  // STORAGE_UTIL_BIT_VECTOR_H_

