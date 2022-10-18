// Copyright 2019 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef STORAGE_BASE_PAGED_ARRAY_ENCODED_H_
#define STORAGE_BASE_PAGED_ARRAY_ENCODED_H_

// Related include
#include "storage/base/paged_array.h"

// Project include
#include "storage/base/segment_encoded.h"

namespace storage {

/**
 * @brief paged_array's variation which is compressed
 * @details employ dictionary encoding and bit-packing
 * @details segments are divided into updateable DYNAMIC and read-only STATIC
 * @details DYNAMIC segments can be compressed to STATIC
 * @author Ilju Lee, ijlee@kdb.snu.ac.kr
 */
template <class T>
class PagedArrayEncoded : public PABase {
  public:
    typedef typename T::eval_type eval;

    /**
     * @brief PagedArrayEncoded iterator
     */
    class iterator : public paged_array_base::iterator {
      public:
        /**
         * @brief create null iterator
         */
        iterator() : paged_array_base::iterator() {}

        /**
         * @brief PagedArrayEncoded::iterator constructor
         * @param SegmentVec sgmts: ids of segment to iterator
         * @Transaction* trans: tx that call iterator, null if not transactional
         */
        iterator(const SegmentVec& sgmts, Transaction* trans);

        /**
         * @brief PagedArrayEncoded:iterator copy constructor
         */
        iterator(const iterator& other) {
            typedef typename SegmentEncoded<T>::iterator SgmtItr;

            sgmts_.clear();
            sgmts_ = other.sgmts_;
            sgmt_itr_ = (other.sgmt_itr_)?
                new SgmtItr(*reinterpret_cast<SgmtItr*>(other.sgmt_itr_))
                : nullptr;
            sgmt_idx_ = other.sgmt_idx_;
            is_ended_ = other.is_ended_;
            trans_ = other.trans_;
        }

        /**
         * @brief PagedArrayEncoded:iterator copy constructor(not const)
         */
        iterator(iterator&& other) : iterator() {
            swap(*this, other);
        }

        /**
         * @brief PagedArrayEncoded::iterator destructor
         */
        ~iterator() {
            typedef typename SegmentEncoded<T>::iterator SgmtItr;
            if (sgmt_itr_ != nullptr)
                delete reinterpret_cast<SgmtItr*>(sgmt_itr_);
            sgmt_itr_ = nullptr;
        }

        /**
         * @brief PagedArrayEncoded::iterator copy operator
         */
        iterator& operator=(iterator other) {
            swap(*this, other);
            return *this;
        }

        /**
         * @brief move to next slot
         */
        iterator& operator++();

        /**
         * @brief change current slot
         * @details used to start the iteration from the middle of the segment
         * @param LogicalPtr lptr: index of new current slot
         */
        void setLptr(LogicalPtr lptr);

        /**
         * @brief read value from current slot
         */
        eval getValue() {
            typedef typename SegmentEncoded<T>::iterator SgmtItr;
            return reinterpret_cast<SgmtItr*>(sgmt_itr_)->getValue();
        }

        /**
         * @brief read value-id from current slot
         */
        int getVid() {
            typedef typename SegmentEncoded<T>::iterator SgmtItr;
            return reinterpret_cast<SgmtItr*>(sgmt_itr_)->getVid();
        }
    };

    /**
     * @brief PagedArrayEncoded constructor
     */
    PagedArrayEncoded() : paged_array_base(false, 4) {}

    /**
     * @brief PagedArrayEncoded constructor
     * @details constructor for recovery
     * @param uint32_t pa_id: id of this paged_array
     */
    PagedArrayEncoded(uint32_t pa_id) : paged_array_base(false, 4) {}

    /**
     * @brief insert an element to designated slot
     */
    AddressPair insert(eval val, LogicalPtr idx);

    /**
     * @brief insert an element to designated slot (transactional)
     */
    AddressPair insert(Transaction& trans, eval val, LogicalPtr idx);

    /**
     * @brief read value from a slot
     */
    eval operator[](LogicalPtr idx);

    /**
     * @brief read value from a slot
     */
    EvalValue getValue(LogicalPtr idx);

    /**
     * @brief read value-id from a slot
     */
    int getVid(LogicalPtr idx);

    /**
     * @brief check this is a PagedArrayEncoded
     */
    bool isEncoded() { return true; }

    /**
     * @brief compress all DYNAMIC segment to STATIC
     */
    void compressDelta();

    /**
     * @brief compress a specific DYNAMIC segment to STATIC
     * @todo impl concurrent work
     */
    bool compressDelta(SegmentLptr sgmt_id);

    /**
     * @brief create an iterator for all segments
     * @param Transaction trans: null if not transactional operation
     */
    iterator begin(Transaction* trans = nullptr);

    /**
     * @brief create an iterator from specific index
     * @param LogicalPtr lptr: starting index of iterator
     * @param Transaction trans: null if not transactional operation
     */
    iterator begin(LogicalPtr lptr, Transaction* trans = nullptr);

    /**
     * @brief create an iterator for designated segments
     * @param Transaction trans: null if not transactional operation
     */
    iterator begin(SgmtLptrVec& sgmt_ids, Transaction* trans = nullptr);

    void deserialize(SegmentLptr id,
            boost::archive::binary_iarchive& ia);

    friend class ColumnTable;

  protected:
    /**
     * @brief create a new DYNAMIC encoded segment
     */
    SegmentLptr allocateEncodedSegment();

    /**
     * @brief create a new STATIC encoded segment
     */
    SegmentEncoded<T>*
    allocateStaticSegment(SegmentLptr sgmt_id, LogicalPtr start,
            uint32_t num_records, uint32_t dict_size, uint32_t var_size);
};

}  // namespace storage

#include "storage/base/paged_array_encoded.inl"

#endif  // STORAGE_BASE_PAGED_ARRAY_ENCODED_H_
