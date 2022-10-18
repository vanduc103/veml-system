// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef STORAGE_BASE_PAGED_ARRAY_H_
#define STORAGE_BASE_PAGED_ARRAY_H_

// Project include
#include "storage/base/segment.h"
#include "storage/util/segment_map.h"

class Transaction;

namespace persistency {
    class ArchiveManager;
}

namespace dan {
    class PABinary;
}

namespace storage {

typedef std::vector<Segment*> SegmentVec;

/**
 * @brief base implemtation of paged_array<p>
 * @li data structure underlying RTI's various data model stores<p>
 * @li logically resizable array data structure<p>
 * @li supports version management and logging for its slot<p>
 *
 * @author Ilju Lee, ijlee@kdb.snu.ac.kr
 */
class paged_array_base {
  public:
    /**
     * @brief paged_array iterator
     */
    class iterator {
      public:
        /**
         * @brief create null iterator
         */
        iterator() : sgmt_itr_(nullptr),
            sgmt_idx_(0), is_ended_(true), trans_(nullptr) {}

        /**
         * @brief paged_array::iterator constructor
         * @param SegmentVec sgmts: ids of segment to iterator
         * @Transaction* trans: tx that call iterator, null if not transactional
         */
        iterator(const SegmentVec& sgmts, Transaction* trans);

        /**
         * @brief paged_array:iterator copy constructor
         */
        iterator(const iterator& other) {
            sgmts_.clear();
            sgmts_ = other.sgmts_;
            sgmt_itr_ = (other.sgmt_itr_)?
                new Segment::iterator(*other.sgmt_itr_) : nullptr;
            sgmt_idx_ = other.sgmt_idx_;
            is_ended_ = other.is_ended_;
            trans_ = other.trans_;
        }

        /**
         * @brief paged_array:iterator copy constructor(not const)
         */
        iterator(iterator&& other) : iterator() {
            swap(*this, other);
        }

        /**
         * @brief swap paged_array::iterator
         */
        friend void swap(iterator& itr1, iterator& itr2) throw() {
            using std::swap;
            swap(itr1.sgmts_, itr2.sgmts_);
            swap(itr1.sgmt_itr_, itr2.sgmt_itr_);
            swap(itr1.sgmt_idx_, itr2.sgmt_idx_);
            swap(itr1.is_ended_, itr2.is_ended_);
            swap(itr1.trans_, itr2.trans_);
        }

        /**
         * @brief paged_array::iterator destructor
         */
        virtual ~iterator();

        /**
         * @brief paged_array::iterator copy operator
         */
        virtual iterator& operator=(const iterator& itr);

        /**
         * @brief move to next slot
         */
        virtual iterator& operator++();

        /**
         * @brief move to next slot
         */
        void next() { ++(*this); }

        /**
         * @brief move to next page
         */
        void nextPage();

        /**
         * @brief get current slot
         * @return slot
         */
        AddressPair operator*() const;

        /**
         * @brief get logical address of current slot
         * @return logical address of slot
         */
        LogicalPtr getLptr() const;

        /**
         * @brief get physical address of current slot
         * @return physical address of slot
         */
        PhysicalPtr getPptr() const;

        /**
         * @brief get current page
         */
        Page* getPage() const;

        /**
         * @brief check current slot is valid
         * @return true if current slot is valid
         */
        bool isValid() const;

        /**
         * @brief check the iteration is finished
         */
        operator bool() const { return !is_ended_; }

        /**
         * @brief init iterator
         */
        void init();
        
        /**
         * @brief change current slot
         * @details used to start the iteration from the middle of the segment
         * @param LogicalPtr lptr: index of new current slot
         */
        virtual void setLptr(LogicalPtr lptr);

      protected:
        /**
         * @brief set flag for end of iteration
         */
        void setEnd() { is_ended_ = true; }

        SegmentVec          sgmts_;
        Segment::iterator*  sgmt_itr_;
        SegmentLptr         sgmt_idx_;
        bool                is_ended_;

        Transaction*        trans_;
    };

    /**
     * @brief paged_array constructor
     * @param bool is_metadata: true if this is paged_array for metadata
     * @param uint16_t slot_width: byte size of the slot
     */
    paged_array_base(bool is_metadata, uint16_t slot_width);

    /**
     * @brief paged_array constructor for recovery
     * @param uint32_t id: id of paged_array
     * @param bool is_metadata: true if this is paged_array for metadata
     * @param uint16_t slot_width: byte size of the slot
     */
    paged_array_base(uint32_t id, bool is_metadata, uint16_t slot_width);

    /**
     * @brief paged_array destructor
     */
    virtual ~paged_array_base();

    /**
     * @brief insert an element to any unused slot
     * @param EvalValue val: value to insert
     * @return logical and physical address of slot
     */
    AddressPair insert(EvalValue& val);

    /**
     * @brief insert an element to designated slot
     * @param EvalValue val: value to insert
     * @param LogicalPtr idx: index of slot to insert
     * @return address of slot, null address if the slot is in use
     */
    AddressPair insert(EvalValue& val, LogicalPtr idx);

    /**
     * @brief insert an element to any unused slot (transactional operation)
     * @param Transaction trans: transaction
     * @param EvalValue val: value to insert
     * @return logical and physical address of slot
     */
    AddressPair insert(Transaction& trans, EvalValue& val);

    /**
     * @brief insert an element to designated slot (transactional operation)
     * @param Transaction trans: transaction
     * @param EvalValue val: value to insert
     * @param LogicalPtr idx: index of slot to insert
     * @return address of slot, null address if the slot is in use
     */
    AddressPair insert(Transaction& trans, EvalValue& val, LogicalPtr idx);

    /**
     * @brief erase an element
     * @param LogicalPtr idx: index of slot to erase
     * @return true if success
     */
    virtual bool erase(LogicalPtr idx);

    /**
     * @brief erase an element (transactional operation)
     * @param Transaction trans: transaction
     * @param LogicalPtr idx: index of slot to erase
     * @return true if success
     */
    virtual bool erase(Transaction& trans, LogicalPtr idx);

    /**
     * @brief truncate all elements
     */
    void truncate();

    /**
     * @brief update a slot (transactional operation)
     * @param Transaction trans: transaction
     * @param LogicalPtr idx: index of slot to update
     * @param EvalValue val: new values
     */
    bool update(Transaction& trans, LogicalPtr idx, EvalValue& val);

    /**
     * @brief get slot from paged_array
     * @param LogicalPtr idx: index of slot
     * @param bool force: true if get slot regardless valid
     * @return AddressPair: logical and physical address of slot
     */
    AddressPair getSlot(LogicalPtr idx, bool force = false);

    /**
     * @brief get slot from paged_array (transactional operation)
     * @param LogicalPtr idx: index of slot
     * @param bool force: true if get slot regardless valid
     * @return AddressPair: logical and physical address of slot
     */
    AddressPair getSlot(Transaction& trans, LogicalPtr idx, bool force = false);

    /**
     * @brief check the slot is in used
     */
    bool isValid(LogicalPtr idx);

    /**
     * @brief get value from paged_array (for element type Tuple)
     * @param LogicalPtr idx: index of slot
     * @param ValueType vt: type of value
     * @param int32_t offset: byte offset of value in slot
     */
    EvalValue getValue(LogicalPtr idx, ValueType vt, int32_t offset);

    /**
     * @brief get value from paged_array
     * @param LogicalPtr idx: index of slot
     */
    virtual EvalValue getValue(LogicalPtr idx) = 0;

    /**
     * @check return true if it is PagedArrayEncoded
     */
    virtual bool isEncoded() { return false; }

    /**
     * @brief get the current number of segments
     */
    uint32_t getNumSgmts() const { return sgmt_map_.size(); }
    
    /**
     * @brief get the current number of valid slots
     * @return the number of valid slots
     */
    uint64_t getNumElement();

    /**
     * @brief get id of paged_array
     */
    uint32_t getId() const { return id_; }

    /**
     * @brief get starting index of segment and page
     */
    uint64_t getStartLaddr(SegmentLptr sgmt_id, PageLptr page_id);

    /**
     * @brief get type of segment
     */
    Segment::Type getSegmentType(SegmentLptr sgmt_id);

    /**
     * @brief get ids of all segments
     */
    SgmtLptrVec getAllSgmtIds() const;

    /**
     * @brief create an iterator for all segments
     * @param Transaction trans: null if not transactional operation
     */
    iterator begin(Transaction* trans = nullptr) const;

    /**
     * @brief create an iterator from specific index
     * @param LogicalPtr lptr: starting index of iterator
     * @param Transaction trans: null if not transactional operation
     */
    iterator begin(LogicalPtr lptr, Transaction* trans = nullptr) const;

    /**
     * @brief create an iterator for designated segments
     * @param SgmtLptrVec sgmt_ids: ids of segment to iterate
     * @param Transaction trans: null if not transactional operation
     */
    iterator begin(SgmtLptrVec& sgmt_ids, Transaction* trans = nullptr) const;

    /**
     * @brief consolidate a slot's versions
     * @param LogicalPtr idx: index of slot to consolidate
     */
    void consolidateVersion(LogicalPtr idx);

    /**
     * @brief serialize paged_array to binary
     */
    virtual void serialize(boost::archive::binary_oarchive& oa) const;
    void serialize(SegmentLptr id, boost::archive::binary_oarchive& oa) const;

    /**
     * @brief deserialize paged_array from binary
     */
    virtual void deserialize(boost::archive::binary_iarchive& ia);
    virtual void deserialize(SegmentLptr id,
                    boost::archive::binary_iarchive& ia) = 0;

    /**
     * @brief recover paged_array from files
     */
    bool recover(std::string path);
    bool recover(std::string& header, StrVec& sgmts);

    /**
     * @brief reserve paged_array's capacity
     * @param uint64_t size: size to reserve
     * @param bool dirty: if true, set all slots with 0XFF (used for BitVector impl)
     */
    void reserve(uint64_t size, bool dirty = false);

    /**
     * @brief get byte size of memory consumption
     * @return byte size of memory
     */
    virtual size_t getMemoryUsage();

    /**
     * @brief set the segments so that they can no longer allocate (for dbg)
     */
    void finalizeCurrentSgmt();

    /**
     * @brief read value from slot
     * @param PhysicalPtr slot: slot
     * @param FieldInfo fi: field info of relational table
     * @retrun value in slot
     */
    static EvalValue getValueFromSlot(PhysicalPtr slot, const FieldInfo* fi);

    /**
     * @brief read value from slot
     * @param PhysicalPtr slot: slot
     * @param ValueType vt: value type of slot
     * @retrun value in slot
     */
    static EvalValue getValueFromSlot(PhysicalPtr slot, ValueType vt);

    friend class RowTable;
    friend class Segment;
    friend class persistency::ArchiveManager;

  protected:
    /**
     * @brief allocate any unused slot and insert an element
     * @param PhysicalPtr val: physical value to insert
     * @return logical addressed of new slot
     */
    AddressPair insertSlot(PhysicalPtr val);

    /**
     * @brief allocate designated slot and insert an element
     * @param PhysicalPtr val: physical value to insert
     * @param LogicalPtr idx: logical address of new slot
     * @return logical addressed of new slot
     */
    AddressPair insertSlot(PhysicalPtr val, LogicalPtr idx);

    /**
     * @brief allocate any unused slot and insert an element (transactional)
     * @param Transaction trans: transaction
     * @param PhysicalPtr val: physical value to insert
     * @return logical addressed of new slot
     */
    AddressPair insertSlot(Transaction& trans, PhysicalPtr val);

    /**
     * @brief allocate designated slot and insert an element (transactional)
     * @param Transaction trans: transaction
     * @param PhysicalPtr val: physical value to insert
     * @param LogicalPtr idx: logical address of new slot
     * @return logical addressed of new slot
     */
    AddressPair insertSlot(Transaction& trans, PhysicalPtr val, LogicalPtr idx);

    /**
     * @brief allocate any unused slot
     * @param uint32_t list_id: id of page-list
     * @param bool is_trans: true if it is transacational operation
     * @return logical addressed of new slot
     */
    AddressPair allocateSlot(uint32_t list_id, bool is_trans);

    /**
     * @brief allocate designated slot
     * @param LogicalPtr idx: requested index of new slot
     * @param bool is_trans: true if it is transacational operation
     * @return logical addressed of new slot
     */
    AddressPair assignSlot(LogicalPtr idx, bool is_trans);

    /**
     * @brief allocate new segment
     * @details id of segment is assigned by next_sgmt_id_
     * @return segment-id of created 
     */
    SegmentLptr allocateSegment();

    /**
     * @brief allocate new segment for specified sgmt-id
     * @param SegmentLptr sgmt_id: id of new segment
     * @return segment-id of created 
     */
    SegmentLptr allocateSegment(SegmentLptr sgmt_id);

    /**
     * @brief create segment
     */
    SegmentLptr createSegment(SegmentLptr sgmt_id);

    /**
     * @brief check segment that contains newly allocated slot
     * @details if segment becomes full, revise not-full segment list
     */
    void checkAllocatedSgmt(Segment* sgmt, SegmentLptr sgmt_id);

    /**
     * @brief check segment that contains slot released
     * @details if segment was full, revise not-full segment list
     */
    void checkReleaseSlot(Segment* sgmt, SegmentLptr sgmt_id);

    /**
     * @brief get segment from paged_array
     */
    Segment* getSegment(SegmentLptr sgmt_id) const;

    SegmentMap  sgmt_map_;

    uint64_t    next_sgmt_id_;
    uint32_t    id_;
    uint16_t    slot_width_;
    int64_t     var_id_;

    Mutex       mx_sgmt_list_;
};

class PagedArrayVar;

/**
 * @brief paged_array for variaous data type
 * @author Ilju Lee, ijlee@kdb.snu.ac.kr
 */
template <class T>
class PagedArray : public paged_array_base {
  public:
    typedef typename T::eval_type eval;
    typedef typename T::stor_type stor;

    /**
     * @brief PagedArray<T>::iterator
     */
    class iterator : public paged_array_base::iterator {
      public:
        /**
         * @brief create null iterator
         */
        iterator() : paged_array_base::iterator() {}

        /**
         * @brief PagedArray<T>::iterator constructor
         * @param SegmentVec sgmts: ids of segment to iterator
         * @Transaction* trans: tx that call iterator, null if not transactional
         */
        iterator(SegmentVec& sgmts, Transaction* trans)
            : paged_array_base::iterator(sgmts, trans) {}

        /**
         * @brief PagedArray<T>:iterator copy constructor
         */
        iterator(const paged_array_base::iterator& other)
            : paged_array_base::iterator(other) {}

        /**
         * @brief read value from current slot
         */
        eval getValue() {
            return reinterpret_cast<T*>(this->getPptr())->getValue();
        }
    };

    /**
     * @brief PagedArray<T> constructor
     * @param bool is_metadata: true if this is paged_array for metadata
     * @param uint16_t slot_width, if 0 set as sizeof(T)
     */
    PagedArray(bool is_metadata = false, uint16_t slot_width = 0);

    /**
     * @brief PagedArray<T> constructor for recovery
     * @param uint32_t id: id of paged_array
     * @param bool is_metadata: true if this is paged_array for metadata
     * @param uint16_t slot_width, if 0 set as sizeof(T)
     */
    PagedArray(uint32_t id, bool is_metadata = false, uint16_t slot_width = 0);

    /**
     * @brief PagedArray<T> constructor
     * @details constructor for recovery
     */

    /**
     * @brief PagedArray<T> destructor
     */
    ~PagedArray() {};

    /**
     * @brief insert an element to any unused slot
     */
    AddressPair insert(eval val);

    /**
     * @brief insert an element to designated slot
     */
    AddressPair insert(eval val, LogicalPtr idx);

    /**
     * @brief insert an element to any unused slot (transactional)
     */
    AddressPair insert(Transaction& trans, eval val);

    /**
     * @brief insert an element to designated slot (transactional)
     */
    AddressPair insert(Transaction& trans, eval val, LogicalPtr idx);

    /**
     * @brief get storage_type value of a slot
     */
    stor& operator[](LogicalPtr idx);

    /**
     * @breif read value from a slot
     */
    EvalValue getValue(LogicalPtr idx);

    /**
     * @brief set value on slot (for Array impl)
     * @details get slot regardless valid, then set value
     */
    void setValue(LogicalPtr idx, eval val);

    /**
     * @brief copy elements to std vector
     */
    void copyTo(LongVec& to, LogicalPtr start=0,
                LogicalPtr end=LOGICAL_NULL_PTR);

    /**
     * @brief create an iterator for all segments
     * @param Transaction trans: null if not transactional operation
     */
    iterator begin(Transaction* trans = nullptr) const;

    /**
     * @brief create an iterator from specific index
     * @param LogicalPtr lptr: starting index of iterator
     * @param Transaction trans: null if not transactional operation
     */
    iterator begin(LogicalPtr lptr, Transaction* trans = nullptr) const;

    /**
     * @brief create an iterator for designated segments
     * @param SgmtLptrVec sgmt_ids: ids of segment to iterate
     * @param Transaction trans: null if not transactional operation
     */
    iterator begin(SgmtLptrVec& sgmt_ids, Transaction* trans = nullptr) const;

    void deserialize(SegmentLptr id,
            boost::archive::binary_iarchive& ia);

    /**
     * @brief get byte size of memory consumption
     */
    size_t getMemoryUsage();

  protected:
    friend class RowTable;

    /**
     * @brief set dedicated PagedArrayVar for var-size data
     */
    void setVarSlots(int64_t var_id, PagedArrayVar* var_slots) {
        var_id_ = var_id;
        var_slots_ = var_slots;
    }

    PagedArrayVar*  var_slots_;
};

typedef paged_array_base PABase;

}  // namespace storage

#include "storage/base/paged_array.inl"

#endif  // STORAGE_BASE_PAGED_ARRAY_H_
