// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef STORAGE_SEGMENT_BASE_ENCODED_H_
#define STORAGE_SEGMENT_BASE_ENCODED_H_

// Related include
#include "storage/base/segment.h"

// Project include
#include "storage/util/btree.h"

namespace storage {

template <class T>
class PagedArrayEncoded;

/**
 * @brief dictionary of dynamic SegmentEncoded
 * @details vid is not ordered by value, assigned by insertion order
 * @author Ilju Lee, ijlee@kdb.snu.ac.kr
 */
template <class T>
class DeltaDict {
  public:
    typedef typename T::eval_type eval;
    /**
     * @brief DeltaDict constructor
     */
    DeltaDict() : value_id_(0) {}

    /**
     * @brief DeltaDict destructor
     */
    ~DeltaDict() {value_map_.release();}

    /**
     * @brief get value-id for value
     * @param eval value: value
     * @param bool insert: if true, update dictionary with new value-vid pair
     */
    int getVid(eval value, bool insert = false);

    /**
     * @brief get value for value-id
     */
    eval getValue(int vid);

    /**
     * @brief get physical pointer of value in dictionary for vid
     */
    PhysicalPtr getValuePtr(int vid);

    /**
     * @brief get the number of values in dictionary
     */
    std::size_t size();

    /**
     * @brief get byte size of DeltaDict
     */
    std::size_t getMemoryUsage();

    /**
     * @brief get byte size of var-size data
     * @details only for Varchar_T data type
     */
    uint32_t getVarSize() { return 0; }

    /**
     * @brief geneate values list and value-id list ordered by value
     * @param std::vector<int> vids: ordering map current vid to ordered vid
     * @param std::vector<eval> values: ordered value list
     */
    void getStaticDict(std::vector<int>& vids, std::vector<eval>& values);

    /**
     * @brief clear dictionary
     */
    void clear();

  private:
    Mutex                   mx_dict_;
    std::vector<eval>       values_;
    BTree<eval, int>        value_map_;
    std::atomic<int32_t>    value_id_;
};

/**
 * @brief compressed segment with dictionary encoding
 * @details DYNAMIC: accept insert only employ dictionary encoding
 * @details STATIC: compress DYNAMIC segment using bit-packing
 * @author Ilju Lee, ijlee@kdb.snu.ac.kr
 */
template <class T>
class SegmentEncoded : public Segment {
  protected:
    /**
     * @brief iterator of encoded segment
     */
    typedef typename T::eval_type eval;
    class iterator : public Segment::iterator {
      public:
        /**
         * @brief create null iterator
         */
        iterator() {}

        /**
         * @brief SegmentEncoded::iterator constructor
         */
        iterator(Segment* sgmt, Transaction* trans);

        /**
         * @brief SegmentEncoded::iterator copy constructor
         */
        iterator(const iterator& itr) : Segment::iterator(itr),
            bit_length_(itr.bit_length_), mask_(itr.mask_) {}

        /**
         * @brief SegmentEncoded::iterator destructor
         */
        virtual ~iterator() {};

        /**
         * @brief SegmentEncoded::iterator copy operator
         */
        iterator& operator=(const iterator& itr);

        /**
         * @brief move to next slot
         */
        iterator& operator++();

        /**
         * @brief get value-id of current slot
         */
        int getVid() const;

        /**
         * @brief get value of current slot
         */
        eval getValue() const;

        /**
         * @brief init iterator
         */
        void init();

        /**
         * @brief change current slot
         * @details used to start the iteration from the middle of the segment
         * @param LogicalPtr lptr: index of new current slot
         */
        void setLptr(LogicalPtr lptr);
        
      private:
        /**
         * @brief move to next page of the segment
         */
        bool getNextPage();

        uint32_t bit_length_;
        uint32_t mask_;
    };

  public:
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
     * @param uint32_t bit_length: length of bit-packing (only for STATIC)
     * @param uint32_t mask: encoding mask for bit-packing (only for STATIC)
     * @param uint32_t var_offset: byte offset for var-size data from header (only for STATIC)
     * @param uint32_t var_size: byte size of al var-size data (only for STATIC)
     * @param uint32_t dict_size: the number of values in dictionary (only for STATIC)
     * @param PhysicalPtr delta_dict: physical ptr for DeltaDict (only for DYNAMIC)
     */
    SegmentEncoded(Type sgmt_type, Page::Type page_type, uint32_t pa_id,
            uint32_t num_header_pages, uint32_t num_data_pages,
            uint32_t num_slots, uint32_t slots_per_page, uint16_t slot_width,
            uint64_t start_index, SegmentLptr sgmt_lptr, uint32_t dict_offset,
            uint32_t bit_length, uint32_t mask, uint32_t var_offset, uint32_t var_size,
            uint32_t dict_size, PhysicalPtr delta_dict);

    /**
     * @brief insert a single record to segment (only for DYNAMIC)
     * @param uint32_t offset: slot's offset from start_index
     */
    void insert(uint32_t offset, eval value);

    /**
     * @brief insert a single record to segment (transactonal, only for DYNAMIC)
     * @param uint32_t offset: slot's offset from start_index
     * @todo: impl create version
     */
    void insert(Transaction& trans, uint32_t offset, eval value);

    /**
     * @brief create a iterator of this segment
     * @param Transaction* trans: tx, null if it is not transactional operation
     */
    Segment::iterator* begin(Transaction* trans);

    /**
     * @brief get value-id of a slot
     * @param uint32_t offset: slot's offset from start_index
     */
    int readVid(uint32_t offset);

    /**
     * @brief get value-id for a value
     * @return integer value-id, -1 if not existing value
     */
    int exact_search(const EvalValue& val);

    /**
     * @brief binary search in dictionary
     * @param bool exact: return true if found value is equal to input
     */
    int lower_bound(const EvalValue& val, bool& exact);

    /**
     * @brief get byte size of memory consumption
     */
    std::size_t getMemoryUsage();

    friend class PagedArrayEncoded<T>;

  protected:
    DISALLOW_COMPILER_GENERATED(SegmentEncoded);
    static const uint32_t DELTA_BIT_LENGTH = 32;  // log2(BASE_SEG_SIZE)
    static const uint32_t DELTA_MASK = std::numeric_limits<uint32_t>::max();

    /**
     * @detail get physical pointer of dictionary
     */
    PhysicalPtr getDictPtr(int vid);

    /**
     * @detail get value for value-id
     */
    eval getValueFromVid(int vid);

    /**
     * @detail write dictionary's values to header of STATIC SegmentEncoded
     */
    void writeEmbeddedDict(std::vector<eval>& values);

    /**
     * @detail write value-id to slots in STATIC SegmentEncoded
     */
    void writeVidSequence(
            std::vector<int>& vids, std::vector<int>& ordered_vids);
};

template <>
int DeltaDict<Varchar_T>::getVid(String value, bool insert);

template <>
int DeltaDict<Char_T>::getVid(String value, bool insert);

template <>
uint32_t DeltaDict<Varchar_T>::getVarSize();

template <>
void SegmentEncoded<Varchar_T>::writeEmbeddedDict(std::vector<String>& values);

}  // namespace storage

#include "storage/base/segment_encoded.inl"

#endif  // STORAGE_SEGMENT_BASE_ENCODED_H_
