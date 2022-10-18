// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

// Project include
#include "persistency/archive_manager.h"

// C & C++ system include
#include <fstream>

// Other include
#include <boost/serialization/binary_object.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>

namespace storage {

/*
 * dml interface
 */
template <class T>
PagedArrayEncoded<T>::iterator::iterator(
        const SegmentVec& sgmts, Transaction* trans)
        : paged_array_base::iterator() {
    sgmts_ = sgmts;
    sgmt_itr_ = nullptr;
    sgmt_idx_ = 0;
    is_ended_ = true;
    trans_ = trans;

    while (sgmt_idx_ < sgmts_.size()) {
        sgmt_itr_ = reinterpret_cast<SegmentEncoded<T>*>(sgmts_[sgmt_idx_])->begin(trans_);
        is_ended_ = !(*sgmt_itr_);
        if (!is_ended_) break;
        delete reinterpret_cast<typename SegmentEncoded<T>::iterator*>(sgmt_itr_);
        sgmt_itr_ = nullptr;
        sgmt_idx_++;
    }
}


template <class T>
inline typename PagedArrayEncoded<T>::iterator&
PagedArrayEncoded<T>::iterator::operator++() {
    if (is_ended_) {
        return *this;
    }
    while (true) {
        ++(*reinterpret_cast<typename SegmentEncoded<T>::iterator*>(sgmt_itr_));

        if (!(*reinterpret_cast<typename SegmentEncoded<T>::iterator*>(sgmt_itr_))) {
            if (++sgmt_idx_ == sgmts_.size()) {
                setEnd();
            } else {
                sgmt_itr_ = sgmts_[sgmt_idx_]->begin(trans_);
                if (!(*reinterpret_cast<typename SegmentEncoded<T>::iterator*>(sgmt_itr_)))
                    continue;
            }
            return *this;
        }
        return *this;
    }
    assert(false && "it can't be reached");
}

template <class T>
void PagedArrayEncoded<T>::iterator::setLptr(LogicalPtr lptr) {
    if (sgmts_.size() == 0 && lptr == 0) {
        if (sgmt_itr_ != nullptr)
            delete reinterpret_cast<typename SegmentEncoded<T>::iterator*>(sgmt_itr_);
        is_ended_ = true;
        return;
    } // possible but ended
    SegmentLptr sgmt_id = Segment::calculateSgmtId(lptr);
    SegmentEncoded<T>* sgmt = nullptr;
    sgmt_idx_ = 0;
    for (;sgmt_idx_ < sgmts_.size(); ++sgmt_idx_) {
        if (sgmts_[sgmt_idx_]->getSegmentLptr() == sgmt_id) {
            sgmt = reinterpret_cast<SegmentEncoded<T>*>(sgmts_[sgmt_idx_]);
            break;
        }
    }
    if (sgmt_idx_ == sgmts_.size()) {
        std::string err("invalid index @PagedArrayEncoded::iterator::setLptr, ");
        RTI_EXCEPTION(err);
    }
    if (sgmt_itr_ != nullptr) delete reinterpret_cast<typename SegmentEncoded<T>::iterator*>(sgmt_itr_);
    sgmt_itr_ = sgmt->begin(trans_);
    reinterpret_cast<typename SegmentEncoded<T>::iterator*>(sgmt_itr_)->setLptr(lptr);
}

template <class T>
AddressPair PagedArrayEncoded<T>::insert(typename T::eval_type val, LogicalPtr idx) {
    SegmentLptr sgmt_id = Segment::calculateSgmtId(idx);
    SegmentEncoded<T>* sgmt = nullptr;

    while (true) {
        sgmt = reinterpret_cast<SegmentEncoded<T>*>(getSegment(sgmt_id));
        if (sgmt != nullptr)
            break;
        allocateEncodedSegment();
    }

    uint32_t offset = idx - sgmt->getStartIndex();
    sgmt->insert(offset, val);

    checkAllocatedSgmt(sgmt, sgmt_id);

    return {idx, nullptr};
}

template <class T>
AddressPair PagedArrayEncoded<T>::insert(Transaction& trans,
                typename T::eval_type val, LogicalPtr idx) {
    SegmentLptr sgmt_id = Segment::calculateSgmtId(idx);
    SegmentEncoded<T>* sgmt = nullptr;

    while (true) {
        sgmt = reinterpret_cast<SegmentEncoded<T>*>(getSegment(sgmt_id));
        if (sgmt != nullptr)
            break;
        allocateEncodedSegment();
    }

    uint32_t offset = idx - sgmt->getStartIndex();
    sgmt->insert(trans, offset, val);

    checkAllocatedSgmt(sgmt, sgmt_id);

    return {idx, nullptr};
}

/*
 * search interface
 */
template <class T>
typename T::eval_type PagedArrayEncoded<T>::operator[](LogicalPtr idx) {
    SegmentLptr sgmt_id = Segment::calculateSgmtId(idx);
    if (sgmt_id >= sgmt_map_.size()) {
        std::string err("invalid index @PagedArrayEncoded[], ");
        RTI_EXCEPTION(err);
    }

    SegmentEncoded<T>* sgmt = reinterpret_cast<SegmentEncoded<T>*>(getSegment(sgmt_id));
    uint32_t offset = idx - sgmt->getStartIndex();
    
    int vid = sgmt->readVid(offset);
    return sgmt->getValueFromVid(vid);
}

template <class T>
EvalValue PagedArrayEncoded<T>::getValue(LogicalPtr idx) {
    return (*this)[idx];
}

template <class T>
int PagedArrayEncoded<T>::getVid(LogicalPtr idx) {
    SegmentLptr sgmt_id = Segment::calculateSgmtId(idx);
    if (sgmt_id >= sgmt_map_.size()) {
        std::string err("invalid index @PagedArrayEncoded[], ");
        RTI_EXCEPTION(err);
    }

    SegmentEncoded<T>* sgmt = reinterpret_cast<SegmentEncoded<T>*>(getSegment(sgmt_id));
    uint32_t offset = idx - sgmt->getStartIndex();
    
    return sgmt->readVid(offset);
}

template <class T>
void PagedArrayEncoded<T>::compressDelta() {
    for (int i = 0; i < sgmt_map_.size(); i++) {
        Segment* sgmt = getSegment(i);
        if (sgmt && sgmt->getSegmentType() == Segment::DYNAMIC) {
            compressDelta(i);
        }
    }
}

template <class T>
bool PagedArrayEncoded<T>::compressDelta(SegmentLptr sgmt_id) {
    // create new static segment
    Segment* old_sgmt = sgmt_map_[sgmt_id];
    if (old_sgmt->getSegmentType() == Segment::STATIC) return true;

    LogicalPtr start_index = old_sgmt->getStartIndex();
    uint32_t num_records = old_sgmt->getNumSlots();

    DeltaDict<T>* dict = reinterpret_cast<DeltaDict<T>*>(old_sgmt->getDict());
    uint32_t dCount = dict->size();
    uint32_t var_size = dict->getVarSize();

    SegmentEncoded<T>* new_sgmt = allocateStaticSegment(sgmt_id,
                                    start_index, num_records, dCount, var_size);

    // read vids from delta
    SgmtLptrVec sgmt_ids;
    sgmt_ids.push_back(sgmt_id);
    std::vector<int> vids;
    vids.reserve(num_records);
    auto itr = this->begin(sgmt_ids);
    for ( ; itr; ++itr) {
        vids.push_back(itr.getVid());
    }

    // get ordered vids and values
    std::vector<int> ordered_vids;
    std::vector<eval> values;
    dict->getStaticDict(ordered_vids, values);

    // write vids and values for static segment
    new_sgmt->writeEmbeddedDict(values);
    new_sgmt->writeVidSequence(vids, ordered_vids);

    //swap sgmt and delete old one
    sgmt_map_[sgmt_id] = new_sgmt;
    free((void*)old_sgmt);
    delete dict;
    dict = nullptr;

    return true;
} 
/*
 * iterator
 */
template <class T>
typename PagedArrayEncoded<T>::iterator
PagedArrayEncoded<T>::begin(Transaction* trans) {
    SgmtLptrVec sgmt_ids = getAllSgmtIds();
    SegmentVec sgmts;
    sgmts.reserve(sgmt_ids.size());
    for (int s : sgmt_ids) {
        sgmts.push_back(getSegment(s));
    }

    return iterator(sgmts, trans);
}

template <class T>
typename PagedArrayEncoded<T>::iterator
PagedArrayEncoded<T>::begin(LogicalPtr lptr, Transaction* trans) {
    SegmentLptr sgmt_idx = Segment::calculateSgmtId(lptr);
    SegmentVec sgmts;
    sgmts.reserve(sgmt_map_.size()-sgmt_idx);
    for (;sgmt_idx < sgmt_map_.size(); ++sgmt_idx) {
        sgmts.push_back(getSegment(sgmt_idx));
    }
    iterator it(sgmts, trans);
    it.setLptr(lptr);
    return it;
}

template <class T>
typename PagedArrayEncoded<T>::iterator
PagedArrayEncoded<T>::begin(SgmtLptrVec& sgmt_ids, Transaction* trans) {
    SegmentVec sgmts;
    for (int s : sgmt_ids) {
        sgmts.emplace_back(getSegment(s));
    }

    return iterator(sgmts, trans);
}

template <class T>
void PagedArrayEncoded<T>::deserialize(SegmentLptr id,
        boost::archive::binary_iarchive& ia) {
    // TODO: impl
}

/*
 * allocate/release
 */
template <class T>
SegmentLptr PagedArrayEncoded<T>::allocateEncodedSegment() {
    uint32_t num_sgmt = sgmt_map_.size();
    uint64_t num_slots;
    LogicalPtr start_index = 0;

    if (num_sgmt == 0) {
        num_slots = BASE_SUB_SGMT_SIZE;
    } else if (num_sgmt < MAX_SUB_SGMT) {
        num_slots = (uint64_t)BASE_SUB_SGMT_SIZE << (num_sgmt - 1);
        start_index = num_slots;
    } else {
        num_slots = BASE_SGMT_SIZE;
        start_index = BASE_SGMT_SIZE * (num_sgmt + 1 - MAX_SUB_SGMT);
    }

    // encoding info
    uint32_t mask = SegmentEncoded<T>::DELTA_MASK;
    uint32_t bit_length = SegmentEncoded<T>::DELTA_BIT_LENGTH;
    DeltaDict<T>* dict = new DeltaDict<T>();

    // calcualte num_header_pages
    // uint32_t version_flag_size = num_slots / 8;
    uint32_t header_size = Segment::HEADER_SIZE;
    uint32_t num_header_pages = (header_size + PAGE_SIZE - 1) / PAGE_SIZE;

    // calculate num_data_pages
    uint32_t slots_per_page = (Page::getDataSize() * 8) / bit_length;
    uint32_t num_data_pages = (num_slots + slots_per_page - 1) / slots_per_page;
    if (num_slots <= slots_per_page) {
        num_data_pages = 1;
        slots_per_page = num_slots;
    }

    PhysicalPtr ptr = nullptr;
    if (posix_memalign(reinterpret_cast<void**>(&ptr),
            PAGE_SIZE, PAGE_SIZE * (num_header_pages + num_data_pages))) {
        assert(0 && "memory allocation failed");
    }

    SegmentEncoded<T>* new_sgmt = new (ptr) SegmentEncoded<T>(
            Segment::DYNAMIC, Page::ENCODED, id_,
            num_header_pages, num_data_pages, num_slots, slots_per_page,
            4 /* slot_width */, start_index, next_sgmt_id_,
            Segment::HEADER_SIZE,
            bit_length, mask, 0 /* var_offset */, 0 /* var_size */, 0 /* dict_size */,
            reinterpret_cast<PhysicalPtr>(dict)
            );

    sgmt_map_.insert(num_sgmt, new_sgmt);

    SegmentLptr header_lptr = sgmt_map_.getNfSgmtHeader();
    if (header_lptr == NULL_SGMT_LPTR) {
        sgmt_map_.getNfSgmtHeader() = next_sgmt_id_;
        sgmt_map_.getNfSgmtTail() = next_sgmt_id_;
    } else {
        Segment* header = getSegment(header_lptr);
        new_sgmt->setNextNfSegment(header_lptr);
        header->setPrevNfSegment(next_sgmt_id_);
        sgmt_map_.getNfSgmtHeader() = next_sgmt_id_;
    }

    return next_sgmt_id_++;
}


template <class T>
SegmentEncoded<T>* PagedArrayEncoded<T>::allocateStaticSegment(
        SegmentLptr sgmt_id, LogicalPtr start_index,
        uint32_t num_records, uint32_t dict_count, uint32_t var_size) {
    // calcalate dictionary size
    uint32_t dict_size = dict_count;
    if (dict_count == 1) dict_count++;
    uint32_t bit_length = (uint32_t)ceil(log2(dict_count));
    uint32_t var_offset = dict_count * sizeof(T);

    // calcaulate num_header_pages
    // uint32_t version_flag_size = (num_records + 7) / 8;
    uint32_t header_size = Segment::HEADER_SIZE + var_offset + var_size;
    uint32_t num_header_pages = (header_size + PAGE_SIZE - 1) / PAGE_SIZE;

    // calculate num_data_pages
    uint32_t slots_per_page = (Page::getDataSize() * 8) / bit_length;
    uint32_t num_data_pages = (num_records + slots_per_page - 1) / slots_per_page;
    if (num_records <= slots_per_page) {
        num_data_pages = 1;
        slots_per_page = num_records;
    }

    uint32_t mask = 1;
    for (unsigned i = 1; i < bit_length; i++) {
        mask <<= 1;
        mask += 1;
    }
    
    PhysicalPtr ptr = nullptr;
    if (posix_memalign(reinterpret_cast<void**>(&ptr),
            PAGE_SIZE, PAGE_SIZE * (num_header_pages + num_data_pages))) {
        assert(0 && "memory allocation failed");
    }

    SegmentEncoded<T>* new_sgmt = new (ptr) SegmentEncoded<T>(
            Segment::STATIC, Page::ENCODED, id_,
            num_header_pages, num_data_pages, num_records,
            slots_per_page, 4 /* slot_width */, start_index, sgmt_id,
            Segment::HEADER_SIZE, bit_length, mask,
            var_offset, var_size, dict_size, nullptr
            );

    return new_sgmt;
}
}  // namespace storage
