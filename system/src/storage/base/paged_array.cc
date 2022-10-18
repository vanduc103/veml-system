// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

// Related include
#include "storage/base/paged_array.h"
#include "storage/base/paged_array_var.h"

// Project include
#include "storage/catalog.h"
#include "persistency/archive_manager.h"
#include "concurrency/rti_thread.h"

// C & C++ system include
#include <fstream>

// Other include
#include <boost/serialization/binary_object.hpp>

// gRPC generated file
#include "interface/interface.grpc.pb.h"

namespace storage {

/*
 * paged_array_base::iterator
 */
PABase::iterator::iterator(const SegmentVec& sgmts, Transaction* trans)
        : sgmts_(sgmts), sgmt_itr_(nullptr),
        sgmt_idx_(0), is_ended_(true), trans_(trans) {
    while (sgmt_idx_ < sgmts_.size()) {
        sgmt_itr_ = sgmts_[sgmt_idx_]->begin(trans_);
        is_ended_ = !(*sgmt_itr_);
        if (!is_ended_) break;
        delete sgmt_itr_;
        sgmt_itr_ = nullptr;
        sgmt_idx_++;
    }
}

PABase::iterator::~iterator() {
    if (sgmt_itr_) delete sgmt_itr_;
    sgmt_itr_ = nullptr;
}

void PABase::iterator::init() {
    sgmt_itr_->init();
    sgmt_idx_ = 0;
    is_ended_ = false;
}

void PABase::iterator::setLptr(LogicalPtr lptr) {
    if (sgmts_.size() == 0 && lptr == 0) {
        if (sgmt_itr_ != nullptr) delete sgmt_itr_;
        is_ended_ = true;
        return;
    }
    SegmentLptr sgmt_id = Segment::calculateSgmtId(lptr);

    Segment* sgmt = nullptr;
    sgmt_idx_ = 0;
    for ( ; sgmt_idx_ < sgmts_.size(); sgmt_idx_++) {
        if (sgmts_[sgmt_idx_]->getSegmentLptr() == sgmt_id) {
            sgmt = sgmts_[sgmt_idx_];
            break;
        }
    }

    if (sgmt_idx_ == sgmts_.size()) {
        std::string err("invalid index @paged_array_base::iterator::setLptr, ");
        RTI_EXCEPTION(err);
    }

    sgmt_itr_ = sgmt->begin(trans_);
    sgmt_itr_->setLptr(lptr);
}

/*
 * paged_array_base, constructor/destructor
 */
PABase::paged_array_base(bool is_metadata, uint16_t slot_width)
        : next_sgmt_id_(0), slot_width_(slot_width), var_id_(-1) {
    if (!is_metadata) {
        id_ = Metadata::addNewPagedArray(this);
    }
}

PABase::paged_array_base(uint32_t id, bool is_metadata, uint16_t slot_width)
        : next_sgmt_id_(0), id_(id), slot_width_(slot_width), var_id_(-1) {
    if (!is_metadata) {
        Metadata::addNewPagedArray(this, id_);
    }
}

PABase::~paged_array_base() {
    truncate();
}

void PABase::truncate() {
    for (unsigned i = 0 ; i < sgmt_map_.size(); i++) {
        if (sgmt_map_[i] != nullptr) {
            sgmt_map_[i]->~Segment();
            free((void*)sgmt_map_[i]);
            sgmt_map_[i] = nullptr;
        }
    }
    sgmt_map_.clear();
    next_sgmt_id_ = 0;
}

/*
 * paged_array_base, public getter
 */
size_t PABase::getNumElement() {
    class Worker : public RTIThread {
      public:
        Worker(std::vector<Segment*> sgmt)
            : sgmt_(sgmt), cnt_(0) {}

        size_t cnt() { return cnt_; }

        void* run() {
            for (auto sgmt : sgmt_) {
                cnt_ += sgmt->getNumElement();
            }
            return nullptr;
        }

      private:
        std::vector<Segment*> sgmt_;
        size_t cnt_;
    };

    if (sgmt_map_.size() == 0) return 0;

    unsigned dop = sgmt_map_.size() > 32? 32 : sgmt_map_.size();
    unsigned division = sgmt_map_.size() / dop;
    std::vector<Worker> workers;
    for (unsigned i = 0; i < dop; i++) {
        unsigned start = i * division;
        unsigned end = i == (dop-1)? sgmt_map_.size() : (i+1) * division;

        std::vector<Segment*> sgmts;
        for ( ; start < end; start++) {
            sgmts.push_back(sgmt_map_[start]);
        }
        workers.push_back(Worker(sgmts));
    }

    for (auto& w : workers) {
        w.start();
    }

    for (auto& w : workers) {
        w.join();
    }

    size_t num = 0;
    for (auto& w : workers) {
        num += w.cnt();
    }

    return num;
}

LogicalPtr PABase::getStartLaddr(SegmentLptr sgmt_id, PageLptr page_id) {
    Segment* sgmt = getSegment(sgmt_id);
    return sgmt->getStartIndex(page_id);
}

Segment::Type PABase::getSegmentType(SegmentLptr sgmt_id) {
    return sgmt_map_[sgmt_id]->getSegmentType();
}

SgmtLptrVec PABase::getAllSgmtIds() const {
    SgmtLptrVec sgmt_ids;
    sgmt_ids.reserve(sgmt_map_.size());
    for (SegmentLptr id = 0; id < sgmt_map_.size(); id++) {
        if (sgmt_map_[id] != nullptr)
            sgmt_ids.push_back(id);
    }

    return sgmt_ids;
}

/*
 * paged_array_base, iterator
 */
PABase::iterator PABase::begin(Transaction* trans) const {
    SgmtLptrVec sgmt_ids = getAllSgmtIds();
    return begin(sgmt_ids, trans);
}

PABase::iterator PABase::begin(LogicalPtr lptr, Transaction* trans) const {
    SegmentLptr sgmt_id = Segment::calculateSgmtId(lptr);
    SgmtLptrVec sgmt_ids;
    sgmt_ids.reserve(sgmt_map_.size()-sgmt_id);
    for ( ; sgmt_id < sgmt_map_.size(); sgmt_id++) {
        sgmt_ids.push_back(sgmt_id);
    }

    PABase::iterator itr = begin(sgmt_ids, trans);
    itr.setLptr(lptr);
    return itr;
}

PABase::iterator PABase::begin(
        SgmtLptrVec& sgmt_ids, Transaction* trans) const {
    SegmentVec sgmts;
    sgmts.reserve(sgmt_ids.size());
    for (auto id : sgmt_ids) {
        sgmts.push_back(getSegment(id));
    }

    return iterator(sgmts, trans);
}

/*
 * paged_array_base, multi-version concurrency control
 */
void PABase::consolidateVersion(LogicalPtr idx) {
    Segment* sgmt = nullptr;
    SegmentLptr sgmt_id = Segment::calculateSgmtId(idx);
    sgmt = getSegment(sgmt_id);

    uint32_t offset = idx - sgmt->getStartIndex();
    sgmt->consolidateVersion(offset);
}

void PABase::serialize(boost::archive::binary_oarchive& oa) const {
    sgmt_map_.serialize(oa);
    oa << next_sgmt_id_;
    oa << slot_width_;
}

void PABase::serialize(SegmentLptr id,
                boost::archive::binary_oarchive& oa) const {
    Segment* sgmt = getSegment(id);
    if (sgmt) {
        sgmt->serialize(oa);
    }
}

void PABase::deserialize(boost::archive::binary_iarchive& ia) {
    this->sgmt_map_.deserialize(ia);
    ia >> next_sgmt_id_;
    ia >> slot_width_;
}

bool PABase::recover(std::string path) {
    // get header archive
    std::ifstream header(path + "/header.archive");
    if (!header.good()) {
        std::string err("failed to open archive file "
                            + path + "/header.archive");
        RTI_EXCEPTION(err); 
        return false;
    }

    // deserialize segment
    boost::archive::binary_iarchive ia(header);
    this->deserialize(ia);

    for (unsigned i = 0; i < next_sgmt_id_; i++) {
        std::ifstream sgmt(path + "/" + std::to_string(i) + ".segment");
        if (!sgmt.good()) continue;

        boost::archive::binary_iarchive ia(sgmt);
        this->deserialize(i, ia);
    }

    return true;
}

bool PABase::recover(std::string& header, StrVec& sgmts) {
    std::stringstream ss;
    ss << header;
    boost::archive::binary_iarchive ia(ss);
    this->deserialize(ia);

    for (unsigned i = 0; i < next_sgmt_id_; i++) {
        std::stringstream ss;
        ss << sgmts[i];
        boost::archive::binary_iarchive ia(ss);
        this->deserialize(i, ia);
    }

    return true;
}

/*
 * paged_array_base, util/dbg
 */
void PABase::reserve(size_t size, bool dirty) {
    size_t currentCap = 0;
    SegmentLptr sgmt_lptr(0);
    Segment* sgmt = nullptr;

    for (SegmentLptr id = 0; id < sgmt_map_.size(); id++) {
        currentCap += sgmt_map_[id]->getNumSlots();
    }

    while (currentCap < size) {
        sgmt_lptr = allocateSegment();
        sgmt = getSegment(sgmt_lptr);
        currentCap += sgmt->getNumSlots();
    }
}

size_t PABase::getMemoryUsage() {
    size_t size = 0;
    for (unsigned i = 0; i < sgmt_map_.size(); i++) {
        Segment* sgmt = getSegment(i);
        if (sgmt) size += sgmt->getMemoryUsage();
    }

    return size + sizeof(PABase) + sgmt_map_.getMemoryUsage();
}

void PABase::finalizeCurrentSgmt() {
    // set all segment as full
    SegmentLptr sgmt_id = sgmt_map_.getNfSgmtHeader();
    while (sgmt_id != NULL_SGMT_LPTR) {
        Segment* sgmt = getSegment(sgmt_id);
        sgmt_id = sgmt->getNextNfSegment();

        sgmt->finalize();
        sgmt->setNextNfSegment(NULL_SGMT_LPTR);
    }

    sgmt_map_.getNfSgmtHeader() = NULL_SGMT_LPTR;
    sgmt_map_.getNfSgmtTail() = NULL_SGMT_LPTR;
}

/*
 * protected interfaces
 */
AddressPair PABase::insertSlot(PhysicalPtr val) {
    // allocate slot
    uint32_t list_id = TransactionManager::getGlobalTid() % NUM_PAGE_LIST;

    AddressPair slot = allocateSlot(list_id, false);
    memcpy(slot.pptr_, val, slot_width_);

    return slot;
}

AddressPair PABase::insertSlot(PhysicalPtr val, LogicalPtr idx) {
    AddressPair slot = assignSlot(idx, false);
    memcpy(slot.pptr_, val, slot_width_);

    return slot;
}

AddressPair PABase::insertSlot(Transaction& trans, PhysicalPtr val) {
    // allocate slot
    uint32_t list_id = trans.getTid() % NUM_PAGE_LIST;
    AddressPair slot = allocateSlot(list_id, true);
    memcpy(slot.pptr_, val, slot_width_);

    // create version
    Version* version = trans.createVersion(Version::INSERT, trans.getTid(),
            trans.getStartTs(), id_, slot.lptr_, nullptr, 0);

    SegmentLptr sgmt_id = Segment::calculateSgmtId(slot.lptr_);
    Segment* sgmt = getSegment(sgmt_id);
    uint32_t offset = slot.lptr_ - sgmt->getStartIndex();
    sgmt->insertNewVersion(offset, version);

    return slot;
}

AddressPair PABase::insertSlot(
        Transaction& trans, PhysicalPtr val, LogicalPtr idx) {
    // allocate slot
    AddressPair slot = assignSlot(idx, true);
    memcpy(slot.pptr_, val, slot_width_);

    // create version
    Version* version = trans.createVersion(Version::INSERT, trans.getTid(),
            trans.getStartTs(), id_, slot.lptr_, val, slot_width_);

    SegmentLptr sgmt_id = Segment::calculateSgmtId(slot.lptr_);
    Segment* sgmt = getSegment(sgmt_id);
    uint32_t offset = slot.lptr_ - sgmt->getStartIndex();
    sgmt->insertNewVersion(offset, version);

    return slot;
}

AddressPair PABase::allocateSlot(uint32_t list_id, bool is_trans) {
    SegmentLptr sgmt_lptr = NULL_SGMT_LPTR;
    Segment* sgmt = nullptr;
    AddressPair slot;
    bool allocated = false;

    while (!allocated) {
        while (sgmt_lptr == NULL_SGMT_LPTR) {
            sgmt_lptr = sgmt_map_.getNfSgmtHeader();
            if (sgmt_lptr == NULL_SGMT_LPTR) {
                sgmt_lptr = allocateSegment();
            }
        }

        bool is_full = true;
        sgmt = getSegment(sgmt_lptr);
        while (sgmt && !allocated) {
            if (!sgmt->allocateSlot(slot, list_id, is_trans)) {
                is_full &= sgmt->isFull(list_id);
                sgmt_lptr = sgmt->getNextNfSegment();
                sgmt = getSegment(sgmt_lptr);
            } else {
                allocated = true;
            }
        }

        if (!allocated && is_full) {
            sgmt_lptr = allocateSegment();
        }
    }

    checkAllocatedSgmt(sgmt, sgmt_lptr);
    return slot;
}

AddressPair PABase::assignSlot(LogicalPtr idx, bool is_trans) {
    SegmentLptr sgmt_id = Segment::calculateSgmtId(idx);
    Segment* sgmt = getSegment(sgmt_id);
    AddressPair slot;
    bool allocated = false;

    while (!allocated) {
        while (sgmt == nullptr) {
            SegmentLptr sgmt_lptr = allocateSegment(sgmt_id);
            sgmt = getSegment(sgmt_lptr);
        }

        uint32_t offset = idx - sgmt->getStartIndex();
        if (!sgmt->assignSlot(slot, offset, is_trans)) {
            return AddressPair();
        } else {
            allocated = true;
        }
    }

    checkAllocatedSgmt(sgmt, sgmt_id);
    return slot;
}

SegmentLptr PABase::allocateSegment() {
    WriteLock write_lock(mx_sgmt_list_, try_to_lock);
    if (!write_lock.owns_lock())
        return NULL_SGMT_LPTR;

    SegmentLptr sgmt_id = sgmt_map_.size();
    return createSegment(sgmt_id);
}

SegmentLptr PABase::allocateSegment(SegmentLptr sgmt_id) {
    WriteLock write_lock(mx_sgmt_list_, try_to_lock);
    if (!write_lock.owns_lock())
        return NULL_SGMT_LPTR;

    Segment* sgmt = getSegment(sgmt_id);
    if (sgmt != nullptr) return sgmt_id;

    return createSegment(sgmt_id);
}

SegmentLptr PABase::createSegment(SegmentLptr sgmt_id) {
    LogicalPtr start_index, num_slots;
    Segment::getSgmtStat(sgmt_id, start_index, num_slots);

    // calculate num_header_pages
    uint32_t header_size = Segment::HEADER_SIZE;
    uint32_t num_header_pages = (header_size + PAGE_SIZE - 1) / PAGE_SIZE;

    // calculate num_data_pages
    uint64_t slots_per_page = Page::getMaxNumSlots(slot_width_);
    uint64_t num_data_pages = (num_slots + slots_per_page - 1) / slots_per_page;
    if (num_slots <= slots_per_page) {
        num_data_pages = 1;
        slots_per_page = num_slots;
    }

    // Allocate space for segment from heap or shared memory
    PhysicalPtr ptr = nullptr;
    if (posix_memalign(reinterpret_cast<void**>(&ptr),
                PAGE_SIZE, PAGE_SIZE * (num_header_pages + num_data_pages))) {
        assert(0 && "memory allocation failed");
    }

    Segment* sgmt = new (ptr) Segment(Segment::NORMAL, Page::Type::FIXED, id_,
            num_header_pages, num_data_pages, num_slots, slots_per_page,
            slot_width_, start_index, sgmt_id,
            Segment::HEADER_SIZE);

    sgmt_map_.insert(sgmt_id, sgmt);

    SegmentLptr header_lptr = sgmt_map_.getNfSgmtHeader();
    if (header_lptr == NULL_SGMT_LPTR) {
        sgmt_map_.getNfSgmtHeader() = sgmt_id;
        sgmt_map_.getNfSgmtTail() = sgmt_id;
    } else {
        Segment* header = getSegment(header_lptr);
        sgmt->setNextNfSegment(header_lptr);
        header->setPrevNfSegment(sgmt_id);
        sgmt_map_.getNfSgmtHeader() = sgmt_id;
    }

    if (sgmt_id >= next_sgmt_id_) next_sgmt_id_ = sgmt_id + 1;

    return SegmentLptr(sgmt_id);
}

void PABase::checkAllocatedSgmt(Segment* sgmt, SegmentLptr sgmt_lptr) {
    if (sgmt->isFull()) {
        // if segment becomes full, remove from nf_segment list
        WriteLock write_lock(mx_sgmt_list_);
        if (!sgmt->getFullFlag()) {
            Segment* prev = getSegment(sgmt->getPrevNfSegment());
            Segment* next = getSegment(sgmt->getNextNfSegment());

            if (sgmt_map_.getNfSgmtHeader() == sgmt_lptr)
                sgmt_map_.getNfSgmtHeader() = sgmt->getNextNfSegment();

            if (sgmt_map_.getNfSgmtTail() == sgmt_lptr)
                sgmt_map_.getNfSgmtTail() = sgmt->getPrevNfSegment();

            if (prev)
                prev->setNextNfSegment(sgmt->getNextNfSegment());

            if (next)
                next->setPrevNfSegment(sgmt->getPrevNfSegment());

            sgmt->setPrevNfSegment(NULL_SGMT_LPTR);
            sgmt->setNextNfSegment(NULL_SGMT_LPTR);
            sgmt->setFullFlag();
        }
    }
}

void PABase::checkReleaseSlot(Segment* sgmt, SegmentLptr sgmt_id) {
    WriteLock write_lock(mx_sgmt_list_);
    if (!sgmt->isFull() && sgmt->getFullFlag()) {
        if (sgmt_map_.getNfSgmtHeader() == NULL_SGMT_LPTR) {
            sgmt_map_.getNfSgmtHeader() = sgmt_id;
        } else {
            SegmentLptr tail_id = sgmt_map_.getNfSgmtTail();
            Segment* tail = getSegment(tail_id);
            sgmt->setPrevNfSegment(tail_id);
            tail->setNextNfSegment(sgmt_id);
        }
        sgmt_map_.getNfSgmtTail() = sgmt_id;
        sgmt->setFullFlag(false);
    }
}

/*
 * template specialiaztion for
 * PagedArray<T>
 */
template <>
PagedArray<Varchar_T>::PagedArray(bool is_metadata, uint16_t slot_width)
        : paged_array_base(sizeof(Varchar_T), is_metadata) {
    var_slots_ = new PagedArrayVar();
    var_id_ = var_slots_->getId();
}

template <>
PagedArray<Varchar_T>::PagedArray(
        uint32_t id, bool is_metadata, uint16_t slot_width)
        : paged_array_base(id, sizeof(Varchar_T), is_metadata) {
    var_slots_ = new PagedArrayVar();
    var_id_ = var_slots_->getId();
}

template <>
PagedArray<Varchar_T>::~PagedArray() {
    if(var_slots_)
        delete var_slots_;
}

template <>
AddressPair PagedArray<Varchar_T>::insert(String val) {
    Varchar_T* varchar = nullptr;
    if (val.isEmbedded()) {
        varchar = new Varchar_T(val, val.size());
    } else {
        PagedArrayVar* var_pa = reinterpret_cast<PagedArrayVar*>(
                Metadata::getPagedArray(var_id_));
        AddressPair vslot = var_pa->allocateVarSlot(val.size());
        varchar = new Varchar_T(val, var_id_, vslot);
    }

    AddressPair ret = insertSlot(reinterpret_cast<PhysicalPtr>(varchar));
    delete varchar;

    return ret;
}

template <>
AddressPair PagedArray<Varchar_T>::insert(String val, LogicalPtr idx) {
    Varchar_T* varchar = nullptr;
    if (val.isEmbedded()) {
        varchar = new Varchar_T(val, val.size());
    } else {
        PagedArrayVar* var_pa = reinterpret_cast<PagedArrayVar*>(
                Metadata::getPagedArray(var_id_));
        AddressPair vslot = var_pa->allocateVarSlot(val.size());
        varchar = new Varchar_T(val, var_id_, vslot);
    }

    AddressPair ret = insertSlot(reinterpret_cast<PhysicalPtr>(varchar), idx);
    delete varchar;

    return ret;
}

template <>
AddressPair PagedArray<Tuple_T>::insert(Tuple val) {
    return insertSlot(val.get());
}

template <>
AddressPair PagedArray<Tuple_T>::insert(Tuple val, LogicalPtr idx) {
    return insertSlot(val.get(), idx);
}

template <>
AddressPair PagedArray<Tuple_T>::insert(Transaction& trans, Tuple val) {
    return insertSlot(trans, val.get());
}

template <>
AddressPair PagedArray<Tuple_T>::insert(
        Transaction& trans, Tuple val, LogicalPtr idx) {
    return insertSlot(trans, val.get(), idx);
}

}  // namespace storage
