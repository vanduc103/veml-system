// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

// Related include
#include "storage/base/page.h"

// C & C++ system include
#include <cstring>
#include <cassert>

namespace storage {

Page::Page(Type type, LogicalPtr start_laddr, uint32_t max_num_slots,
        uint16_t slot_width, PageLptr prev_page, PageLptr next_page, bool dirty)
    : start_laddr_(start_laddr), type_(type), max_num_slots_(max_num_slots),
    num_slots_(0), slot_width_(slot_width), full_flag_(0),
    prev_page_(prev_page), next_page_(next_page) {
    if (dirty) {
        num_slots_ = max_num_slots;
        memset(slot_, 0xFF, DATA_SIZE);
        memset(used_flag_, 0xFF, sizeof(uint64_t) * 60);
        memset(version_flag_, 0xFF, sizeof(uint64_t) * 60);
    } else {
        memset(slot_, 0, DATA_SIZE);
        memset(used_flag_, 0, sizeof(uint64_t) * 60);
        memset(version_flag_, 0, sizeof(uint64_t) * 60);
    }
}

/*
 * allocate slot
 */
AddressPair Page::allocateSlot(bool is_trans) {
    // try lock
    WriteLock write_lock(mx_page_lock_, try_to_lock);
    if (!write_lock.owns_lock()) {
        return AddressPair();
    }

    if (isFull()) {
        return AddressPair();
    }

    // find unused slot
    int i;
    int offset = -1;
    if (type_ == ENCODED) {
        offset = num_slots_;
        i = 0;
    } else {
        for (i = 0; i < 64 || i*64 < (int)max_num_slots_; ++i) {
            int idx = ffsl(~(used_flag_[i]));   // find first set bit from LSB
            if (idx != 0 && i*64 + offset < (int32_t)max_num_slots_) {
                offset = idx-1 + i*64;
                break;
            }
        }
        assert(offset < max_num_slots_);
    }

    return allocate(i, offset, is_trans);
}

AddressPair Page::allocateSlot(int offset, bool is_trans) {
    WriteLock write_lock(mx_page_lock_);
    if (isUsed(offset)) {
        std::cout << "offset error:" << offset << std::endl;
        std::string err("allocate invalid slot, @Page::allocateSlot, ");
        RTI_EXCEPTION(err);
    }

    assert(offset < max_num_slots_);
    int idx = offset / 64;

    return allocate(idx, offset, is_trans);
}

bool Page::releaseSlot(uint32_t offset, bool& was_full) {
    WriteLock write_lock(mx_page_lock_);
    return release(offset, was_full);
}

bool Page::releaseVarSlot(uint32_t offset, bool& was_full) {
    // calculate slot index from byte offset
    uint64_t slot_pos = reinterpret_cast<uint64_t>(slot_) - reinterpret_cast<uint64_t>(this);
    uint32_t slot_idx = (offset - slot_pos) / slot_width_;

    WriteLock write_lock(mx_page_lock_);
    return release(slot_idx, was_full);
}

void Page::updateSlot(uint32_t offset, PhysicalPtr ptr) {
    PhysicalPtr slot = const_cast<char*>(slot_) + offset * slot_width_;

    //WriteLock write_lock(mx_page_lock_);
    memcpy(slot, ptr, slot_width_);
}

bool Page::getFullFlag() {
    ReadLock read_lock(mx_full_flag_);
    return full_flag_ != 0;
}

void Page::setFullFlag(bool set) {
    WriteLock write_lock(mx_full_flag_);
    if (set) {
        full_flag_ = 1;
    } else {
        full_flag_ = 0;
    }
}

uint32_t Page::getLastSlot() const {
    if (num_slots_ == 0) return 0;

    uint32_t idx = 0;
    for (unsigned i = 0; i < 60; i++) {
        if (used_flag_[i] > 0) idx = i;
    }

    int offset = __builtin_clzl(used_flag_[idx]);
    return idx * 64 + (64 - offset);
}


void Page::setVersionFlag(uint32_t offset, bool flag) {
    int idx = offset / 64;
    if (flag)
        version_flag_[idx] = (version_flag_[idx]) | (1l << (offset % 64));
    else {
        uint32_t i = offset / 64;
        uint32_t woffset = offset % 64;
        uint64_t uf = version_flag_[i];
        version_flag_[i] = (uf & ~(1l << woffset));
    }
}

void Page::init() {
    max_num_slots_ = 0;
    slot_width_ = 0;
    num_slots_ = 0;
    memset(used_flag_, 0, sizeof(uint64_t) * 60);
    memset(version_flag_, 0, sizeof(uint64_t) * 60);
    memset(slot_, 0, DATA_SIZE);
}

AddressPair Page::allocate(int idx, int offset, bool is_trans) {
    // update used bit
    used_flag_[idx] = (used_flag_[idx]) | (1l << (offset % 64));
    num_slots_ += 1;

    // calculate laddr
    LogicalPtr lptr = LOGICAL_NULL_PTR;
    if (type_ == VAR) {
        uint16_t byte_offset = (slot_ - (char*)this) + (offset * slot_width_);
        lptr = start_laddr_ + byte_offset;
    } else {
        lptr = start_laddr_ + offset;
    }

    PhysicalPtr pptr = slot_ + (offset * slot_width_);
    
    if (is_trans)
        version_flag_[idx] = (version_flag_[idx]) | (1l << (offset % 64));

    return {lptr, pptr};
}

bool Page::release(uint32_t offset, bool& was_full) {
    was_full = getFullFlag(); 

    // search flag for slot
    assert(offset < max_num_slots_);
    uint32_t i = offset / 64;
    uint32_t woffset = offset % 64;
    uint64_t uf = used_flag_[i];

    // if already released, throw
    if (!(uf & (1l << woffset))) return false;
 
    used_flag_[i] = (uf & ~(1l << woffset));
    num_slots_ -= 1;

    return true;
}

}  // namespace storage
