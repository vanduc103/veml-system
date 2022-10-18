// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

// Related include
#include "storage/util/page_allocator.h"

namespace storage {

PageAllocator::PageAllocator() : num_pages_(PAGE_PER_SEGMENT) {}

PageAllocator::~PageAllocator() {
    clear();
}

void* PageAllocator::allocate_() {
    WriteLock write_lock(mx_segments_);

    if (num_pages_ == PAGE_PER_SEGMENT) {
        char* new_sgmt = new char[CHUNK_SIZE];
        memset(new_sgmt, 0, CHUNK_SIZE);
        segments_.push_back(new_sgmt);
        num_pages_ = 0;
    }

    void* allocated = segments_.back() + (num_pages_ * PAGE_SIZE);
    num_pages_++;

    return allocated;
}

void PageAllocator::clear() {
    WriteLock write_lock(mx_segments_);

    for (unsigned i = 0; i < segments_.size(); i++) {
        if (segments_[i]) delete[] segments_[i];
    }
    segments_.clear();
    num_pages_ = PAGE_PER_SEGMENT;
}

}  // namespace storage

void* operator new (std::size_t bytes, storage::PageAllocator& allocator) {
    return storage::PageAllocator::allocate();
}

void* operator new[] (std::size_t bytes, storage::PageAllocator& allocator) {
    return storage::PageAllocator::allocate();
}
