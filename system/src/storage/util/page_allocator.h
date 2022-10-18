// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef STORAGE_UTIL_PAGE_ALLOCATOR_H_
#define STORAGE_UTIL_PAGE_ALLOCATOR_H_

// Project include
#include "common/def_const.h"
#include "storage/def_const.h"
#include "concurrency/mutex_lock.h"

namespace storage {

/**
 * @brief page allocator for util module
 * @details to reduce the os cost of memory allocation,
 * @details allocate a large chunk(16MB) of memory and divide it into pages(16KB)
 * @details hand out modules that need additional memory
 * @author Ilju Lee, ijlee@kdb.snu.ac.kr
 */
class PageAllocator {
  public:
    PageAllocator();
    ~PageAllocator();

    /**
     * @brief allocate a page
     * @return 16KB page
     */
    static void* allocate()
        { return PageAllocator::get().allocate_(); }

    /**
     * @brief get PageAllocator sigleton
     * @return page_allocator ref
     */
    static PageAllocator& get() {
        static PageAllocator page_allocator_;
        return page_allocator_;
    }

    static const std::size_t CHUNK_SIZE = 16 * 1024 * 1024;
    static const std::size_t PAGE_PER_SEGMENT = 1024;

  private:
    /**
     * @brief allocate a page
     * @return 16KB page
     */
    void* allocate_();

    /**
     * @brief clear allocator
     */
    void clear();

    Mutex mx_segments_;
    std::vector<char*> segments_;
    uint32_t num_pages_;
};

/**
 * @brief item that can make a list of page
 * @details assign a page from page allocator
 * @author Ilju Lee, ijlee@kdb.snu.ac.kr
 */
class ListItem {
  public:
    /**
     * @brief create an item
     */
    ListItem() : next_(0) {}

    /**
     * @brief ListItem destructor
     */
    virtual ~ListItem() {}

    /**
     * @brief get data region of memory (except header)
     * @return physical pointer
     */
    virtual char* get() {
        return data_;
    }

    /**
     * @brief get next item
     * @return next item
     */
    ListItem* getNext() {
        return next_;
    }

    /**
     * @brief set next item
     */
    void setNext(ListItem* next) {
        next_ = next;
    }
  
    static const size_t ITEM_SIZE = PAGE_SIZE - sizeof(ListItem*);

  protected:
    ListItem* next_;
    char data_[ITEM_SIZE];
};

}  // namespace storage

void* operator new (std::size_t bytes, storage::PageAllocator& allocator);
void* operator new[] (std::size_t bytes, storage::PageAllocator& allocator);

#endif  // STORAGE_UTIL_PAGE_ALLOCATOR_H_
