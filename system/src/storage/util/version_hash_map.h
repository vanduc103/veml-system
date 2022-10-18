// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef STORAGE_UTIL_VERSION_HASH_MAP_
#define STORAGE_UTIL_VERSION_HASH_MAP_

// Project include
#include "concurrency/mutex_lock.h"
#include "concurrency/version.h"

// C & C++ system include
#include <unordered_map>

namespace storage {

/**
 * @brief version map for each segment
 * @author Ilju Lee, ijlee@kdb.snu.ac.kr
 */
class VersionHashMap {
  public:
    /**
     * @brief clear entier version map
     */
    void clear() {
        for (int i = 0; i < NUM_VERSION_HASH_SIZE; i++) {
            bucket_[i].node_map_.clear();;
        }
    }

    /**
     * @brief insert a new version to map
     * @param uint32_t offset: slot's offset in segment
     * @param Version* version to insert
     */
    void insertNewVersion(uint32_t offset, Version* version) {
        bucket_[offset % NUM_VERSION_HASH_SIZE].insertNewVersion(offset, version);
    }

    /**
     * @brief get version of a slot
     * @param uint32_t offset: slot's offset in segment
     * @return version
     */
    Version* getVersion(uint32_t offset) {
        return bucket_[offset % NUM_VERSION_HASH_SIZE].getVersion(offset);
    }
    
    /**
     * @brief remove a slot's versions
     * @param uint32_t offset: slot's offset in segment
     */
    Version* removeVersion(uint32_t offset) {
        return bucket_[offset % NUM_VERSION_HASH_SIZE].removeVersion(offset);
    }

  private:
    /**
     * @brief bucket in version-map
     * @details a slot is assigned to a specific bucket by modular function
     * @details thread safe impl for concurrent work
     */
    class Bucket {
      public:
        typedef std::unordered_map<uint32_t, Version*> NodeMap;

        /**
         * @brief create an empty bucket
         */
        Bucket() {}
        
        /**
         * @brief insert a new version to bucket
         * @param uint32_t offset: slot's offset in segment
         * @param Version* version to insert
         */
        void insertNewVersion(uint32_t offset, Version* version) {
            WriteLock wl(mx_node_);
            auto itr = node_map_.find(offset);
            if (itr != node_map_.end()) {
                version->setPrev(itr->second);
                itr->second = version;
            } else {
                node_map_.insert(std::make_pair(offset, version));
            }
        }

        /**
         * @brief get version of a slot
         * @param uint32_t offset: slot's offset in segment
         * @return version
         */
        Version* getVersion(uint32_t offset) {
            ReadLock rl(mx_node_);
            auto itr = node_map_.find(offset);
            if (itr != node_map_.end()) {
                return itr->second;
            }

            return nullptr;
        }

        /**
         * @brief remove a slot's versions
         * @param uint32_t offset: slot's offset in segment
         */
        Version* removeVersion(uint32_t offset) {
            WriteLock wl(mx_node_);
            auto itr = node_map_.find(offset);
            if (itr != node_map_.end()) {
                Version* version = itr->second;
                node_map_.erase(itr);
                return version;
            }

            return nullptr;
        }

        Mutex   mx_node_;
        NodeMap node_map_;
    };

    Bucket bucket_[NUM_VERSION_HASH_SIZE];
};

} // namespace storage

#endif // STORAGE_UTIL_VERSION_HASH_MAP_
