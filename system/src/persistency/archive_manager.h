// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef PERSISTENCY_ARCHIVE_MANAGER_H_
#define PERSISTENCY_ARCHIVE_MANAGER_H_

// C & C++ system include
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <string>

namespace storage {
    class paged_array_base;
    class Table;
    class Graph;
}

namespace persistency {

using namespace storage;

class ArchiveManager {
  public:
    /**
     * archive system
     */
    static bool archive(const std::string path);
    static bool archive(const paged_array_base* pa);
    static bool archive(const Table* table);
    static bool archive(const Graph* graph);

    /**
     * recover system
     */
    static bool recover(const std::string path, int dop = 4);

    /**
     * util
     */
    static std::string path() { return ArchiveManager::path_; }

  private:
    static std::string path_;
};

}  // namespace persistency

#endif  // PERSISTENCY_ARCHIVE_MANAGER_H_
