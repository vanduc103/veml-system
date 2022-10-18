// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

// Related include
#include "storage/def_const.h"

namespace storage {

std::string memory_unit_str(size_t size) {
    if (size > 10737418240ull) {
        size /= 10737418240ull;
        return std::to_string(size) + "GB";
    } else if (size > 1048576ull) {
        size /= 1048576ull;
        return std::to_string(size) + "MB";
    } else if (size > 10240) {
        size /= 1024;
        return std::to_string(size) + "KB";
    } else {
        return std::to_string(size) + "B";
    }
}

}  // namespace storage
