// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr
#ifndef STORAGE_DEF_CONST_H_
#define STORAGE_DEF_CONST_H_

// Project include
#include "common/def_const.h"

// C & C++ system include
#include <cstdint>
#include <ostream>
#include <vector>

// Other include
#include <boost/serialization/access.hpp>

namespace storage {

/*
 * Segment
 */
const uint32_t MAX_SUB_SGMT = 11;
const uint32_t BASE_SGMT_SIZE = 1024 * 1024;  // 1M records
const uint32_t BASE_SUB_SGMT_SIZE = 1024;     // BASE_SGMT_SIZE >> (MAX_SUB_SGMT - 1)

const uint32_t BASE_VAR_SGMT_SIZE = 64 * 1024 * 1024;  // 64MB
const uint32_t BASE_VAR_SUB_SGMT_SIZE = 64 * 1024;     // BASE_VAR_SGMT_SIZE >> (MAX_SUB_SGMT - 1)

const uint8_t NUM_BITS_RECORD_OFFSET = 20;  // 1M records per segment
const uint8_t NUM_BITS_BYTE_OFFSET = 26;    // 64MB

const uint64_t MASK_SGMT_ID_GETTER = 0xFFFFFFFFFFF00000;  // 44bit set + 20bit unset
const uint64_t MASK_RECORD_OFFSET_GETTER = ~(MASK_SGMT_ID_GETTER);  // 44bit unset + 20bit set

const uint64_t MASK_VAR_SGMT_ID_GETTER = 0xFFFFFFFFFC000000;  // 38bit set + 26bit unset
const uint64_t MASK_BYTE_OFFSET_GETTER = ~(MASK_VAR_SGMT_ID_GETTER);  // 38bit unset + 26bit set

/**
 * Page
 */
typedef uint32_t PageLptr;
const PageLptr NULL_PAGE_LPTR = 0xFFFFFFFF;

const uint32_t PAGE_SIZE = 16 * 1024;   // 16KB page
const uint8_t NUM_VAR_PAGE_CATEGORY = 11;   // 16B ~ 16KB

const uint64_t PAGE_MASK = 0xFFFFFFFFFFFFC000;

typedef uint32_t SegmentLptr;
const SegmentLptr NULL_SGMT_LPTR = 0xFFFFFFFF;
typedef std::vector<SegmentLptr> SgmtLptrVec;

/**
 * BTree
 */
const uint32_t BTREE_NODE_SIZE = PAGE_SIZE;

/**
 * util
 */
std::string memory_unit_str(size_t size);

}  // namespace storage

#endif // STORAGE_DEF_CONST_H_
