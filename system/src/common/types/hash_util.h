// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef COMMON_TYPES_HASH_UTIL_H_
#define COMMON_TYPES_HASH_UTIL_H_

// Code from impala (13.03.26)
// https://github.com/cloudera/Impala/blob/master/be/src/util/hash-util.h
// Use fnvhash. not crchash result are correlated

#ifdef __SSE4_2__
#include <nmmintrin.h>
#endif

#include <cstdint>
#include <vector>

class HashUtil {
 public:
  static const uint32_t INIT_SEED = 1;
#ifdef __SSE4_2__
  // Compute the Crc32 hash for data using SSE4 instructions. The input hash parameter is
  // the current hash/seed value.
  // This should only be called if SSE is supported.
  // This is ~4x faster than Fnv/Boost Hash.
  // NOTE: Any changes made to this function need to be reflected in Codegen::GetHashFn.
  // TODO: crc32 hashes with different seeds do not result in different hash functions.
  // The resulting hashes are correlated.
  static uint32_t crcHash(const void* data, int32_t bytes, uint32_t hash) {
    // DCHECK(CpuInfo::IsSupported(CpuInfo::SSE4_2));
    uint32_t words = bytes / sizeof(uint32_t);
    bytes = bytes % sizeof(uint32_t);

    const uint32_t* p = reinterpret_cast<const uint32_t*>(data);
    while (words--) {
      hash = _mm_crc32_u32(hash, *p);
      ++p;
    }

    const uint8_t* s = reinterpret_cast<const uint8_t*>(p);
    while (bytes--) {
      hash = _mm_crc32_u8(hash, *s);
      ++s;
    }

    // The lower half of the CRC hash has has poor uniformity, so swap the halves
    // for anyone who only uses the first several bits of the hash.
    hash = (hash << 16) | (hash >> 16);
    return hash;
  }
#endif

  // default values recommended by http://isthe.com/chongo/tech/comp/fnv/
  static const uint32_t FNV_PRIME = 0x01000193; // 16777619
  static const uint32_t FNV_SEED = 0x811C9DC5; // 2166136261
  static const uint64_t FNV64_PRIME = 1099511628211UL;
  static const uint64_t FNV64_SEED = 14695981039346656037UL;

  // Implementation of the Fowler–Noll–Vo hash function. This is not as performant
  // as boost's hash on int types (2x slower) but has bit entropy.
  // For ints, boost just returns the value of the int which can be pathological.
  // For example, if the data is <1000, 2000, 3000, 4000, ..> and then the mod of 1000
  // is taken on the hash, all values will collide to the same bucket.
  // For string values, Fnv is slightly faster than boost.
  static uint32_t fnvHash(const void* data, int32_t bytes, uint32_t hash) {
    const uint8_t* ptr = reinterpret_cast<const uint8_t*>(data);
    while (bytes--) {
      hash = (*ptr ^ hash) * FNV_PRIME;
      ++ptr;
    }
    return hash;
  }

  static uint64_t fnvHash64(const void* data, int32_t bytes, uint64_t hash) {
    const uint8_t* ptr = reinterpret_cast<const uint8_t*>(data);
    while (bytes--) {
      hash = (*ptr ^ hash) * FNV64_PRIME;
      ++ptr;
    }
    return hash;
  }

  // Computes the hash value for data. Will call either CrcHash or FnvHash
  // depending on hardware capabilities.
  // Seed values for different steps of the query execution should use different seeds
  // to prevent accidental key collisions. (See IMPALA-219 for more details).
  static uint32_t hash(const void* data, int32_t bytes, uint32_t seed) {
#ifdef __SSE4_2__
    // if (LIKELY(CpuInfo::IsSupported(CpuInfo::SSE4_2))) {
      return crcHash(data, bytes, seed);
    // } else {
    //   return fnvHash(data, bytes, seed);
    // }
#else
    return fnvHash(data, bytes, seed);
#endif
  }

    static inline int pow2roundup (int x)
    {
        if (x < 0)
            return 0;
        --x;
        x |= x >> 1;
        x |= x >> 2;
        x |= x >> 4;
        x |= x >> 8;
        x |= x >> 16;
        return x+1;
    }
};

#endif  // COMMON_TYPES_HASH_UTIL_H_
