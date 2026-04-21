#include "hash.h"

/*
 * xxHash64 Implementation
 * Based on xxHash by Yann Collet
 * https://github.com/Cyan4973/xxHash
 */

#define XXH64_PRIME1 0x9E3779B185EBCA87ULL
#define XXH64_PRIME2 0xC2B2AE3D27D4EB4FULL
#define XXH64_PRIME3 0x165667B19E3779F9ULL
#define XXH64_PRIME4 0x85EBCA77C2B2AE63ULL
#define XXH64_PRIME5 0x27D4EB2F165667C5ULL

static inline uint64_t xxh64_rotl(uint64_t x, int r) {
  return (x << r) | (x >> (64 - r));
}

static inline uint64_t xxh64_round(uint64_t acc, uint64_t input) {
  acc += input * XXH64_PRIME2;
  acc = xxh64_rotl(acc, 31);
  acc *= XXH64_PRIME1;
  return acc;
}

static inline uint64_t xxh64_merge_round(uint64_t acc, uint64_t val) {
  val = xxh64_round(0, val);
  acc ^= val;
  acc = acc * XXH64_PRIME1 + XXH64_PRIME4;
  return acc;
}

static inline uint64_t xxh64_avalanche(uint64_t h) {
  h ^= h >> 33;
  h *= XXH64_PRIME2;
  h ^= h >> 29;
  h *= XXH64_PRIME3;
  h ^= h >> 32;
  return h;
}

static inline uint64_t xxh64_read64(const uint8_t *p) {
  return ((uint64_t)p[0]) | ((uint64_t)p[1] << 8) | ((uint64_t)p[2] << 16) |
         ((uint64_t)p[3] << 24) | ((uint64_t)p[4] << 32) |
         ((uint64_t)p[5] << 40) | ((uint64_t)p[6] << 48) |
         ((uint64_t)p[7] << 56);
}

static inline uint32_t xxh64_read32(const uint8_t *p) {
  return ((uint32_t)p[0]) | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) |
         ((uint32_t)p[3] << 24);
}

uint64_t xxhash64(const void *data, size_t len, uint64_t seed) {
  const uint8_t *p = (const uint8_t *)data;
  const uint8_t *end = p + len;
  uint64_t h64;

  if (len >= 32) {
    const uint8_t *limit = end - 32;
    uint64_t v1 = seed + XXH64_PRIME1 + XXH64_PRIME2;
    uint64_t v2 = seed + XXH64_PRIME2;
    uint64_t v3 = seed + 0;
    uint64_t v4 = seed - XXH64_PRIME1;

    do {
      v1 = xxh64_round(v1, xxh64_read64(p));
      p += 8;
      v2 = xxh64_round(v2, xxh64_read64(p));
      p += 8;
      v3 = xxh64_round(v3, xxh64_read64(p));
      p += 8;
      v4 = xxh64_round(v4, xxh64_read64(p));
      p += 8;
    } while (p <= limit);

    h64 = xxh64_rotl(v1, 1) + xxh64_rotl(v2, 7) + xxh64_rotl(v3, 12) +
          xxh64_rotl(v4, 18);
    h64 = xxh64_merge_round(h64, v1);
    h64 = xxh64_merge_round(h64, v2);
    h64 = xxh64_merge_round(h64, v3);
    h64 = xxh64_merge_round(h64, v4);
  } else {
    h64 = seed + XXH64_PRIME5;
  }

  h64 += (uint64_t)len;

  /* Process remaining bytes */
  while (p + 8 <= end) {
    uint64_t k1 = xxh64_round(0, xxh64_read64(p));
    h64 ^= k1;
    h64 = xxh64_rotl(h64, 27) * XXH64_PRIME1 + XXH64_PRIME4;
    p += 8;
  }

  if (p + 4 <= end) {
    h64 ^= (uint64_t)(xxh64_read32(p)) * XXH64_PRIME1;
    h64 = xxh64_rotl(h64, 23) * XXH64_PRIME2 + XXH64_PRIME3;
    p += 4;
  }

  while (p < end) {
    h64 ^= (*p) * XXH64_PRIME5;
    h64 = xxh64_rotl(h64, 11) * XXH64_PRIME1;
    p++;
  }

  return xxh64_avalanche(h64);
}

/*
 * MurmurHash3 Implementation (x64 128-bit variant, returning lower 64 bits)
 * Based on MurmurHash3 by Austin Appleby
 * https://github.com/aappleby/smhasher
 */

#define MURMUR3_C1 0x87c37b91114253d5ULL
#define MURMUR3_C2 0x4cf5ad432745937fULL

static inline uint64_t murmur3_rotl64(uint64_t x, int8_t r) {
  return (x << r) | (x >> (64 - r));
}

static inline uint64_t murmur3_fmix64(uint64_t k) {
  k ^= k >> 33;
  k *= 0xff51afd7ed558ccdULL;
  k ^= k >> 33;
  k *= 0xc4ceb9fe1a85ec53ULL;
  k ^= k >> 33;
  return k;
}

static inline uint64_t murmur3_read64(const uint8_t *p) {
  return ((uint64_t)p[0]) | ((uint64_t)p[1] << 8) | ((uint64_t)p[2] << 16) |
         ((uint64_t)p[3] << 24) | ((uint64_t)p[4] << 32) |
         ((uint64_t)p[5] << 40) | ((uint64_t)p[6] << 48) |
         ((uint64_t)p[7] << 56);
}

uint64_t murmur3_64(const void *key, size_t len, uint64_t seed) {
  const uint8_t *data = (const uint8_t *)key;
  const size_t nblocks = len / 16;

  uint64_t h1 = seed;
  uint64_t h2 = seed;

  /* Body - process 16-byte blocks */
  const uint8_t *blocks = data;
  for (size_t i = 0; i < nblocks; i++) {
    uint64_t k1 = murmur3_read64(blocks + i * 16);
    uint64_t k2 = murmur3_read64(blocks + i * 16 + 8);

    k1 *= MURMUR3_C1;
    k1 = murmur3_rotl64(k1, 31);
    k1 *= MURMUR3_C2;
    h1 ^= k1;

    h1 = murmur3_rotl64(h1, 27);
    h1 += h2;
    h1 = h1 * 5 + 0x52dce729;

    k2 *= MURMUR3_C2;
    k2 = murmur3_rotl64(k2, 33);
    k2 *= MURMUR3_C1;
    h2 ^= k2;

    h2 = murmur3_rotl64(h2, 31);
    h2 += h1;
    h2 = h2 * 5 + 0x38495ab5;
  }

  /* Tail - handle remaining bytes */
  const uint8_t *tail = data + nblocks * 16;
  uint64_t k1 = 0;
  uint64_t k2 = 0;

  switch (len & 15) {
  case 15:
    k2 ^= ((uint64_t)tail[14]) << 48;
    /* fallthrough */
  case 14:
    k2 ^= ((uint64_t)tail[13]) << 40;
    /* fallthrough */
  case 13:
    k2 ^= ((uint64_t)tail[12]) << 32;
    /* fallthrough */
  case 12:
    k2 ^= ((uint64_t)tail[11]) << 24;
    /* fallthrough */
  case 11:
    k2 ^= ((uint64_t)tail[10]) << 16;
    /* fallthrough */
  case 10:
    k2 ^= ((uint64_t)tail[9]) << 8;
    /* fallthrough */
  case 9:
    k2 ^= ((uint64_t)tail[8]) << 0;
    k2 *= MURMUR3_C2;
    k2 = murmur3_rotl64(k2, 33);
    k2 *= MURMUR3_C1;
    h2 ^= k2;
    /* fallthrough */
  case 8:
    k1 ^= ((uint64_t)tail[7]) << 56;
    /* fallthrough */
  case 7:
    k1 ^= ((uint64_t)tail[6]) << 48;
    /* fallthrough */
  case 6:
    k1 ^= ((uint64_t)tail[5]) << 40;
    /* fallthrough */
  case 5:
    k1 ^= ((uint64_t)tail[4]) << 32;
    /* fallthrough */
  case 4:
    k1 ^= ((uint64_t)tail[3]) << 24;
    /* fallthrough */
  case 3:
    k1 ^= ((uint64_t)tail[2]) << 16;
    /* fallthrough */
  case 2:
    k1 ^= ((uint64_t)tail[1]) << 8;
    /* fallthrough */
  case 1:
    k1 ^= ((uint64_t)tail[0]) << 0;
    k1 *= MURMUR3_C1;
    k1 = murmur3_rotl64(k1, 31);
    k1 *= MURMUR3_C2;
    h1 ^= k1;
  }

  /* Finalization */
  h1 ^= len;
  h2 ^= len;

  h1 += h2;
  h2 += h1;

  h1 = murmur3_fmix64(h1);
  h2 = murmur3_fmix64(h2);

  h1 += h2;
  /* h2 += h1; // Not needed, we only return h1 */

  return h1;
}
