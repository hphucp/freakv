#ifndef HASH_H
#define HASH_H

#include <stddef.h>
#include <stdint.h>

#ifndef VALKYD_HASH
#define VALKYD_HASH(data, len) xxhash64(data, len, 0)
#endif

static inline size_t next_power_of_2(size_t n) {
  if (n == 0)
    return 1;
  n--;
  n |= n >> 1;
  n |= n >> 2;
  n |= n >> 4;
  n |= n >> 8;
  n |= n >> 16;
  n |= n >> 32;
  return n + 1;
}

/**
 * xxHash64 - Fast non-cryptographic hash function
 * @data: pointer to data to hash
 * @len: length of data in bytes
 * @seed: seed value
 * @return: 64-bit hash value
 */
uint64_t xxhash64(const void *data, size_t len, uint64_t seed);

/**
 * MurmurHash3 - 64-bit variant (from 128-bit x64 version)
 * @key: pointer to data to hash
 * @len: length of data in bytes
 * @seed: seed value
 * @return: 64-bit hash value
 */
uint64_t murmur3_64(const void *key, size_t len, uint64_t seed);

/**
 * Hash an integer key
 * Uses xxhash64 by default
 */
static inline uint64_t hash_int(int key) {
  return xxhash64(&key, sizeof(key), 0);
}

#endif /* HASH_H */
