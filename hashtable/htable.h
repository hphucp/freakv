#ifndef HTABLE_H
#define HTABLE_H

/*
 * htable.h — bucket hash table with chained overflow.
 *
 * ── struct bucket layout (32 bytes, cache-line aligned) ──────────────────────
 *
 *   Bytes  0- 7  │ meta    │ packed word:
 *                │         │   bits 63-8 (56 bits) = hash56
 *                │         │   bits  7-0 ( 8 bits) = enum val_type tag
 *   Bytes  8-15  │ expire  │ absolute expiry (ms epoch); 0 = no TTL
 *   Bytes 16-23  │ obj     │ pointer to struct kv_obj
 *   Bytes 24-31  │ next    │ pointer to next struct bucket in overflow chain
 *
 * Empty sentinel: meta == 0.  Valid entries always have hash56 != 0.
 *
 * ── Lookup ────────────────────────────────────────────────────────────
 *   index  = (meta >> 8) & (cap - 1)
 *   filter = compare meta in one 64-bit op (hash56 + type)
 *   confirm = memcmp full key only on meta match
 *   TTL:    if expire != 0 && net_time_ms_get >= expire → treat as missing
 *
 * ── Rehash ────────────────────────────────────────────────────────────
 *   In-place incremental doubling.  Old and new halves coexist until
 *   rehash_idx reaches old_cap.
 */

#include "../core/object.h"
#include "../mem/mem_heap.h"
#include "../mem/mem_linear.h"
#include "hash.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* ── wyhash v4 ──────────────────────────────────────────────────────────
 * ~1 ns/call for short keys.
 * Ref: https://github.com/wangyi-fudan/wyhash
 * ─────────────────────────────────────────────────────────────────── */

static inline uint64_t _wyrot(uint64_t x) { return (x >> 32) | (x << 32); }
static inline void _wymum(uint64_t *a, uint64_t *b) {
  __uint128_t r = (__uint128_t)(*a) * (*b);
  *a = (uint64_t)r;
  *b = (uint64_t)(r >> 64);
}
static inline uint64_t _wymix(uint64_t a, uint64_t b) {
  _wymum(&a, &b);
  return a ^ b;
}

static inline uint64_t ht_key_hash(const void *key, size_t len) {
  static const uint64_t s0 = 0xa0761d6478bd642fULL;
  static const uint64_t s1 = 0xe7037ed1a0b428dbULL;
  static const uint64_t s2 = 0x8ebc6af09c88c6e3ULL;
  static const uint64_t s3 = 0x589965cc75374cc3ULL;
  const uint8_t *p = (const uint8_t *)key;
  uint64_t seed = s0, a, b;
  size_t i = len;

  if (__builtin_expect(i <= 16, 1)) {
    if (__builtin_expect(i >= 4, 1)) {
      uint32_t lo, hi, m1, m2;
      __builtin_memcpy(&lo, p, 4);
      __builtin_memcpy(&hi, p + i - 4, 4);
      a = ((uint64_t)lo << 32) | hi;
      if (i > 8) {
        __builtin_memcpy(&m1, p + 4, 4);
        __builtin_memcpy(&m2, p + i - 8, 4);
        b = ((uint64_t)m1 << 32) | m2;
      } else {
        b = _wyrot(a);
      }
    } else if (__builtin_expect(i > 0, 1)) {
      a = ((uint64_t)p[0] << 16) | ((uint64_t)p[i >> 1] << 8) | p[i - 1];
      b = 0;
    } else {
      a = b = 0;
    }
  } else {
    size_t j = i;
    while (__builtin_expect(j > 48, 0)) {
      uint64_t see1 = seed, see2 = seed, w0, w1, w2, w3, w4, w5;
      __builtin_memcpy(&w0, p, 8);
      __builtin_memcpy(&w1, p + 8, 8);
      __builtin_memcpy(&w2, p + 16, 8);
      __builtin_memcpy(&w3, p + 24, 8);
      __builtin_memcpy(&w4, p + 32, 8);
      __builtin_memcpy(&w5, p + 40, 8);
      seed = _wymix(w0 ^ s1, w1 ^ seed);
      see1 = _wymix(w2 ^ s2, w3 ^ see1);
      see2 = _wymix(w4 ^ s3, w5 ^ see2);
      p += 48;
      j -= 48;
      seed ^= see1 ^ see2;
    }
    while (__builtin_expect(j > 16, 0)) {
      uint64_t w0, w1;
      __builtin_memcpy(&w0, p, 8);
      __builtin_memcpy(&w1, p + 8, 8);
      seed = _wymix(w0 ^ s1, w1 ^ seed);
      p += 16;
      j -= 16;
    }
    uint64_t w0, w1;
    __builtin_memcpy(&w0, p + j - 16, 8);
    __builtin_memcpy(&w1, p + j - 8, 8);
    a = w0;
    b = w1;
  }
  return _wymix(s1 ^ len, _wymix(a ^ s1, b ^ seed));
}

/* ── meta word ──────────────────────────────────────────────────────────
 *   bits[63:8] = top 56 bits of hash
 *   bits[ 7:0] = enum val_type
 *   meta == 0  is the empty sentinel
 * ─────────────────────────────────────────────────────────────────── */

static inline uint64_t ht_meta_pack(uint64_t h, enum val_type type) {
  uint64_t m = (h & ~(uint64_t)0xFF) | (uint64_t)(uint8_t)type;
  return m ? m : 1u;
}

static inline bool ht_meta_empty(uint64_t m) { return m == 0; }
static inline enum val_type ht_meta_type(uint64_t m) {
  return (enum val_type)(m & 0xFF);
}

/* ── struct bucket
 * ───────────────────────────────────────────────────────────── */

struct bucket {
  uint64_t meta;       /*  0: hash56 | enum val_type                      */
  uint64_t expire;     /*  8: expiry epoch ms (0 = none)            */
  struct kv_obj *obj;  /* 16: the stored object (current live ptr)  */
  struct bucket *next; /* 24: overflow chain                        */
} __attribute__((aligned(32)));

_Static_assert(sizeof(struct bucket) == 32, "struct bucket must be 32 bytes");
_Static_assert(offsetof(struct bucket, meta) == 0, "");
_Static_assert(offsetof(struct bucket, expire) == 8, "");
_Static_assert(offsetof(struct bucket, obj) == 16, "");
_Static_assert(offsetof(struct bucket, next) == 24, "");

/* ── struct hash_table
 * ────────────────────────────────────────────────────────── */

#define HT_LOAD_MAX 0.75
#define HT_OVF_RATIO 8
#define HT_MIN_BUCKETS 16
#define HT_REHASH_STEP 64

struct hash_table {
  struct bucket *buckets;
  size_t cap;
  size_t old_cap;
  size_t used;
  int64_t rehash_idx; /* -1 = idle */

  struct bucket *ovf_pool;
  uint32_t ovf_pool_cap;
  struct bucket *ovf_free;

  /* Linear arenas for HT and OVF (owned by hash table) */
  struct linear_arena ht_arena;
  struct linear_arena ovf_arena;

  /* Thread heap for data allocations (key/value payloads) */
  struct thread_heap *heap;
};

/* ── API ──────────────────────────────────────────────────────────────── */

struct hash_table *ht_table_create(struct thread_heap *heap, uint32_t thread_id,
                                   size_t initial_cap);
void ht_table_destroy(struct hash_table *ht);

enum ht_put_flags {
  HT_PUT_NONE = 0,
  HT_PUT_NX = (1 << 0), /* Set only if key does NOT exist */
  HT_PUT_XX = (1 << 1), /* Set only if key ALREADY exists */
};

/* --- Bucket puts return old obj if updated, NULL if new, (struct kv_obj*)-2 if
 * condition failed --- */
struct kv_obj *ht_bucket_put(struct hash_table *ht, struct kv_obj *obj,
                             enum val_type type, uint64_t expire_ms,
                             uint32_t put_flags);

/*
 * ht_bucket_get — ht_key_lookup with TTL check.
 *   net_time_ms_get: current epoch ms (pass 0 to skip TTL check).
 * Returns obj or NULL (missing or expired).
 * Expired entries are NOT removed here; caller should call ht_bucket_delete.
 */
struct kv_obj *ht_bucket_get(struct hash_table *ht, const void *key,
                             size_t klen, enum val_type type,
                             uint64_t net_time_ms_get);

/*
 * ht_bucket_get_lazy — ht_key_lookup with inline lazy expiry (single hash,
 * single walk). Returns live obj, or NULL if missing/expired. On expired hit:
 * unlinks the bucket, stores stale obj in *expired_out. Caller must free
 * *expired_out if non-NULL.
 */
struct kv_obj *ht_bucket_get_lazy(struct hash_table *ht, const void *key,
                                  size_t klen, enum val_type type,
                                  uint64_t net_time_ms_get,
                                  struct kv_obj **expired_out);

/*
 * ht_bucket_set_lock_status — sets or clears the lock bit in the bucket meta.
 * Returns true if successful, false if key not found.
 */
bool ht_bucket_set_lock_status(struct hash_table *ht, const void *key,
                               size_t klen, enum val_type type, bool locked);

/*
 * ht_bucket_is_locked — returns true if the key is currently locked in the HT.
 */
bool ht_bucket_is_locked(struct hash_table *ht, const void *key, size_t klen,
                         enum val_type type);

/* ht_bucket_take — remove and return obj (caller frees it). */
struct kv_obj *ht_bucket_take(struct hash_table *ht, const void *key,
                              size_t klen, enum val_type type);

/* ht_bucket_take_if_unlocked — remove only if the bucket is unlocked and still
 * points at expected_obj.  If locked_out is non-NULL it is set when the matching
 * bucket exists but is locked. */
struct kv_obj *ht_bucket_take_if_unlocked(struct hash_table *ht,
                                          const void *key, size_t klen,
                                          enum val_type type,
                                          struct kv_obj *expected_obj,
                                          bool *locked_out);

/* ht_bucket_delete — remove and discard obj. */
bool ht_bucket_delete(struct hash_table *ht, const void *key, size_t klen,
                      enum val_type type);

/*
 * ht_bucket_expire — scan bucket at idx and its overflow chain.
 * Removes expired objects, stores struct kv_obj* in out[].
 * Returns count (up to out_cap). Caller frees each via mem_data_free.
 */
int ht_bucket_expire(struct hash_table *ht, size_t idx,
                     uint64_t net_time_ms_get, struct kv_obj **out,
                     int out_cap);

/*
 * ht_expire_get — return the expire field for a key.
 *   (uint64_t)-1 = key not found
 *   0            = no TTL
 *   >0           = absolute expiry epoch ms
 */
uint64_t ht_expire_get(struct hash_table *ht, const void *key, size_t klen,
                       enum val_type type);

bool ht_rehash_status(struct hash_table *ht);

/* ht_table_flush — remove all objects, free all struct kv_obj, reset table. */
void ht_table_flush(struct hash_table *ht);

static inline size_t ht_cap_get(const struct hash_table *ht) { return ht->cap; }

struct bucket *ht_bucket_random(struct hash_table *ht);

#endif /* HTABLE_H */
