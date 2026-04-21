#ifndef MEM_INTERNAL_H
#define MEM_INTERNAL_H

/*
 * mem_internal.h — Internal helpers for mem/ module implementation.
 * NOT part of the public API - do not include from outside mem/ directory.
 */

#include <stdbool.h>
#include <stdint.h>

/* ── Dirty bitarray helpers (internal only) ──────────────────────────── */

static inline void dirty_bit_set(uint64_t *dirty_bits, uint32_t lpage) {
  dirty_bits[lpage >> 6] |= (1ULL << (lpage & 63));
}

static inline void dirty_bit_clear(uint64_t *dirty_bits, uint32_t lpage) {
  dirty_bits[lpage >> 6] &= ~(1ULL << (lpage & 63));
}

static inline bool dirty_bit_test(const uint64_t *dirty_bits, uint32_t lpage) {
  return (dirty_bits[lpage >> 6] >> (lpage & 63)) & 1;
}

static inline uint32_t dirty_bit_next(const uint64_t *dirty_bits, uint32_t from,
                                      uint32_t nr_pages) {
  uint32_t word = from >> 6;
  uint32_t bit = from & 63;
  uint32_t nr_words = (nr_pages + 63) >> 6;

  if (word < nr_words) {
    uint64_t masked = dirty_bits[word] >> bit;
    if (masked) {
      uint32_t pos = word * 64 + bit + (uint32_t)__builtin_ctzll(masked);
      return pos < nr_pages ? pos : nr_pages;
    }
  }
  for (uint32_t w = word + 1; w < nr_words; w++) {
    if (dirty_bits[w]) {
      uint32_t pos = w * 64 + (uint32_t)__builtin_ctzll(dirty_bits[w]);
      return pos < nr_pages ? pos : nr_pages;
    }
  }
  return nr_pages;
}

#endif /* MEM_INTERNAL_H */
