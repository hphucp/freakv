#ifndef MEM_ARENA_H
#define MEM_ARENA_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* ── Page size constants ────────────────────────────────────────────── */

#define HPAGE_SIZE (2 * 1024 * 1024) /* 2MB - mmap commit granularity */
#define LPAGE_SIZE (4 * 1024)        /* 4KB - buddy/pool tracking granularity */
#define LPAGES_PER_HPAGE (HPAGE_SIZE / LPAGE_SIZE) /* 512 */

#define LPAGE_SHIFT 12 /* log2(LPAGE_SIZE) */
#define HPAGE_SHIFT 21 /* log2(HPAGE_SIZE) */

/* ── VA reservation sizes ────────────────────────────────────────────── */

#define ARENA_DATA_RESERVE (1ULL << 40)  /* 1TB total for data */
#define ARENA_META_RESERVE (64ULL << 30) /* 64GB total for metadata */
/* 2^32 bucket slots * 32 bytes. This is reserved per shard with
 * MAP_NORESERVE; physical pages are committed only as the HT/OVF arenas grow. */
#define ARENA_LINEAR_RESERVE (1ULL << 37) /* 128GiB per linear arena slice */

/* ── Global VA layout ────────────────────────────────────────────────── */

struct mem_global_arena {
  uintptr_t data_base;  /* Start of data VA region */
  uintptr_t meta_base;  /* Start of metadata VA region */
  uintptr_t arena_base; /* Start of linear arena VA region */

  size_t per_thread_data_size;  /* Must be power-of-2 */
  size_t per_thread_meta_size;  /* Metadata per thread */
  size_t per_thread_arena_size; /* Linear arena per thread */

  uint32_t n_threads;
  uint32_t thread_id_shift; /* log2(per_thread_data_size) */
};

/* Global singleton */
extern struct mem_global_arena g_mem_arena;

/* ── Pointer decoding ────────────────────────────────────────────────── */

/* Extract thread ID from allocated pointer */
static inline uint32_t ptr_thread_id(const void *ptr) {
  uintptr_t offset = (uintptr_t)ptr - g_mem_arena.data_base;
  return (uint32_t)(offset >> g_mem_arena.thread_id_shift);
}

/* Extract logical page index within thread's data slice */
static inline uint32_t ptr_lpage_idx(const void *ptr) {
  uintptr_t offset = (uintptr_t)ptr - g_mem_arena.data_base;
  uint32_t thread_id = (uint32_t)(offset >> g_mem_arena.thread_id_shift);
  uintptr_t thread_base = g_mem_arena.data_base +
                          ((uintptr_t)thread_id << g_mem_arena.thread_id_shift);
  return (uint32_t)((uintptr_t)ptr - thread_base) >> LPAGE_SHIFT;
}

/* Get thread's data slice base address */
static inline uintptr_t thread_data_base(uint32_t thread_id) {
  return g_mem_arena.data_base +
         ((uintptr_t)thread_id << g_mem_arena.thread_id_shift);
}

/* Get thread's metadata slice base address */
static inline uintptr_t thread_meta_base(uint32_t thread_id) {
  return g_mem_arena.meta_base + (thread_id * g_mem_arena.per_thread_meta_size);
}

/* Get thread's linear arena slice base address */
static inline uintptr_t thread_arena_base(uint32_t thread_id) {
  return g_mem_arena.arena_base +
         (thread_id * g_mem_arena.per_thread_arena_size);
}

/* ── Initialization ──────────────────────────────────────────────────── */

/**
 * Initialize global memory arena with VA reservation.
 *
 * @param n_threads Number of threads/shards
 * @param per_thread_hint Suggested per-thread data size (0 = auto)
 * @return true on success, false on failure
 *
 * Note: per_thread_data_size will be capped at:
 *   prev_power_of_2(MemAvailable * 0.80 / n_threads)
 */
bool mem_lib_init(uint32_t n_threads, size_t per_thread_hint);

/**
 * Initialize thread-local heap for the calling thread.
 * Must be called once per thread after mem_lib_init().
 *
 * @param thread_id Thread identifier (0..n_threads-1)
 * @return true on success, false on failure
 */
bool mem_thread_init(uint32_t thread_id);

/**
 * Clean up thread-local heap for the calling thread.
 * Uses madvise(MADV_DONTNEED) to decommit physical pages.
 *
 * @param thread_id Thread identifier (0..n_threads-1)
 */
void mem_thread_destroy(uint32_t thread_id);

/**
 * Clean up global memory arena.
 * Unmaps all VA regions.
 */
void mem_lib_destroy(void);

/* ── Utility functions ────────────────────────────────────────────────── */

/* Round down to previous power of 2 */
static inline size_t prev_power_of_2(size_t n) {
  if (n == 0)
    return 0;
  n |= n >> 1;
  n |= n >> 2;
  n |= n >> 4;
  n |= n >> 8;
  n |= n >> 16;
  n |= n >> 32;
  return n - (n >> 1);
}

/* Get log2 of power-of-2 value */
static inline uint32_t log2_u64(uint64_t n) {
  uint32_t log = 0;
  while (n > 1) {
    n >>= 1;
    log++;
  }
  return log;
}

#endif /* MEM_ARENA_H */
