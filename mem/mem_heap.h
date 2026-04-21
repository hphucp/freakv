#ifndef MEM_HEAP_H
#define MEM_HEAP_H

#include "mem_arena.h"
#include "mem_buddy.h"
#include "mem_pool.h"
#include <stdbool.h>
#include <stdint.h>

/* ── Eviction callback ───────────────────────────────────────────────── */

/**
 * Eviction callback invoked when allocator needs memory.
 *
 * @param ctx User context
 * @param bytes_needed Minimum bytes to free
 * @return Number of bytes actually freed
 */
typedef size_t (*mem_evict_fn)(void *ctx, size_t bytes_needed);

/* ── struct thread_heap ──────────────────────────────────────────────── */

struct thread_heap {
  uint32_t thread_id;

  /* Data region pointers */
  char *data_base;       /* Start of this thread's data slice */
  size_t data_reserved;  /* Total VA reserved (per_thread_data_size) */
  size_t data_committed; /* Physical memory committed (in bytes) */

  /* Metadata pointers (in separate VA region) */
  struct page_info *pages;        /* page_info array in meta_base */
  struct buddy_node *buddy_nodes; /* buddy_node array in meta_base */
  uint64_t *dirty_bits;           /* active page bitmap */
  uint64_t *snap_dirty_bits;      /* frozen snapshot bitmap */
  uint8_t *slot_bits;      /* active per-page slot bitmaps (contiguous) */
  uint8_t *snap_slot_bits; /* frozen per-page slot bitmaps (contiguous) */
  size_t meta_committed;   /* Metadata physical memory committed */

  uint32_t
      nr_pages_capacity; /* VA capacity - set once at init, never changes */
  uint32_t
      nr_pages_committed;  /* grows in mem_data_grow alongside data_committed */
  uint32_t nr_pages_dirty; /* Number of pages currently allocated */

  /* Allocators */
  struct buddy_allocator buddy;
  struct small_pool pools[NR_SMALL_POOLS];
  struct small_pool span_meta_pool; /* Specialized pool for span_info structs */
  struct kv_obj *snap_large_freelist; /* Deferred frees for large objects */

  /* Eviction */
  mem_evict_fn evict_fn;
  void *evict_ctx;

  /* Memory limit (0 = unlimited) */
  size_t limit_bytes;
};

/* Global array of thread heaps (allocated by mem_thread_init) */
extern struct thread_heap *g_thread_heaps;

/* ── Initialization ──────────────────────────────────────────────────── */

/**
 * Initialize thread heap structure.
 * Called by mem_thread_init() from mem_arena.c
 *
 * @param heap Heap structure to initialize
 * @param thread_id Thread identifier
 * @return true on success, false on failure
 */
bool thread_heap_init(struct thread_heap *heap, uint32_t thread_id);

/**
 * Clean up thread heap structure.
 * Called by mem_thread_destroy() from mem_arena.c
 *
 * @param heap Heap structure to clean up
 */
void thread_heap_destroy(struct thread_heap *heap);

/* ── Memory growth ───────────────────────────────────────────────────── */

/**
 * Grow data region by committing more physical pages.
 * Metadata grows in lockstep.
 *
 * @param heap Thread heap
 * @param new_committed New committed size in bytes (must be HPAGE-aligned)
 * @return true on success, false on failure
 */
bool mem_data_grow(struct thread_heap *heap, size_t new_committed);

/* ── Eviction ────────────────────────────────────────────────────────── */

/**
 * Register eviction callback for OOM handling.
 *
 * @param thread_id Thread identifier
 * @param evict_fn Eviction function
 * @param evict_ctx User context passed to eviction function
 */
void mem_evict_register(uint32_t thread_id, mem_evict_fn evict_fn,
                        void *evict_ctx);

/**
 * Set memory limit for a thread.
 *
 * @param thread_id Thread identifier
 * @param limit_bytes Maximum bytes (0 = unlimited)
 */
void mem_limit_set(uint32_t thread_id, size_t limit_bytes);

/**
 * Get current memory usage for a thread.
 *
 * @param thread_id Thread identifier
 * @return Number of bytes currently allocated
 */
size_t mem_usage_get(uint32_t thread_id);

#endif /* MEM_HEAP_H */
