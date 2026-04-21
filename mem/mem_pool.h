#ifndef MEM_POOL_H
#define MEM_POOL_H

#include "mem_buddy.h"
#include <stddef.h>
#include <stdint.h>

/*
 * mem_pool.h — slab allocator on top of the buddy (4KB logical pages).
 */

#ifndef USE_OLD_MERGE
#define USE_OLD_MERGE 1
#endif

#define NR_SMALL_POOLS 36
#define SMALL_ALLOC_MAX 65536

#define MERGE_NODE_SIZE 128

struct small_pool {
  uint32_t obj_size;        /* size class in bytes                      */
  uint32_t lpages_per_span; /* logical (4KB) pages per span             */
  uint32_t objs_per_span;   /* floor(span_bytes / obj_size)             */

  struct span_info *partial_spans; /* Head of spans with free slots         */
  struct span_info *empty_spans;   /* Head of reserved empty spans          */
  uint32_t nr_empty;

  /* SPSC Wait-Free Ring Buffer */
  _Atomic uint32_t merge_head;
  _Atomic uint32_t merge_tail;
  uint32_t merge_queue[128];
  uint32_t merge_q_size;

  /* Lock-Free Fallback Stack */
  _Atomic uint32_t to_merge_head;
};

static const uint32_t POOL_CLASSES[] = {
    8,     16,    32,    48,    64,    80,    112,   144,   192,
    240,   304,   384,   480,   608,   768,   960,   1200,  1504,
    1888,  2368,  2960,  3712,  4096,  5120,  6400,  8000,  10000,
    12512, 15648, 19568, 24464, 30592, 38240, 47808, 59760, 65536,
};

_Static_assert(sizeof(POOL_CLASSES) / sizeof(POOL_CLASSES[0]) == NR_SMALL_POOLS,
               "");

static inline uint32_t pool_idx_get(size_t size) {
  for (uint32_t i = 0; i < NR_SMALL_POOLS; i++)
    if (size <= POOL_CLASSES[i])
      return i;
  return NR_SMALL_POOLS;
}

static inline uint32_t pool_lpages_calculate(uint32_t obj_size) {
  uint32_t min_bytes = obj_size * 64;
  uint32_t lpages =
      (min_bytes + (uint32_t)LPAGE_SIZE - 1) / (uint32_t)LPAGE_SIZE;
  return (lpages == 0) ? 1 : lpages;
}

void pool_init(struct small_pool *pool, uint32_t obj_size);
void pool_set_init(struct small_pool *pools, uint32_t count);

void *pool_obj_alloc(struct small_pool *pool, struct buddy_allocator *buddy,
                     struct page_info *pages, struct small_pool *meta_pool,
                     char *base);

void pool_obj_free(struct small_pool *pool, struct span_info *span, void *ptr);

void pool_snap_defer_free(struct small_pool *pool, struct span_info *span,
                          void *ptr);
bool pool_snap_enqueue(struct small_pool *pool, struct span_info *span);
void pool_snap_merge_incremental(struct small_pool *pool,
                                 struct page_info *pages, uint32_t limit);
void pool_snap_merge(struct small_pool *pool, struct page_info *pages);

void pool_span_force_reclaim_all(struct small_pool *pools,
                                 struct buddy_allocator *buddy,
                                 struct page_info *pages,
                                 struct small_pool *meta_pool, char *base);

#endif /* MEM_POOL_H */
