#include "mem_pool.h"
#include <assert.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

void pool_init(struct small_pool *pool, uint32_t obj_size) {
  uint32_t lpages = pool_lpages_calculate(obj_size);
  uint32_t span_bytes = lpages * (uint32_t)LPAGE_SIZE;

  pool->obj_size = obj_size;
  pool->lpages_per_span = lpages;
  pool->objs_per_span = span_bytes / obj_size;

  pool->partial_spans = NULL;
  pool->empty_spans = NULL;
  pool->nr_empty = 0;

  atomic_init(&pool->merge_head, 0);
  atomic_init(&pool->merge_tail, 0);
  pool->merge_q_size = 128;
  atomic_init(&pool->to_merge_head, INVALID);
}

void pool_set_init(struct small_pool *pools, uint32_t count) {
  for (uint32_t i = 0; i < count; i++) {
    pool_init(&pools[i], POOL_CLASSES[i]);
  }
}

static inline void pool_list_add_head(struct span_info **head,
                                      struct span_info *span) {
  span->next = *head;
  span->prev = NULL;
  if (*head)
    (*head)->prev = span;
  *head = span;
}

static inline void pool_list_remove(struct span_info **head,
                                    struct span_info *span) {
  if (span->prev)
    span->prev->next = span->next;
  else
    *head = span->next;
  if (span->next)
    span->next->prev = span->prev;
  span->next = span->prev = NULL;
}

static void pool_span_init(struct small_pool *pool, struct span_info *span,
                           uint32_t head_lpage, uint32_t span_size,
                           struct page_info *pages, char *base) {
  span->pool = pool;
  span->head_lpage = head_lpage;
  span->span_size = span_size;
  span->nr_alloc = 0;
  span->nr_snap_alloc = 0;
  span->freelist = NULL;
  span->snap_freelist = NULL;
  span->snap_freelist_tail = NULL;
  span->next = span->prev = NULL;
  span->next_to_merge = INVALID;

  char *slab = base + (size_t)head_lpage * LPAGE_SIZE;
  uint32_t sz = pool->obj_size;

  /* Initialize freelist in the span */
  for (uint32_t i = 0; i < pool->objs_per_span; i++) {
    struct free_obj *o = (struct free_obj *)(slab + (size_t)i * sz);
    o->next = span->freelist;
    span->freelist = o;
  }

  /* Update page metadata */
  for (uint32_t i = 0; i < span_size; i++) {
    pages[head_lpage + i].span = span;
    pages[head_lpage + i].span_size = span_size;
    pages[head_lpage + i].offset_in_span = (uint16_t)i;
  }
}

/* Internal version for allocating span_info from span_meta_pool.
 * To break recursion, the first slot of the allocated span is used as its own
 * span_info. */
static struct span_info *pool_meta_alloc(struct small_pool *pool,
                                         struct buddy_allocator *buddy,
                                         struct page_info *pages, char *base) {
  uint32_t n = pool->lpages_per_span;
  uint32_t head = buddy_span_alloc(buddy, n);
  if (head == INVALID)
    return NULL;

  uint32_t actual = pages[head].span_size;
  char *slab = base + (size_t)head * LPAGE_SIZE;
  uint32_t sz = pool->obj_size;
  uint32_t n_objs = pool->objs_per_span;

  /* The first slot is the span_info for this span. */
  struct span_info *ms = (struct span_info *)slab;

  ms->pool = pool;
  ms->head_lpage = head;
  ms->span_size = actual;
  ms->nr_alloc = 1; /* ms itself is allocated */
  ms->nr_snap_alloc = 0;
  ms->freelist = NULL;
  ms->snap_freelist = NULL;
  ms->snap_freelist_tail = NULL;
  ms->next = ms->prev = NULL;
  ms->next_to_merge = INVALID;

  /* Initialize page metadata pointing to ms */
  for (uint32_t i = 0; i < actual; i++) {
    pages[head + i].span = ms;
    pages[head + i].span_size = actual;
    pages[head + i].offset_in_span = (uint16_t)i;
  }

  /* Build freelist from remaining slots (starting at slot 1) */
  for (uint32_t i = 1; i < n_objs; i++) {
    struct free_obj *o = (struct free_obj *)(slab + (size_t)i * sz);
    o->next = ms->freelist;
    ms->freelist = o;
  }

  pool_list_add_head(&pool->partial_spans, ms);
  return ms;
}

void *pool_obj_alloc(struct small_pool *pool, struct buddy_allocator *buddy,
                     struct page_info *pages, struct small_pool *meta_pool,
                     char *base) {
  struct span_info *span = pool->partial_spans;
  if (!span) {
    /* 1. Try incremental merge queue first */
    pool_snap_merge_incremental(pool, pages, 16);
    if (pool->partial_spans)
      return pool_obj_alloc(pool, buddy, pages, meta_pool, base);

    /* 2. Drain the queue completely if incremental didn't help (fallback) */
    pool_snap_merge(pool, pages);
    if (pool->partial_spans)
      return pool_obj_alloc(pool, buddy, pages, meta_pool, base);

    /* 3. Actually grow the pool from buddy */
    uint32_t n = pool->lpages_per_span;
    uint32_t head = buddy_span_alloc(buddy, n);
    if (head == INVALID)
      return NULL;

    uint32_t actual = pages[head].span_size;
    /* Allocate span_info from meta_pool */
    if (pool == meta_pool) {
      span = pool_meta_alloc(pool, buddy, pages, base);
    } else {
      struct span_info *ms = (struct span_info *)pool_obj_alloc(
          meta_pool, buddy, pages, meta_pool, base);
      if (!ms) {
        buddy_span_free(buddy, head, actual);
        return NULL;
      }
      pool_span_init(pool, ms, head, actual, pages, base);
      pool_list_add_head(&pool->partial_spans, ms);
      span = ms;
    }

    if (!span) {
      buddy_span_free(buddy, head, actual);
      return NULL;
    }
  }

  struct free_obj *obj = span->freelist;
  span->freelist = obj->next;
  span->nr_alloc++;

  if (!span->freelist) {
    pool_list_remove(&pool->partial_spans, span);
  }

  return obj;
}

void pool_obj_free(struct small_pool *pool, struct span_info *span, void *ptr) {
  struct free_obj *obj = (struct free_obj *)ptr;
  bool was_full = (span->freelist == NULL);

  obj->next = span->freelist;
  span->freelist = obj;
  span->nr_alloc--;

  if (span->nr_alloc == 0) {
    if (!was_full)
      pool_list_remove(&pool->partial_spans, span);
    if (pool->obj_size == sizeof(struct span_info)) {
      pool_list_add_head(&pool->partial_spans, span);
      return;
    }
    pool_list_add_head(&pool->empty_spans, span);
    pool->nr_empty++;
  } else if (was_full) {
    pool_list_add_head(&pool->partial_spans, span);
  } else {
    /* LIFO: move to head */
    pool_list_remove(&pool->partial_spans, span);
    pool_list_add_head(&pool->partial_spans, span);
  }
}

void pool_snap_defer_free(struct small_pool *pool, struct span_info *span,
                          void *ptr) {
  (void)pool; /* unused */
  struct free_obj *obj = (struct free_obj *)ptr;
  obj->next = span->snap_freelist;
  span->snap_freelist = obj;
  if (!span->snap_freelist_tail)
    span->snap_freelist_tail = obj;
  span->nr_snap_alloc++;

  /* Defer free handles linking the object. Atomic enqueue happens in snapshot.c
   */
}

bool pool_snap_enqueue(struct small_pool *pool, struct span_info *span) {
  uint32_t head_lpage = span->head_lpage;
  uint32_t tail = atomic_load_explicit(&pool->merge_tail, memory_order_relaxed);
  uint32_t next = (tail + 1) % pool->merge_q_size;
  uint32_t head = atomic_load_explicit(&pool->merge_head, memory_order_acquire);

  if (next != head) {
    pool->merge_queue[tail] = head_lpage;
    atomic_store_explicit(&pool->merge_tail, next, memory_order_release);
    return true;
  }
  return false;
}

static void span_merge_logic(struct small_pool *pool, struct span_info *span) {
  if (!span->snap_freelist)
    return;

  struct free_obj *tail = span->snap_freelist_tail;
  tail->next = span->freelist;
  span->freelist = span->snap_freelist;

  bool was_full = (tail->next == NULL);
  span->nr_alloc -= span->nr_snap_alloc;

  span->snap_freelist = NULL;
  span->snap_freelist_tail = NULL;
  span->nr_snap_alloc = 0;

  span->next_to_merge = INVALID;

  if (span->nr_alloc == 0) {
    if (!was_full)
      pool_list_remove(&pool->partial_spans, span);
    pool_list_add_head(&pool->empty_spans, span);
    pool->nr_empty++;
  } else if (was_full) {
    pool_list_add_head(&pool->partial_spans, span);
  }
}

void pool_snap_merge_incremental(struct small_pool *pool,
                                 struct page_info *pages, uint32_t limit) {
  uint32_t count = 0;
  while (count < limit) {
    uint32_t h = atomic_load_explicit(&pool->merge_head, memory_order_relaxed);
    uint32_t t = atomic_load_explicit(&pool->merge_tail, memory_order_acquire);
    if (h == t)
      break;

    uint32_t head_lpage = pool->merge_queue[h];
    struct span_info *span = pages[head_lpage].span;

    if (span)
      span_merge_logic(pool, span);

    atomic_store_explicit(&pool->merge_head, (h + 1) % pool->merge_q_size,
                          memory_order_release);
    count++;
  }
}

void pool_snap_merge(struct small_pool *pool, struct page_info *pages) {
  pool_snap_merge_incremental(pool, pages, pool->merge_q_size);

  // Drain lock-free stack
  uint32_t head = atomic_exchange_explicit(&pool->to_merge_head, INVALID,
                                           memory_order_acquire);
  while (head != INVALID) {
    struct span_info *span = pages[head].span;
    if (!span)
      break;
    uint32_t next = span->next_to_merge;
    span_merge_logic(pool, span);
    head = next;
  }
}

void pool_span_force_reclaim_all(struct small_pool *pools,
                                 struct buddy_allocator *buddy,
                                 struct page_info *pages,
                                 struct small_pool *meta_pool, char *base) {
  for (int i = 0; i < NR_SMALL_POOLS; i++) {
    struct small_pool *pool = &pools[i];
    while (pool->empty_spans) {
      struct span_info *span = pool->empty_spans;
      pool_list_remove(&pool->empty_spans, span);
      uint32_t head = span->head_lpage;
      uint32_t sz = span->span_size;

      buddy_span_free(buddy, head, sz);
      pool->nr_empty--;

      if (pool != meta_pool) {
        pool_obj_free(meta_pool, pages[ptr_lpage_idx(span)].span, span);
      }
    }
  }
}
