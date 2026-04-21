#include "mem_api.h"
#include "../core/object.h"
#include "../core/snapshot.h"
#include "mem_arena.h"
#include "mem_buddy.h"
#include "mem_heap.h"
#include "mem_pool.h"

#include <pthread.h>
#include <stdio.h>
#include <string.h>

static __thread uint32_t tls_thread_id = 0;
static __thread bool tls_thread_id_set = false;

void mem_set_thread_id(uint32_t thread_id) {
  tls_thread_id = thread_id;
  tls_thread_id_set = true;
}

uint32_t mem_get_thread_id(void) {
  if (!tls_thread_id_set)
    return 0;
  return tls_thread_id;
}

static size_t mem_alloc_size(void *ptr) {
  if (!ptr)
    return 0;
  uint32_t thread_id = ptr_thread_id(ptr);
  if (thread_id >= g_mem_arena.n_threads)
    return 0;

  struct thread_heap *heap = &g_thread_heaps[thread_id];
  uint32_t lpage = ptr_lpage_idx(ptr);
  if (lpage >= heap->nr_pages_committed)
    return 0;

  struct page_info *pi = &heap->pages[lpage];
  if (pi->span && pi->span->pool)
    return pi->span->pool->obj_size;
  if (pi->span_size > 0)
    return (size_t)pi->span_size * LPAGE_SIZE;
  return 0;
}

void *mem_malloc(size_t size) {
  if (size == 0)
    return NULL;
  uint32_t thread_id = mem_get_thread_id();
  if (thread_id >= g_mem_arena.n_threads)
    return NULL;

  struct thread_heap *heap = &g_thread_heaps[thread_id];

  if (size <= SMALL_ALLOC_MAX) {
    uint32_t pool_idx = pool_idx_get(size);
    if (pool_idx < NR_SMALL_POOLS) {
      void *ptr =
          pool_obj_alloc(&heap->pools[pool_idx], &heap->buddy, heap->pages,
                         &heap->span_meta_pool, heap->data_base);
      if (ptr)
        return ptr;

      if (mem_data_grow(heap, heap->data_committed + HPAGE_SIZE)) {
        return pool_obj_alloc(&heap->pools[pool_idx], &heap->buddy, heap->pages,
                              &heap->span_meta_pool, heap->data_base);
      }
      return NULL;
    }
  }

  uint32_t npages = (uint32_t)((size + LPAGE_SIZE - 1) / LPAGE_SIZE);
  uint32_t lpage = buddy_span_alloc(&heap->buddy, npages);
  if (lpage == INVALID) {
    if (mem_data_grow(heap, heap->data_committed + HPAGE_SIZE)) {
      lpage = buddy_span_alloc(&heap->buddy, npages);
    }
  }
  return (lpage == INVALID) ? NULL
                            : (heap->data_base + (size_t)lpage * LPAGE_SIZE);
}

void *mem_calloc(size_t nmemb, size_t size) {
  size_t total = nmemb * size;
  if (nmemb > 0 && total / nmemb != size)
    return NULL;
  void *ptr = mem_malloc(total);
  if (ptr)
    memset(ptr, 0, total);
  return ptr;
}

void *mem_realloc(void *ptr, size_t new_size) {
  if (!ptr)
    return mem_malloc(new_size);
  if (new_size == 0) {
    mem_free(ptr);
    return NULL;
  }
  size_t old_size = mem_alloc_size(ptr);
  if (old_size == 0 || new_size <= old_size)
    return ptr;
  void *new_ptr = mem_malloc(new_size);
  if (!new_ptr)
    return NULL;
  memcpy(new_ptr, ptr, old_size);
  mem_free(ptr);
  return new_ptr;
}

void mem_free(void *ptr) {
  if (!ptr)
    return;
  uint32_t thread_id = ptr_thread_id(ptr);
  if (thread_id >= g_mem_arena.n_threads)
    return;

  struct thread_heap *heap = &g_thread_heaps[thread_id];
  uint32_t lpage = ptr_lpage_idx(ptr);
  if (lpage >= heap->nr_pages_committed)
    return;

  struct page_info *pi = &heap->pages[lpage];

  if (pi->span && pi->span->pool) {
    pool_obj_free(pi->span->pool, pi->span, ptr);
  } else if (pi->span_size > 0) {
    buddy_span_free(&heap->buddy, lpage, pi->span_size);
  }
}

void mem_snap_free(void *ptr) {
  if (!ptr)
    return;
  uint32_t thread_id = ptr_thread_id(ptr);
  if (thread_id >= g_mem_arena.n_threads)
    return;

  struct thread_heap *heap = &g_thread_heaps[thread_id];
  uint32_t lpage = ptr_lpage_idx(ptr);
  if (lpage >= heap->nr_pages_committed)
    return;

  struct page_info *pi = &heap->pages[lpage];

  if (pi->span && pi->span->pool) {
    pool_snap_defer_free(pi->span->pool, pi->span, ptr);
  } else if (pi->span_size > 0) {
    struct kv_obj *obj = (struct kv_obj *)ptr;
    obj->lru_next = heap->snap_large_freelist;
    heap->snap_large_freelist = obj;
  }
}
