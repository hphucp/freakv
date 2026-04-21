#define _GNU_SOURCE
#include "mem_heap.h"
#include "mem_barrier.h"

#include <stdio.h>
#include <string.h>
#include <sys/mman.h>

/* Global thread heaps array */
struct thread_heap *g_thread_heaps = NULL;

/* ── thread_heap_init ────────────────────────────────────────────────── */

bool thread_heap_init(struct thread_heap *heap, uint32_t thread_id) {
  if (thread_id >= g_mem_arena.n_threads) {
    fprintf(stderr, "[mem_heap] Invalid thread_id=%u\n", thread_id);
    return false;
  }

  memset(heap, 0, sizeof(*heap));
  heap->thread_id = thread_id;

  /* Set base pointers */
  heap->data_base = (char *)thread_data_base(thread_id);
  heap->data_reserved = g_mem_arena.per_thread_data_size;
  heap->data_committed = 0;
  heap->meta_committed = 0; /* Bug 5 fix: initialize meta_committed */

  /* Metadata pointers (in separate VA region) */
  uintptr_t meta_start = thread_meta_base(thread_id);
  heap->pages = (struct page_info *)meta_start;

  /* Calculate VA capacity (not to be used for initialization loops!) */
  heap->nr_pages_capacity =
      (uint32_t)(g_mem_arena.per_thread_data_size / LPAGE_SIZE);

  /* buddy_nodes and dirty_bits follow pages array */
  size_t pages_bytes = heap->nr_pages_capacity * sizeof(struct page_info);
  size_t nodes_bytes = heap->nr_pages_capacity * sizeof(struct buddy_node);

  heap->buddy_nodes = (struct buddy_node *)((char *)heap->pages + pages_bytes);
  heap->dirty_bits = (uint64_t *)((char *)heap->buddy_nodes + nodes_bytes);

  size_t dirty_bytes = (heap->nr_pages_capacity + 63) / 64 * sizeof(uint64_t);
  heap->snap_dirty_bits = (uint64_t *)((char *)heap->dirty_bits + dirty_bytes);

  /* Contiguous per-page slot bitmaps (16 bytes per page) */
  size_t slot_bytes = (size_t)heap->nr_pages_capacity * SNAP_MODIFIED_SIZE;
  heap->slot_bits = (uint8_t *)((char *)heap->snap_dirty_bits + dirty_bytes);
  heap->snap_slot_bits = (uint8_t *)((char *)heap->slot_bits + slot_bytes);

  /* Initialize buddy allocator BEFORE initial data grow */
  buddy_alloc_init(&heap->buddy, heap->pages, heap->buddy_nodes,
                   heap->data_base, 0);

  /* Bug 1 & 2 fix: Commit initial data + metadata using mem_data_grow */
  if (!mem_data_grow(heap, HPAGE_SIZE)) {
    fprintf(stderr, "[mem_heap] Failed to grow initial data\n");
    return false;
  }

  uint32_t committed_pages = (uint32_t)(heap->data_committed / LPAGE_SIZE);

  /* Initialize pool allocators */
  pool_set_init(heap->pools, NR_SMALL_POOLS);
  pool_init(&heap->span_meta_pool, sizeof(struct span_info));

  /* No memory allocated yet, no eviction, no limit */
  heap->nr_pages_dirty = 0;
  heap->evict_fn = NULL;
  heap->evict_ctx = NULL;
  heap->limit_bytes = 0;

  printf("[mem_heap] Thread %u heap initialized: %zu MB reserved, %u committed "
         "pages\n",
         thread_id, heap->data_reserved / (1024 * 1024), committed_pages);

  return true;
}

/* ── thread_heap_destroy ─────────────────────────────────────────────── */

void thread_heap_destroy(struct thread_heap *heap) {
  if (!heap)
    return;

  /* Clean up is done by mem_thread_destroy in mem_arena.c via madvise */
  /* Here we just zero out the heap structure */
  memset(heap, 0, sizeof(*heap));
}

/* ── mem_data_grow ───────────────────────────────────────────────────── */

bool mem_data_grow(struct thread_heap *heap, size_t new_committed) {
  if (new_committed <= heap->data_committed) {
    return true; /* Already committed */
  }

  if (new_committed > heap->data_reserved) {
    fprintf(stderr, "[mem_heap] Cannot grow beyond reserved VA (%zu > %zu)\n",
            new_committed, heap->data_reserved);
    return false;
  }

  /* Check memory limit */
  if (heap->limit_bytes > 0 && new_committed > heap->limit_bytes) {
    /* Try eviction */
    if (heap->evict_fn) {
      size_t bytes_needed = new_committed - heap->data_committed;
      size_t freed = heap->evict_fn(heap->evict_ctx, bytes_needed);
      if (freed < bytes_needed) {
        fprintf(stderr,
                "[mem_heap] OOM: limit=%zu current=%zu needed=%zu freed=%zu\n",
                heap->limit_bytes, heap->data_committed, bytes_needed, freed);
        return false;
      }
    } else {
      fprintf(
          stderr,
          "[mem_heap] OOM: limit=%zu requested=%zu (no eviction callback)\n",
          heap->limit_bytes, new_committed);
      return false;
    }
  }

  /* Align to HPAGE boundary */
  size_t aligned = (new_committed + HPAGE_SIZE - 1) & ~(HPAGE_SIZE - 1);
  size_t grow_bytes = aligned - heap->data_committed;

  /* Check memory pressure before unlocking pages */
  size_t allowed = mem_barrier_allowed_bytes(grow_bytes);
  if (allowed < grow_bytes) {
    /* Try eviction */
    if (heap->evict_fn) {
      size_t freed = heap->evict_fn(heap->evict_ctx, grow_bytes);
      if (freed < grow_bytes) {
        fprintf(stderr,
                "[mem_heap] Memory pressure too high: "
                "requested=%zu allowed=%zu freed=%zu\n",
                grow_bytes, allowed, freed);
        return false;
      }
      /* Re-check after eviction */
      allowed = mem_barrier_allowed_bytes(grow_bytes);
    }

    if (allowed < grow_bytes) {
      fprintf(stderr,
              "[mem_heap] Memory pressure critical: "
              "requested=%zu allowed=%zu\n",
              grow_bytes, allowed);
      return false;
    }
  }

  /* Unlock the pre-reserved PROT_NONE region for read/write access */
  void *data_ptr = heap->data_base + heap->data_committed;
  if (mprotect(data_ptr, grow_bytes, PROT_READ | PROT_WRITE) != 0) {
    perror("[mem_heap] mprotect data commit");
    return false;
  }

  /* Calculate metadata requirements for new page count */
  uint32_t old_pages = (uint32_t)(heap->data_committed / LPAGE_SIZE);
  uint32_t new_pages = (uint32_t)(aligned / LPAGE_SIZE);
  uint32_t added_pages = new_pages - old_pages;

  /* Unlock metadata pages for `pages` array */
  uintptr_t p_old_end =
      ((uintptr_t)heap->pages + old_pages * sizeof(struct page_info) + 4095UL) &
      ~4095UL;
  uintptr_t p_new_end =
      ((uintptr_t)heap->pages + new_pages * sizeof(struct page_info) + 4095UL) &
      ~4095UL;
  if (p_new_end > p_old_end) {
    if (mprotect((void *)p_old_end, p_new_end - p_old_end,
                 PROT_READ | PROT_WRITE) != 0) {
      perror("[mem_heap] mprotect pages meta");
      return false;
    }
  }

  /* Unlock metadata pages for `buddy_nodes` array */
  uintptr_t n_old_end = ((uintptr_t)heap->buddy_nodes +
                         old_pages * sizeof(struct buddy_node) + 4095UL) &
                        ~4095UL;
  uintptr_t n_new_end = ((uintptr_t)heap->buddy_nodes +
                         new_pages * sizeof(struct buddy_node) + 4095UL) &
                        ~4095UL;
  if (n_new_end > n_old_end) {
    if (mprotect((void *)n_old_end, n_new_end - n_old_end,
                 PROT_READ | PROT_WRITE) != 0) {
      perror("[mem_heap] mprotect buddy_nodes meta");
      return false;
    }
  }

  /* Unlock metadata pages for `dirty_bits` and `snap_dirty_bits` arrays */
  size_t old_dirty_words = (old_pages + 63) / 64;
  size_t new_dirty_words = (new_pages + 63) / 64;

  /* Active dirty_bits range */
  uintptr_t d_old_end = ((uintptr_t)heap->dirty_bits +
                         old_dirty_words * sizeof(uint64_t) + 4095UL) &
                        ~4095UL;
  uintptr_t d_new_end = ((uintptr_t)heap->dirty_bits +
                         new_dirty_words * sizeof(uint64_t) + 4095UL) &
                        ~4095UL;
  if (d_new_end > d_old_end) {
    if (mprotect((void *)d_old_end, d_new_end - d_old_end,
                 PROT_READ | PROT_WRITE) != 0) {
      perror("[mem_heap] mprotect dirty_bits meta");
      return false;
    }
  }

  /* Snapshot snap_dirty_bits range */
  uintptr_t sd_old_end = ((uintptr_t)heap->snap_dirty_bits +
                          old_dirty_words * sizeof(uint64_t) + 4095UL) &
                         ~4095UL;
  uintptr_t sd_new_end = ((uintptr_t)heap->snap_dirty_bits +
                          new_dirty_words * sizeof(uint64_t) + 4095UL) &
                         ~4095UL;
  if (sd_new_end > sd_old_end) {
    if (mprotect((void *)sd_old_end, sd_new_end - sd_old_end,
                 PROT_READ | PROT_WRITE) != 0) {
      perror("[mem_heap] mprotect snap_dirty_bits meta");
      return false;
    }
  }

  /* Active slot_bits range */
  size_t old_slot_bytes = (size_t)old_pages * SNAP_MODIFIED_SIZE;
  size_t new_slot_bytes = (size_t)new_pages * SNAP_MODIFIED_SIZE;
  uintptr_t sb_old_end =
      ((uintptr_t)heap->slot_bits + old_slot_bytes + 4095UL) & ~4095UL;
  uintptr_t sb_new_end =
      ((uintptr_t)heap->slot_bits + new_slot_bytes + 4095UL) & ~4095UL;
  if (sb_new_end > sb_old_end) {
    if (mprotect((void *)sb_old_end, sb_new_end - sb_old_end,
                 PROT_READ | PROT_WRITE) != 0) {
      perror("[mem_heap] mprotect slot_bits meta");
      return false;
    }
  }

  /* Frozen snap_slot_bits range */
  uintptr_t ssb_old_end =
      ((uintptr_t)heap->snap_slot_bits + old_slot_bytes + 4095UL) & ~4095UL;
  uintptr_t ssb_new_end =
      ((uintptr_t)heap->snap_slot_bits + new_slot_bytes + 4095UL) & ~4095UL;
  if (ssb_new_end > ssb_old_end) {
    if (mprotect((void *)ssb_old_end, ssb_new_end - ssb_old_end,
                 PROT_READ | PROT_WRITE) != 0) {
      perror("[mem_heap] mprotect snap_slot_bits meta");
      return false;
    }
  }

  /* Zero-initialize new metadata entries */
  if (added_pages > 0) {
    memset(heap->pages + old_pages, 0, added_pages * sizeof(struct page_info));
    memset(heap->buddy_nodes + old_pages, 0,
           added_pages * sizeof(struct buddy_node));
    memset(heap->slot_bits + old_slot_bytes, 0,
           (size_t)added_pages * SNAP_MODIFIED_SIZE);
    memset(heap->snap_slot_bits + old_slot_bytes, 0,
           (size_t)added_pages * SNAP_MODIFIED_SIZE);
  }
  if (new_dirty_words > old_dirty_words) {
    size_t added_words = new_dirty_words - old_dirty_words;
    memset(heap->dirty_bits + old_dirty_words, 0,
           added_words * sizeof(uint64_t));
    memset(heap->snap_dirty_bits + old_dirty_words, 0,
           added_words * sizeof(uint64_t));
  }

  /* meta_committed tracked as total end offset of last metadata member */
  heap->meta_committed =
      ssb_new_end - (uintptr_t)thread_meta_base(heap->thread_id);

  /* Extend buddy allocator's view and add new pages to free list */
  heap->buddy.nr_pages = new_pages;
  if (added_pages > 0) {
    buddy_span_free(&heap->buddy, old_pages, added_pages);
  }

  heap->data_committed = aligned;
  heap->nr_pages_committed = new_pages;

  return true;
}

/* ── Eviction and limits ─────────────────────────────────────────────── */

void mem_evict_register(uint32_t thread_id, mem_evict_fn evict_fn,
                        void *evict_ctx) {
  if (thread_id >= g_mem_arena.n_threads || !g_thread_heaps) {
    fprintf(stderr, "[mem_heap] Invalid thread_id=%u in mem_evict_register\n",
            thread_id);
    return;
  }

  g_thread_heaps[thread_id].evict_fn = evict_fn;
  g_thread_heaps[thread_id].evict_ctx = evict_ctx;
}

void mem_limit_set(uint32_t thread_id, size_t limit_bytes) {
  if (thread_id >= g_mem_arena.n_threads || !g_thread_heaps) {
    fprintf(stderr, "[mem_heap] Invalid thread_id=%u in mem_limit_set\n",
            thread_id);
    return;
  }

  g_thread_heaps[thread_id].limit_bytes = limit_bytes;
}

size_t mem_usage_get(uint32_t thread_id) {
  if (thread_id >= g_mem_arena.n_threads || !g_thread_heaps) {
    return 0;
  }

  return g_thread_heaps[thread_id].data_committed;
}
