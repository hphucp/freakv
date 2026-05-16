#define _GNU_SOURCE
#include "mem_linear.h"
#include "mem_arena.h"
#include "mem_barrier.h"
#include "mem_heap.h"

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>

#ifndef MADV_POPULATE_WRITE
#define MADV_POPULATE_WRITE 23
#endif

bool linear_arena_init_manual(struct linear_arena *arena, uint32_t thread_id,
                              void *base, size_t reserved) {
  if (thread_id >= g_mem_arena.n_threads) {
    fprintf(stderr, "[mem_linear] Invalid thread_id=%u\n", thread_id);
    return false;
  }

  memset(arena, 0, sizeof(*arena));
  arena->thread_id = thread_id;
  arena->base = (char *)base;
  arena->reserved = reserved;
  arena->committed = 0;
  arena->used = 0;

  return true;
}

bool linear_arena_init(struct linear_arena *arena, uint32_t thread_id) {
  return linear_arena_init_manual(arena, thread_id,
                                  (void *)thread_arena_base(thread_id),
                                  g_mem_arena.per_thread_arena_size);
}

bool linear_arena_grow(struct linear_arena *arena, size_t new_total_bytes) {
  if (new_total_bytes <= arena->committed) {
    return true; /* Already committed */
  }

  if (new_total_bytes > arena->reserved) {
    fprintf(stderr, "[mem_linear] Cannot grow beyond reserved VA (%zu > %zu)\n",
            new_total_bytes, arena->reserved);
    return false;
  }

  /* Align to HPAGE boundary */
  size_t aligned = (new_total_bytes + HPAGE_SIZE - 1) & ~(HPAGE_SIZE - 1);
  size_t grow_bytes = aligned - arena->committed;
  struct thread_heap *heap = &g_thread_heaps[arena->thread_id];
  size_t current_usage = heap->data_committed + heap->linear_committed;

  if (heap->limit_bytes > 0 &&
      current_usage + grow_bytes > heap->limit_bytes) {
    fprintf(stderr, "[mem_linear] OOM: limit=%zu current=%zu grow=%zu\n",
            heap->limit_bytes, current_usage, grow_bytes);
    return false;
  }

  size_t allowed = mem_barrier_allowed_bytes(grow_bytes);
  if (allowed < grow_bytes) {
    if (heap->evict_fn) {
      size_t freed = heap->evict_fn(heap->evict_ctx, grow_bytes);
      if (freed < grow_bytes) {
        fprintf(stderr,
                "[mem_linear] Memory pressure too high: requested=%zu "
                "allowed=%zu freed=%zu\n",
                grow_bytes, allowed, freed);
        return false;
      }
      allowed = mem_barrier_allowed_bytes(grow_bytes);
    }

    if (allowed < grow_bytes) {
      fprintf(stderr,
              "[mem_linear] Memory pressure critical: requested=%zu "
              "allowed=%zu\n",
              grow_bytes, allowed);
      return false;
    }
  }

  /* Enable access to the already-reserved PROT_NONE VA range. */
  void *commit_ptr = arena->base + arena->committed;
  if (mprotect(commit_ptr, grow_bytes, PROT_READ | PROT_WRITE) != 0) {
    perror("[mem_linear] mprotect grow");
    return false;
  }

  /* Pre-fault writable pages in-kernel so OOM is reported here instead of at
   * an arbitrary later bucket write. This preserves mappings/data; unlike
   * MAP_FIXED, it never replaces existing pages. */
  if (madvise(commit_ptr, grow_bytes, MADV_POPULATE_WRITE) != 0) {
    int saved_errno = errno;
    if (mprotect(commit_ptr, grow_bytes, PROT_NONE) != 0)
      perror("[mem_linear] mprotect rollback");
    errno = saved_errno;
    perror("[mem_linear] madvise MADV_POPULATE_WRITE");
    return false;
  }

  arena->committed = aligned;
  heap->linear_committed += grow_bytes;

  return true;
}

void *linear_arena_base(struct linear_arena *arena) { return arena->base; }

void linear_arena_reset(struct linear_arena *arena) { arena->used = 0; }

void linear_arena_destroy(struct linear_arena *arena) {
  if (arena->committed > 0) {
    if (madvise(arena->base, arena->committed, MADV_DONTNEED) < 0) {
      perror("[mem_linear] madvise MADV_DONTNEED");
    }
    if (g_thread_heaps && arena->thread_id < g_mem_arena.n_threads) {
      struct thread_heap *heap = &g_thread_heaps[arena->thread_id];
      if (heap->linear_committed >= arena->committed)
        heap->linear_committed -= arena->committed;
      else
        heap->linear_committed = 0;
    }
  }
  memset(arena, 0, sizeof(*arena));
}
