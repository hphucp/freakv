#define _GNU_SOURCE
#include "mem_linear.h"
#include "mem_arena.h"

#include <stdio.h>
#include <string.h>
#include <sys/mman.h>

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

  /* Commit pages */
  void *commit_ptr = arena->base + arena->committed;
  void *ret = mmap(commit_ptr, grow_bytes, PROT_READ | PROT_WRITE,
                   MAP_ANONYMOUS | MAP_PRIVATE | MAP_FIXED, -1, 0);
  if (ret == MAP_FAILED) {
    perror("[mem_linear] mmap commit");
    return false;
  }

  arena->committed = aligned;

  return true;
}

void *linear_arena_base(struct linear_arena *arena) { return arena->base; }

void linear_arena_reset(struct linear_arena *arena) { arena->used = 0; }

void linear_arena_destroy(struct linear_arena *arena) {
  if (arena->committed > 0) {
    if (madvise(arena->base, arena->committed, MADV_DONTNEED) < 0) {
      perror("[mem_linear] madvise MADV_DONTNEED");
    }
  }
  memset(arena, 0, sizeof(*arena));
}
