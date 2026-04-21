#ifndef MEM_LINEAR_H
#define MEM_LINEAR_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/**
 * linear_arena — Grow-only bump-pointer allocator
 *
 * Used for hash table buckets and overflow buckets.
 * Lives in the separate arena_base VA region.
 * Never frees individual objects - only bulk reset.
 */
struct linear_arena {
  uint32_t thread_id;
  char *base;       /* Start of VA slice in arena_base */
  size_t reserved;  /* Total VA reserved */
  size_t committed; /* Physical memory committed */
  size_t used;      /* Bump pointer offset */
};

/**
 * Initialize linear arena manually with explicit base/reserved.
 * Used when manually partitioning thread arena space.
 *
 * @param arena Arena structure
 * @param thread_id Thread identifier
 * @param base Base address
 * @param reserved Reserved size
 * @return true on success, false on failure
 */
bool linear_arena_init_manual(struct linear_arena *arena, uint32_t thread_id,
                              void *base, size_t reserved);

/**
 * Initialize linear arena for a thread (uses full thread arena space).
 *
 * @param arena Arena structure
 * @param thread_id Thread identifier
 * @return true on success, false on failure
 */
bool linear_arena_init(struct linear_arena *arena, uint32_t thread_id);

/**
 * Grow arena to new total size.
 * Commits additional physical pages as needed.
 *
 * @param arena Arena structure
 * @param new_total_bytes New total size in bytes
 * @return true on success, false on failure
 */
bool linear_arena_grow(struct linear_arena *arena, size_t new_total_bytes);

/**
 * Get base pointer of arena.
 *
 * @param arena Arena structure
 * @return Base pointer
 */
void *linear_arena_base(struct linear_arena *arena);

/**
 * Reset arena to initial state (used = 0).
 * Does not decommit memory.
 *
 * @param arena Arena structure
 */
void linear_arena_reset(struct linear_arena *arena);

/**
 * Destroy arena and decommit memory.
 *
 * @param arena Arena structure
 */
void linear_arena_destroy(struct linear_arena *arena);

#endif /* MEM_LINEAR_H */
