#ifndef MEM_API_H
#define MEM_API_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* ── Public stdlib-compatible API ────────────────────────────────────── */

/**
 * Allocate memory of given size.
 *
 * @param size Number of bytes to allocate
 * @return Pointer to allocated memory, or NULL on failure
 *
 * Note: No allocator handle needed - thread ID is decoded from TLS.
 */
void *mem_malloc(size_t size);

/**
 * Allocate zero-initialized memory for an array.
 *
 * @param nmemb Number of elements
 * @param size Size of each element
 * @return Pointer to allocated memory, or NULL on failure
 */
void *mem_calloc(size_t nmemb, size_t size);

/**
 * Resize previously allocated memory.
 *
 * @param ptr Pointer to existing allocation (or NULL)
 * @param new_size New size in bytes
 * @return Pointer to resized memory, or NULL on failure
 *
 * Note: If ptr is NULL, behaves like mem_malloc.
 *       If new_size is 0, behaves like mem_free.
 */
void *mem_realloc(void *ptr, size_t new_size);

/**
 * Free previously allocated memory.
 *
 * @param ptr Pointer to memory (or NULL)
 *
 * Note: Thread ID and allocation metadata are decoded from pointer address.
 *       No allocator handle or size parameter needed.
 */
void mem_free(void *ptr);

/* ── Thread-local storage for current thread ID ──────────────────────── */

/**
 * Set current thread ID for TLS.
 * Must be called once per thread before using mem_malloc/free.
 *
 * @param thread_id Thread identifier (0..n_threads-1)
 */
void mem_set_thread_id(uint32_t thread_id);

/**
 * Get current thread ID from TLS.
 *
 * @return Thread identifier
 */
uint32_t mem_get_thread_id(void);

/* ── Dirty Page Tracking (for snapshotting) ──────────────────────────── */

/**
 * Free memory to snap_freelist (deferred path).
 * Called ONLY by the snapshot background thread when it is the last
 * holder of a kv_obj (refcount reached 0 on snap thread).
 * Memory is recycled later by main thread during pool_snap_merge.
 */
void mem_snap_free(void *ptr);

#endif /* MEM_API_H */
