#ifndef SLAB_OPT_H
#define SLAB_OPT_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/*
 * Control-plane allocator.
 *
 * Public API intentionally keeps the old slab_obj_* names. Internally this is
 * a size-class slab allocator backed by a 4KB-page buddy allocator. Size
 * classes cover allocations up to 64KB; larger allocations take exact page
 * spans directly from the buddy.
 */

#define SLAB_NUM_CLASSES 26
#define SLAB_SMALL_MAX 65536u
#define SLAB_PAGE_SIZE 4096u
#define SLAB_ARENA_SIZE (256ULL * 1024ULL * 1024ULL * 1024ULL)
#define SLAB_CHUNK_SIZE (256ULL * 1024ULL * 1024ULL)
#define SLAB_CHUNK_PAGES (SLAB_CHUNK_SIZE / SLAB_PAGE_SIZE)
#define SLAB_MAX_CHUNKS (SLAB_ARENA_SIZE / SLAB_CHUNK_SIZE)
#define SLAB_BUDDY_MAX_ORDER 17 /* per chunk: 2^16 pages = 256MB */

static const size_t SLAB_SIZE_CLASSES[SLAB_NUM_CLASSES] = {
    16,    32,    48,    64,    80,    112,   144,   192,   256,
    320,   384,   512,   768,   1024,  1536,  2048,  3072,  4096,
    5120,  8192,  12288, 16384, 24576, 32768, 49152, 65536,
};

struct slab_stats {
  size_t total_slabs;
  size_t total_allocated;
  size_t slabs_per_class[SLAB_NUM_CLASSES];
  double utilization_per_class[SLAB_NUM_CLASSES];
};

struct slab_free_node {
  struct slab_free_node *next;
};

struct slab_pool;
struct slab_chunk;

struct slab_span {
  struct slab_pool *pool;
  struct slab_chunk *chunk;
  struct slab_free_node *freelist;
  struct slab_span *next;
  struct slab_span *prev;
  uint32_t head_page;
  uint32_t span_pages;
  uint32_t used_slots;
};

struct slab_page {
  struct slab_span *span;
  uint32_t span_pages;
  uint8_t flags;
  uint8_t buddy_order;
  uint16_t reserved;
};

struct slab_buddy_node {
  uint32_t next;
  uint32_t prev;
};

struct slab_buddy {
  struct slab_page *pages;
  struct slab_buddy_node *nodes;
  uint32_t nr_pages;
  uint32_t free_heads[SLAB_BUDDY_MAX_ORDER];
};

struct slab_pool {
  uint32_t slot_size;
  uint32_t pages_per_span;
  uint32_t slots_per_span;
  uint32_t max_empty_spans;
  uint32_t empty_count;
  struct slab_span *partial;
  struct slab_span *empty;
  size_t allocated_bytes;
  size_t active_spans;
};

struct slab_allocator {
  void *arena;
  size_t arena_size;
  struct slab_chunk *chunks[SLAB_MAX_CHUNKS];
  uint32_t nr_chunks;
  struct slab_pool pools[SLAB_NUM_CLASSES];
  size_t total_allocated;
  size_t total_slabs;
  size_t large_allocated;
};

struct slab_allocator *slab_alloc_init(void);
void slab_alloc_destroy(struct slab_allocator *allocator);
void *slab_obj_alloc(struct slab_allocator *allocator, size_t size);
void *slab_obj_calloc(struct slab_allocator *allocator, size_t nmemb,
                      size_t size);
void *slab_obj_realloc(struct slab_allocator *allocator, void *ptr,
                       size_t new_size);
void slab_obj_free(struct slab_allocator *allocator, void *ptr);
void slab_stats_get(const struct slab_allocator *allocator,
                    struct slab_stats *stats);
void slab_stats_print(const struct slab_allocator *allocator);
bool slab_alloc_prewarm(struct slab_allocator *allocator, const size_t *sizes,
                        size_t count);

static inline int slab_class_get(size_t size) {
  for (int i = 0; i < SLAB_NUM_CLASSES; i++) {
    if (size <= SLAB_SIZE_CLASSES[i])
      return i;
  }
  return -1;
}

#endif /* SLAB_OPT_H */
