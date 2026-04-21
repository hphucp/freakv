#ifndef SLAB_OPT_H
#define SLAB_OPT_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/*
 * Optimized struct slab Allocator
 */

#define SLAB_NUM_CLASSES 8
#define SLAB_MEMORY_SIZE (2 * 1024 * 1024) /* 2MB for THP */
#define SLAB_ALIGN_MASK (~((uintptr_t)(SLAB_MEMORY_SIZE - 1)))

/* Size classes */
static const size_t SLAB_SIZE_CLASSES[SLAB_NUM_CLASSES] = {
    16, 32, 64, 128, 256, 512, 1024, 2048};

/* Statistics structure */
struct slab_stats {
  size_t total_slabs;
  size_t total_allocated;
  size_t slabs_per_class[SLAB_NUM_CLASSES];
  double utilization_per_class[SLAB_NUM_CLASSES];
};

/* Free slot node */
struct slab_free_node {
  struct slab_free_node *next;
};

/*
 * Single slab.
 */
#define SLAB_MAGIC 0x534C4142        /* 'SLAB' in hex */
#define SLAB_DIRECT_MAGIC 0x444D4150 /* 'DMAP' - for direct mmap allocations   \
                                      */

struct slab {
  uint32_t magic; /* Magic number for validation */
  struct slab *next;
  void *memory;        /* mmap region (aligned to SLAB_MEMORY_SIZE) */
  void *raw_mmap;      /* Original mmap address (for munmap) */
  size_t raw_mmap_len; /* Original mmap length (for munmap) */
  size_t slot_size;
  size_t num_slots;
  size_t used_slots;
  struct slab_free_node *freelist;
};

/* Direct mmap allocation header (for sizes > max slab class) */
struct slab_direct {
  uint32_t magic;      /* SLAB_DIRECT_MAGIC */
  void *raw_mmap;      /* Original mmap address (for munmap) */
  size_t raw_mmap_len; /* Original mmap length (for munmap) */
  size_t alloc_size;   /* Requested allocation size */
};

/* struct slab allocator */
struct slab_allocator {
  struct slab *slabs[SLAB_NUM_CLASSES];
  size_t total_allocated;
  size_t total_slabs;
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

static inline int slab_class_get(size_t size) {
  for (int i = 0; i < SLAB_NUM_CLASSES; i++) {
    if (size <= SLAB_SIZE_CLASSES[i]) {
      return i;
    }
  }
  return -1;
}

static inline struct slab *slab_ptr_get(void *ptr) {
  uintptr_t base = (uintptr_t)ptr & SLAB_ALIGN_MASK;
  return (struct slab *)base;
}

#endif /* SLAB_OPT_H */
