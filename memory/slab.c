#include "slab.h"

#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

/*
 * Optimized struct slab Allocator Implementation
 * - THP support (2MB slabs)
 * - Memory advise hints
 * - MRU allocation strategy (move free slab to head)
 * - O(1) slab_obj_free via aligned mmap + back-pointer (Fix #2)
 */

/*
 * Internal: Create a new slab with aligned mmap.
 *
 * We allocate SLAB_MEMORY_SIZE * 2 bytes to guarantee we can find a
 * SLAB_MEMORY_SIZE-aligned region within. The first sizeof(struct slab*) bytes
 * of the aligned region store a back-pointer to the struct slab struct.
 * Usable slots start after the back-pointer (aligned to slot_size).
 */
static struct slab *slab_obj_create(size_t slot_size) {
  /* Allocate 2x size to guarantee alignment */
  size_t raw_len = SLAB_MEMORY_SIZE * 2;
  void *raw = mmap(NULL, raw_len, PROT_READ | PROT_WRITE,
                   MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (raw == MAP_FAILED) {
    return NULL;
  }

  /* Align to SLAB_MEMORY_SIZE boundary */
  uintptr_t raw_addr = (uintptr_t)raw;
  uintptr_t aligned_addr = (raw_addr + SLAB_MEMORY_SIZE - 1) & SLAB_ALIGN_MASK;
  void *memory = (void *)aligned_addr;

  /* Hint for transparent huge pages */
#ifdef MADV_HUGEPAGE
  madvise(memory, SLAB_MEMORY_SIZE, MADV_HUGEPAGE);
#endif
#ifdef MADV_DONTFORK
  madvise(memory, SLAB_MEMORY_SIZE, MADV_DONTFORK);
#endif

  /* The struct slab struct itself lives at the exact start of the aligned
   * region */
  struct slab *slab = (struct slab *)memory;

  /* Compute usable area: skip the struct slab struct, align to slot_size */
  size_t header_size = sizeof(struct slab);
  size_t rem = header_size % slot_size;
  if (rem != 0)
    header_size += (slot_size - rem);

  uint8_t *slots_start = (uint8_t *)memory + header_size;
  size_t usable_size = SLAB_MEMORY_SIZE - header_size;

  slab->magic = SLAB_MAGIC;
  slab->next = NULL;
  slab->memory = memory;
  slab->raw_mmap = raw;
  slab->raw_mmap_len = raw_len;
  slab->slot_size = slot_size;
  slab->num_slots = usable_size / slot_size;
  slab->used_slots = 0;

  /* Initialize freelist - chain all slots */
  slab->freelist = NULL;
  for (size_t i = 0; i < slab->num_slots; i++) {
    struct slab_free_node *node =
        (struct slab_free_node *)(slots_start + i * slot_size);
    node->next = slab->freelist;
    slab->freelist = node;
  }

  return slab;
}

/*
 * Internal: Destroy a single slab
 */
static void slab_obj_free_single(struct slab *slab) {
  if (slab && slab->raw_mmap) {
    munmap(slab->raw_mmap, slab->raw_mmap_len);
  }
}

/*
 * Initialize slab allocator
 */
struct slab_allocator *slab_alloc_init(void) {
  struct slab_allocator *allocator =
      mmap(NULL, sizeof(struct slab_allocator), PROT_READ | PROT_WRITE,
           MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (allocator == MAP_FAILED)
    return NULL;

  memset(allocator->slabs, 0, sizeof(allocator->slabs));
  allocator->total_allocated = 0;
  allocator->total_slabs = 0;

  return allocator;
}

/*
 * Destroy slab allocator
 */
void slab_alloc_destroy(struct slab_allocator *allocator) {
  if (!allocator)
    return;

  for (int i = 0; i < SLAB_NUM_CLASSES; i++) {
    struct slab *slab = allocator->slabs[i];
    while (slab) {
      struct slab *next = slab->next;
      slab_obj_free_single(slab);
      slab = next;
    }
  }

  munmap(allocator, sizeof(struct slab_allocator));
}

/*
 * Allocate memory from slab
 */
void *slab_obj_alloc(struct slab_allocator *allocator, size_t size) {
  if (!allocator || size == 0)
    return NULL;

  int class_idx = slab_class_get(size);
  if (class_idx < 0) {
    /* Size too large for slab, fall back to direct mmap with 2MB alignment.
     * This ensures we can identify it via pointer masking, just like slabs.
     */
    size_t raw_len = SLAB_MEMORY_SIZE * 2;
    void *raw = mmap(NULL, raw_len, PROT_READ | PROT_WRITE,
                     MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (raw == MAP_FAILED)
      return NULL;

    /* Align to SLAB_MEMORY_SIZE boundary */
    uintptr_t raw_addr = (uintptr_t)raw;
    uintptr_t aligned_addr =
        (raw_addr + SLAB_MEMORY_SIZE - 1) & SLAB_ALIGN_MASK;
    void *memory = (void *)aligned_addr;

    /* Store header at aligned boundary */
    struct slab_direct *hdr = (struct slab_direct *)memory;
    hdr->magic = SLAB_DIRECT_MAGIC;
    hdr->raw_mmap = raw;
    hdr->raw_mmap_len = raw_len;
    hdr->alloc_size = size;

    /* Return pointer after header, aligned to 64 bytes */
    size_t header_size = sizeof(struct slab_direct);
    size_t rem = header_size % 64;
    if (rem != 0)
      header_size += (64 - rem);

    return (void *)((uint8_t *)memory + header_size);
  }

  size_t slot_size = SLAB_SIZE_CLASSES[class_idx];

  /* Find a slab with free slots */
  struct slab *slab = allocator->slabs[class_idx];
  struct slab *prev = NULL;

  while (slab && !slab->freelist) {
    prev = slab;
    slab = slab->next;
  }

  /* Create new slab if needed */
  if (!slab) {
    slab = slab_obj_create(slot_size);
    if (!slab)
      return NULL;

    slab->next = allocator->slabs[class_idx];
    allocator->slabs[class_idx] = slab;
    allocator->total_slabs++;
  } else if (prev) {
    /* Move slab with free slots to front (MRU optimization) */
    prev->next = slab->next;
    slab->next = allocator->slabs[class_idx];
    allocator->slabs[class_idx] = slab;
  }

  /* Pop from freelist — O(1) */
  struct slab_free_node *node = slab->freelist;
  slab->freelist = node->next;
  slab->used_slots++;
  allocator->total_allocated += slot_size;

  return (void *)node;
}

/*
 * Free memory back to slab — O(1) via aligned pointer arithmetic.
 * No linked-list scan needed. Size is determined from the allocation itself.
 */
void slab_obj_free(struct slab_allocator *allocator, void *ptr) {
  if (!allocator || !ptr)
    return;

  /* Mask to 2MB boundary to get header */
  void *base = (void *)((uintptr_t)ptr & SLAB_ALIGN_MASK);
  uint32_t *magic = (uint32_t *)base;

  /* Check magic number to determine allocation type */
  if (*magic == SLAB_MAGIC) {
    /* Slab allocation */
    struct slab *slab = (struct slab *)base;
    int class_idx = slab_class_get(slab->slot_size);
    if (class_idx >= 0 && SLAB_SIZE_CLASSES[class_idx] == slab->slot_size) {
      size_t slot_size = slab->slot_size;

      /* Push to freelist — O(1) */
      struct slab_free_node *node = (struct slab_free_node *)ptr;
      node->next = slab->freelist;
      slab->freelist = node;
      slab->used_slots--;
      allocator->total_allocated -= slot_size;
      return;
    }
  } else if (*magic == SLAB_DIRECT_MAGIC) {
    /* Direct mmap allocation */
    struct slab_direct *hdr = (struct slab_direct *)base;
    munmap(hdr->raw_mmap, hdr->raw_mmap_len);
    return;
  }

  /* Invalid magic - not a slab allocation */
}

/*
 * Get statistics
 */
void slab_stats_get(const struct slab_allocator *allocator,
                    struct slab_stats *stats) {
  if (!allocator || !stats)
    return;

  memset(stats, 0, sizeof(struct slab_stats));
  stats->total_slabs = allocator->total_slabs;
  stats->total_allocated = allocator->total_allocated;

  for (int i = 0; i < SLAB_NUM_CLASSES; i++) {
    struct slab *slab = allocator->slabs[i];
    size_t class_slabs = 0;
    size_t class_used = 0;
    size_t class_total = 0;

    while (slab) {
      class_slabs++;
      class_used += slab->used_slots;
      class_total += slab->num_slots;
      slab = slab->next;
    }

    stats->slabs_per_class[i] = class_slabs;
    stats->utilization_per_class[i] =
        class_total > 0 ? (double)class_used / class_total : 0.0;
  }
}

/*
 * Print statistics
 */
void slab_stats_print(const struct slab_allocator *allocator) {
  if (!allocator)
    return;

  struct slab_stats stats;
  slab_stats_get(allocator, &stats);

  printf("\n=== struct slab Allocator Statistics ===\n");
  printf("Total slabs:     %zu\n", stats.total_slabs);
  printf("Total allocated: %zu bytes (%.2f MB)\n", stats.total_allocated,
         stats.total_allocated / (1024.0 * 1024.0));
  printf("\nPer-class breakdown:\n");
  printf("%-12s %-10s %-15s\n", "Size Class", "Slabs", "Utilization");
  printf("─────────────────────────────────────────\n");

  for (int i = 0; i < SLAB_NUM_CLASSES; i++) {
    if (stats.slabs_per_class[i] > 0) {
      printf("%-12zu %-10zu %.1f%%\n", SLAB_SIZE_CLASSES[i],
             stats.slabs_per_class[i], stats.utilization_per_class[i] * 100.0);
    }
  }
}

/*
 * slab_obj_calloc: Allocate and zero-initialize memory
 */
void *slab_obj_calloc(struct slab_allocator *allocator, size_t nmemb,
                      size_t size) {
  size_t total_size = nmemb * size;
  void *ptr = slab_obj_alloc(allocator, total_size);
  if (ptr) {
    memset(ptr, 0, total_size);
  }
  return ptr;
}

/*
 * slab_obj_realloc: Reallocate memory block
 * Size is determined from the allocation itself
 */
void *slab_obj_realloc(struct slab_allocator *allocator, void *ptr,
                       size_t new_size) {
  if (!ptr) {
    return slab_obj_alloc(allocator, new_size);
  }

  if (new_size == 0) {
    slab_obj_free(allocator, ptr);
    return NULL;
  }

  /* Determine old size from the allocation */
  size_t old_size;
  void *base = (void *)((uintptr_t)ptr & SLAB_ALIGN_MASK);
  uint32_t *magic = (uint32_t *)base;

  /* Check magic to determine allocation type and size */
  if (*magic == SLAB_MAGIC) {
    /* Slab allocation */
    struct slab *slab = (struct slab *)base;
    int class_idx = slab_class_get(slab->slot_size);
    if (class_idx >= 0 && SLAB_SIZE_CLASSES[class_idx] == slab->slot_size) {
      old_size = slab->slot_size;
    } else {
      /* Invalid - should not happen */
      return NULL;
    }
  } else if (*magic == SLAB_DIRECT_MAGIC) {
    /* Direct mmap allocation */
    struct slab_direct *hdr = (struct slab_direct *)base;
    old_size = hdr->alloc_size;
  } else {
    /* Invalid allocation */
    return NULL;
  }

  /* If new size fits in same size class, reuse the block (unless it's a direct
   * mmap) */
  int old_class = slab_class_get(old_size);
  int new_class = slab_class_get(new_size);
  if (old_class == new_class && old_class >= 0) {
    return ptr;
  }

  /* Allocate new block, copy data, free old block */
  void *new_ptr = slab_obj_alloc(allocator, new_size);
  if (new_ptr) {
    size_t copy_size = (old_size < new_size) ? old_size : new_size;
    memcpy(new_ptr, ptr, copy_size);
    slab_obj_free(allocator, ptr);
  }
  return new_ptr;
}
