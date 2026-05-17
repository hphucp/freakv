#include "slab.h"

#include <stdio.h>
#include <string.h>
#include <sys/mman.h>

#define SLAB_BUDDY_NIL UINT32_MAX
#define SLAB_PAGE_FLAG_FREE 0u
#define SLAB_PAGE_FLAG_SLAB 1u
#define SLAB_PAGE_FLAG_LARGE 2u
#define SLAB_LARGE_MAGIC 0x434C4152u /* 'CLAR' */

struct slab_large_hdr {
  uint32_t magic;
  uint32_t pages;
  size_t requested;
};

struct slab_chunk {
  struct slab_page *pages;
  struct slab_buddy_node *nodes;
  struct slab_span *spans;
  struct slab_buddy buddy;
  uint32_t chunk_idx;
  uint32_t base_page;
};

static inline uint32_t ceil_log2_u32(uint32_t n) {
  if (n <= 1)
    return 0;
  return 32u - (uint32_t)__builtin_clz(n - 1);
}

static inline uint32_t ptr_page_idx(const struct slab_allocator *a,
                                    const void *ptr) {
  return (uint32_t)(((const char *)ptr - (const char *)a->arena) /
                    SLAB_PAGE_SIZE);
}

static inline struct slab_chunk *page_chunk(const struct slab_allocator *a,
                                            uint32_t page, uint32_t *local) {
  uint32_t chunk_idx = page / SLAB_CHUNK_PAGES;
  if (chunk_idx >= SLAB_MAX_CHUNKS || !a->chunks[chunk_idx])
    return NULL;
  if (local)
    *local = page & (SLAB_CHUNK_PAGES - 1);
  return a->chunks[chunk_idx];
}

static inline void list_add(struct slab_span **head, struct slab_span *span) {
  span->prev = NULL;
  span->next = *head;
  if (*head)
    (*head)->prev = span;
  *head = span;
}

static inline void list_remove(struct slab_span **head,
                               struct slab_span *span) {
  if (span->prev)
    span->prev->next = span->next;
  else
    *head = span->next;
  if (span->next)
    span->next->prev = span->prev;
  span->next = NULL;
  span->prev = NULL;
}

static void buddy_list_remove(struct slab_buddy *b, uint32_t idx) {
  uint8_t order = b->pages[idx].buddy_order;
  uint32_t prev = b->nodes[idx].prev;
  uint32_t next = b->nodes[idx].next;

  if (prev != SLAB_BUDDY_NIL)
    b->nodes[prev].next = next;
  else
    b->free_heads[order] = next;

  if (next != SLAB_BUDDY_NIL)
    b->nodes[next].prev = prev;

  b->nodes[idx].next = SLAB_BUDDY_NIL;
  b->nodes[idx].prev = SLAB_BUDDY_NIL;
}

static void buddy_list_push(struct slab_buddy *b, uint32_t idx,
                            uint8_t order) {
  b->pages[idx].buddy_order = order;
  b->nodes[idx].prev = SLAB_BUDDY_NIL;
  b->nodes[idx].next = b->free_heads[order];
  if (b->free_heads[order] != SLAB_BUDDY_NIL)
    b->nodes[b->free_heads[order]].prev = idx;
  b->free_heads[order] = idx;
}

static void buddy_init(struct slab_buddy *b, struct slab_page *pages,
                       struct slab_buddy_node *nodes, uint32_t nr_pages) {
  b->pages = pages;
  b->nodes = nodes;
  b->nr_pages = nr_pages;
  for (uint32_t i = 0; i < SLAB_BUDDY_MAX_ORDER; i++)
    b->free_heads[i] = SLAB_BUDDY_NIL;
  for (uint32_t i = 0; i < nr_pages; i++) {
    pages[i].span = NULL;
    pages[i].span_pages = 0;
    pages[i].flags = SLAB_PAGE_FLAG_FREE;
    pages[i].buddy_order = 0;
    nodes[i].next = SLAB_BUDDY_NIL;
    nodes[i].prev = SLAB_BUDDY_NIL;
  }
}

static void buddy_free_block(struct slab_buddy *b, uint32_t idx,
                             uint32_t order) {
  uint32_t n = 1u << order;
  for (uint32_t i = 0; i < n; i++) {
    b->pages[idx + i].span = NULL;
    b->pages[idx + i].span_pages = 0;
    b->pages[idx + i].flags = SLAB_PAGE_FLAG_FREE;
    b->pages[idx + i].buddy_order = 0;
  }

  uint32_t cur = idx;
  uint32_t cur_order = order;
  while (cur_order + 1 < SLAB_BUDDY_MAX_ORDER) {
    uint32_t buddy = cur ^ (1u << cur_order);
    if (buddy >= b->nr_pages || b->pages[buddy].flags != SLAB_PAGE_FLAG_FREE ||
        b->pages[buddy].buddy_order != cur_order)
      break;
    buddy_list_remove(b, buddy);
    if (buddy < cur)
      cur = buddy;
    cur_order++;
  }

  buddy_list_push(b, cur, (uint8_t)cur_order);
}

static uint32_t buddy_alloc(struct slab_buddy *b, uint32_t n_pages) {
  if (n_pages == 0)
    return SLAB_BUDDY_NIL;
  uint32_t order = ceil_log2_u32(n_pages);
  if (order >= SLAB_BUDDY_MAX_ORDER)
    return SLAB_BUDDY_NIL;

  uint32_t found = order;
  while (found < SLAB_BUDDY_MAX_ORDER &&
         b->free_heads[found] == SLAB_BUDDY_NIL)
    found++;
  if (found >= SLAB_BUDDY_MAX_ORDER)
    return SLAB_BUDDY_NIL;

  uint32_t idx = b->free_heads[found];
  buddy_list_remove(b, idx);
  while (found > order) {
    found--;
    uint32_t half = idx + (1u << found);
    buddy_list_push(b, half, (uint8_t)found);
  }

  uint32_t alloc_pages = 1u << order;
  for (uint32_t i = 0; i < alloc_pages; i++) {
    b->pages[idx + i].flags = SLAB_PAGE_FLAG_SLAB;
    b->pages[idx + i].span_pages = alloc_pages;
    b->pages[idx + i].buddy_order = 0;
  }
  return idx;
}

static uint32_t pages_for_span(uint32_t slot_size) {
  uint32_t target_slots;
  if (slot_size <= 512)
    target_slots = 64;
  else if (slot_size <= 2048)
    target_slots = 32;
  else if (slot_size <= 8192)
    target_slots = 16;
  else if (slot_size <= 16384)
    target_slots = 4;
  else
    target_slots = 1;

  uint64_t bytes = (uint64_t)slot_size * target_slots;
  uint32_t pages = (uint32_t)((bytes + SLAB_PAGE_SIZE - 1) / SLAB_PAGE_SIZE);
  if (pages == 0)
    pages = 1;
  return pages;
}

static uint32_t max_empty_for_size(uint32_t slot_size) {
  if (slot_size <= 1024)
    return 8;
  if (slot_size <= 8192)
    return 2;
  if (slot_size <= 16384)
    return 1;
  return 0;
}

static void pool_init(struct slab_pool *p, uint32_t slot_size) {
  memset(p, 0, sizeof(*p));
  p->slot_size = slot_size;
  p->pages_per_span = pages_for_span(slot_size);
  p->slots_per_span = (p->pages_per_span * SLAB_PAGE_SIZE) / slot_size;
  p->max_empty_spans = max_empty_for_size(slot_size);
}

static struct slab_chunk *chunk_create(struct slab_allocator *a) {
  if (a->nr_chunks >= SLAB_MAX_CHUNKS)
    return NULL;

  uint32_t chunk_idx = a->nr_chunks;
  struct slab_chunk *c =
      mmap(NULL, sizeof(*c), PROT_READ | PROT_WRITE,
           MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (c == MAP_FAILED)
    return NULL;
  memset(c, 0, sizeof(*c));

  size_t pages_len = (size_t)SLAB_CHUNK_PAGES * sizeof(*c->pages);
  size_t nodes_len = (size_t)SLAB_CHUNK_PAGES * sizeof(*c->nodes);
  size_t spans_len = (size_t)SLAB_CHUNK_PAGES * sizeof(*c->spans);

  c->pages = mmap(NULL, pages_len, PROT_READ | PROT_WRITE,
                  MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  c->nodes = mmap(NULL, nodes_len, PROT_READ | PROT_WRITE,
                  MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  c->spans = mmap(NULL, spans_len, PROT_READ | PROT_WRITE,
                  MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (c->pages == MAP_FAILED || c->nodes == MAP_FAILED ||
      c->spans == MAP_FAILED) {
    if (c->spans && c->spans != MAP_FAILED)
      munmap(c->spans, spans_len);
    if (c->nodes && c->nodes != MAP_FAILED)
      munmap(c->nodes, nodes_len);
    if (c->pages && c->pages != MAP_FAILED)
      munmap(c->pages, pages_len);
    munmap(c, sizeof(*c));
    return NULL;
  }

  c->chunk_idx = chunk_idx;
  c->base_page = chunk_idx * SLAB_CHUNK_PAGES;
  buddy_init(&c->buddy, c->pages, c->nodes, SLAB_CHUNK_PAGES);
  buddy_list_push(&c->buddy, 0, SLAB_BUDDY_MAX_ORDER - 1);

  a->chunks[chunk_idx] = c;
  a->nr_chunks++;
  return c;
}

static void chunk_destroy(struct slab_chunk *c) {
  if (!c)
    return;
  size_t pages_len = (size_t)SLAB_CHUNK_PAGES * sizeof(*c->pages);
  size_t nodes_len = (size_t)SLAB_CHUNK_PAGES * sizeof(*c->nodes);
  size_t spans_len = (size_t)SLAB_CHUNK_PAGES * sizeof(*c->spans);
  munmap(c->pages, pages_len);
  munmap(c->nodes, nodes_len);
  munmap(c->spans, spans_len);
  munmap(c, sizeof(*c));
}

static uint32_t chunk_alloc_pages(struct slab_allocator *a,
                                  struct slab_chunk **out_chunk,
                                  uint32_t n_pages) {
  for (uint32_t i = 0; i < a->nr_chunks; i++) {
    struct slab_chunk *c = a->chunks[i];
    uint32_t local = buddy_alloc(&c->buddy, n_pages);
    if (local != SLAB_BUDDY_NIL) {
      *out_chunk = c;
      return local;
    }
  }

  struct slab_chunk *c = chunk_create(a);
  if (!c)
    return SLAB_BUDDY_NIL;
  uint32_t local = buddy_alloc(&c->buddy, n_pages);
  if (local == SLAB_BUDDY_NIL)
    return SLAB_BUDDY_NIL;
  *out_chunk = c;
  return local;
}

static void span_init(struct slab_allocator *a, struct slab_pool *p,
                      struct slab_chunk *chunk, struct slab_span *span,
                      uint32_t head_page,
                      uint32_t span_pages) {
  memset(span, 0, sizeof(*span));
  span->pool = p;
  span->chunk = chunk;
  span->head_page = head_page;
  span->span_pages = span_pages;

  char *base = (char *)a->arena + (size_t)head_page * SLAB_PAGE_SIZE;
  for (uint32_t i = 0; i < p->slots_per_span; i++) {
    struct slab_free_node *node =
        (struct slab_free_node *)(base + (size_t)i * p->slot_size);
    node->next = span->freelist;
    span->freelist = node;
  }

  uint32_t local_head = head_page - chunk->base_page;
  for (uint32_t i = 0; i < span_pages; i++) {
    chunk->pages[local_head + i].span = span;
    chunk->pages[local_head + i].span_pages = span_pages;
    chunk->pages[local_head + i].flags = SLAB_PAGE_FLAG_SLAB;
  }
}

static void span_release(struct slab_allocator *a, struct slab_pool *p,
                         struct slab_span *span) {
  p->active_spans--;
  a->total_slabs--;
  buddy_free_block(&span->chunk->buddy, span->head_page - span->chunk->base_page,
                   ceil_log2_u32(span->span_pages));
  memset(span, 0, sizeof(*span));
}

static void *pool_alloc(struct slab_allocator *a, struct slab_pool *p) {
  struct slab_span *span = p->partial;
  if (!span && p->empty) {
    span = p->empty;
    list_remove(&p->empty, span);
    p->empty_count--;
    list_add(&p->partial, span);
  }

  if (!span) {
    struct slab_chunk *chunk = NULL;
    uint32_t local = chunk_alloc_pages(a, &chunk, p->pages_per_span);
    if (local == SLAB_BUDDY_NIL)
      return NULL;
    uint32_t actual_pages = chunk->pages[local].span_pages;
    uint32_t head = chunk->base_page + local;
    span = &chunk->spans[local];
    span_init(a, p, chunk, span, head, actual_pages);
    list_add(&p->partial, span);
    p->active_spans++;
    a->total_slabs++;
  }

  struct slab_free_node *node = span->freelist;
  span->freelist = node->next;
  span->used_slots++;
  p->allocated_bytes += p->slot_size;
  a->total_allocated += p->slot_size;

  if (!span->freelist)
    list_remove(&p->partial, span);

  return node;
}

static void pool_free(struct slab_allocator *a, struct slab_span *span,
                      void *ptr) {
  struct slab_pool *p = span->pool;
  bool was_full = (span->freelist == NULL);
  struct slab_free_node *node = (struct slab_free_node *)ptr;
  node->next = span->freelist;
  span->freelist = node;
  span->used_slots--;
  p->allocated_bytes -= p->slot_size;
  a->total_allocated -= p->slot_size;

  if (span->used_slots == 0) {
    if (!was_full)
      list_remove(&p->partial, span);
    if (p->empty_count < p->max_empty_spans) {
      list_add(&p->empty, span);
      p->empty_count++;
    } else {
      span_release(a, p, span);
    }
  } else if (was_full) {
    list_add(&p->partial, span);
  }
}

struct slab_allocator *slab_alloc_init(void) {
  struct slab_allocator *a =
      mmap(NULL, sizeof(*a), PROT_READ | PROT_WRITE,
           MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (a == MAP_FAILED)
    return NULL;
  memset(a, 0, sizeof(*a));

  a->arena_size = SLAB_ARENA_SIZE;
  a->arena = mmap(NULL, a->arena_size, PROT_READ | PROT_WRITE,
                  MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
  if (a->arena == MAP_FAILED)
    goto fail_allocator;

  for (uint32_t i = 0; i < SLAB_NUM_CLASSES; i++)
    pool_init(&a->pools[i], (uint32_t)SLAB_SIZE_CLASSES[i]);

  return a;

fail_allocator:
  munmap(a, sizeof(*a));
  return NULL;
}

void slab_alloc_destroy(struct slab_allocator *a) {
  if (!a)
    return;
  for (uint32_t i = 0; i < a->nr_chunks; i++)
    chunk_destroy(a->chunks[i]);
  if (a->arena)
    munmap(a->arena, a->arena_size);
  munmap(a, sizeof(*a));
}

void *slab_obj_alloc(struct slab_allocator *a, size_t size) {
  if (!a || size == 0)
    return NULL;

  int class_idx = slab_class_get(size);
  if (class_idx >= 0)
    return pool_alloc(a, &a->pools[class_idx]);

  size_t total = sizeof(struct slab_large_hdr) + size;
  if (total < size)
    return NULL;
  uint32_t requested_pages =
      (uint32_t)((total + SLAB_PAGE_SIZE - 1) / SLAB_PAGE_SIZE);
  struct slab_chunk *chunk = NULL;
  uint32_t local = chunk_alloc_pages(a, &chunk, requested_pages);
  if (local == SLAB_BUDDY_NIL)
    return NULL;
  uint32_t pages = chunk->pages[local].span_pages;
  uint32_t head = chunk->base_page + local;

  for (uint32_t i = 0; i < pages; i++) {
    chunk->pages[local + i].span = NULL;
    chunk->pages[local + i].span_pages = pages;
    chunk->pages[local + i].flags = SLAB_PAGE_FLAG_LARGE;
  }

  struct slab_large_hdr *hdr =
      (struct slab_large_hdr *)((char *)a->arena +
                                (size_t)head * SLAB_PAGE_SIZE);
  hdr->magic = SLAB_LARGE_MAGIC;
  hdr->pages = pages;
  hdr->requested = size;
  a->large_allocated += (size_t)pages * SLAB_PAGE_SIZE;
  a->total_allocated += size;
  return (void *)(hdr + 1);
}

void *slab_obj_calloc(struct slab_allocator *a, size_t nmemb, size_t size) {
  size_t total = nmemb * size;
  if (nmemb > 0 && total / nmemb != size)
    return NULL;
  void *ptr = slab_obj_alloc(a, total);
  if (ptr)
    memset(ptr, 0, total);
  return ptr;
}

static size_t allocation_size(struct slab_allocator *a, void *ptr) {
  if (!a || !ptr || ptr < a->arena ||
      ptr >= (void *)((char *)a->arena + a->arena_size))
    return 0;
  uint32_t page = ptr_page_idx(a, ptr);
  uint32_t local = 0;
  struct slab_chunk *chunk = page_chunk(a, page, &local);
  if (!chunk)
    return 0;
  struct slab_page *pi = &chunk->pages[local];
  if (pi->flags == SLAB_PAGE_FLAG_SLAB && pi->span && pi->span->pool)
    return pi->span->pool->slot_size;
  if (pi->flags == SLAB_PAGE_FLAG_LARGE) {
    uint32_t head_local = local & ~(pi->span_pages - 1);
    struct slab_large_hdr *hdr =
        (struct slab_large_hdr *)((char *)a->arena +
                                  (size_t)(chunk->base_page + head_local) *
                                      SLAB_PAGE_SIZE);
    if (hdr->magic == SLAB_LARGE_MAGIC)
      return hdr->requested;
  }
  return 0;
}

void *slab_obj_realloc(struct slab_allocator *a, void *ptr, size_t new_size) {
  if (!ptr)
    return slab_obj_alloc(a, new_size);
  if (new_size == 0) {
    slab_obj_free(a, ptr);
    return NULL;
  }

  size_t old_size = allocation_size(a, ptr);
  if (old_size == 0)
    return NULL;
  if (new_size <= old_size)
    return ptr;

  void *new_ptr = slab_obj_alloc(a, new_size);
  if (!new_ptr)
    return NULL;
  memcpy(new_ptr, ptr, old_size);
  slab_obj_free(a, ptr);
  return new_ptr;
}

void slab_obj_free(struct slab_allocator *a, void *ptr) {
  if (!a || !ptr || ptr < a->arena ||
      ptr >= (void *)((char *)a->arena + a->arena_size))
    return;

  uint32_t page = ptr_page_idx(a, ptr);
  uint32_t local = 0;
  struct slab_chunk *chunk = page_chunk(a, page, &local);
  if (!chunk)
    return;

  struct slab_page *pi = &chunk->pages[local];
  if (pi->flags == SLAB_PAGE_FLAG_SLAB && pi->span) {
    pool_free(a, pi->span, ptr);
    return;
  }

  if (pi->flags == SLAB_PAGE_FLAG_LARGE) {
    uint32_t head_local = local & ~(pi->span_pages - 1);
    struct slab_large_hdr *hdr =
        (struct slab_large_hdr *)((char *)a->arena +
                                  (size_t)(chunk->base_page + head_local) *
                                      SLAB_PAGE_SIZE);
    if (hdr->magic != SLAB_LARGE_MAGIC)
      return;
    a->large_allocated -= (size_t)hdr->pages * SLAB_PAGE_SIZE;
    a->total_allocated -= hdr->requested;
    buddy_free_block(&chunk->buddy, head_local, ceil_log2_u32(hdr->pages));
  }
}

void slab_stats_get(const struct slab_allocator *a, struct slab_stats *stats) {
  if (!a || !stats)
    return;
  memset(stats, 0, sizeof(*stats));
  stats->total_slabs = a->total_slabs;
  stats->total_allocated = a->total_allocated;
  for (uint32_t i = 0; i < SLAB_NUM_CLASSES; i++) {
    const struct slab_pool *p = &a->pools[i];
    stats->slabs_per_class[i] = p->active_spans;
    uint64_t capacity = (uint64_t)p->active_spans * p->slots_per_span;
    uint64_t used = p->slot_size ? p->allocated_bytes / p->slot_size : 0;
    stats->utilization_per_class[i] =
        capacity ? (double)used / (double)capacity : 0.0;
  }
}

void slab_stats_print(const struct slab_allocator *a) {
  if (!a)
    return;
  struct slab_stats stats;
  slab_stats_get(a, &stats);
  printf("\n=== Control Allocator Statistics ===\n");
  printf("Total spans:     %zu\n", stats.total_slabs);
  printf("Total allocated: %zu bytes (%.2f MB)\n", stats.total_allocated,
         stats.total_allocated / (1024.0 * 1024.0));
  printf("Large allocated: %zu bytes (%.2f MB)\n", a->large_allocated,
         a->large_allocated / (1024.0 * 1024.0));
  printf("\nPer-class breakdown:\n");
  printf("%-12s %-10s %-15s\n", "Size Class", "Spans", "Utilization");
  for (uint32_t i = 0; i < SLAB_NUM_CLASSES; i++) {
    if (stats.slabs_per_class[i] > 0) {
      printf("%-12zu %-10zu %.1f%%\n", SLAB_SIZE_CLASSES[i],
             stats.slabs_per_class[i],
             stats.utilization_per_class[i] * 100.0);
    }
  }
}

bool slab_alloc_prewarm(struct slab_allocator *a, const size_t *sizes,
                        size_t count) {
  if (!a || !sizes)
    return false;
  for (size_t i = 0; i < count; i++) {
    int class_idx = slab_class_get(sizes[i]);
    if (class_idx < 0)
      continue;
    struct slab_pool *p = &a->pools[class_idx];
    void *ptr = pool_alloc(a, p);
    if (!ptr)
      return false;
    slab_obj_free(a, ptr);
  }
  return true;
}
