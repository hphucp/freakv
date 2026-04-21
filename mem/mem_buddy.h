#ifndef MEM_BUDDY_H
#define MEM_BUDDY_H

/*
 * mem_buddy.h — Standalone buddy allocator (no memory/arena.h dependency)
 */

#include "mem_arena.h"
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#define INVALID 0xFFFFFFFF
#define BUDDY_NIL 0xFFFFFFFF
#define BUDDY_MAX_ORDER 21

#define SNAP_MODIFIED_SIZE 16

struct free_obj {
  struct free_obj *next;
};

struct small_pool;

struct span_info {
  struct small_pool *pool;             /*  8 bytes */
  struct free_obj *freelist;           /*  8 bytes */
  struct free_obj *snap_freelist;      /*  8 bytes */
  struct free_obj *snap_freelist_tail; /* 8 bytes */
  struct span_info *next;              /*  8 bytes */
  struct span_info *prev;              /*  8 bytes */
  uint32_t head_lpage;                 /*  4 bytes */
  uint32_t span_size;                  /*  4 bytes */
  uint16_t nr_alloc;                   /*  2 bytes */
  uint16_t nr_snap_alloc;              /*  2 bytes */
  uint32_t next_to_merge;
};

struct page_info {
  struct span_info *span;  /*  8 bytes */
  uint32_t span_size;      /*  4 bytes */
  uint16_t offset_in_span; /*  2 bytes */
  bool free;               /*  1 byte  */
  uint8_t buddy_order;     /*  1 byte  */
}; /* total: 16 bytes */

/* ── struct buddy_node (8 bytes, separate array)
 * ─────────────────────────────── */
struct buddy_node {
  uint32_t next;
  uint32_t prev;
};

/* ── struct buddy_allocator
 * ──────────────────────────────────────────────────── */
struct buddy_allocator {
  struct page_info *pages;
  struct buddy_node *nodes;
  char *data_base;
  uint32_t nr_pages;
  uint32_t free_heads[BUDDY_MAX_ORDER];
};

/* ── Slot-level modified bitmap helpers ────────────────────────────── */

static inline void modified_bit_set(uint8_t *slot_bits, uint32_t lpage,
                                    uint32_t local_slot) {
  if (local_slot < SNAP_MODIFIED_SIZE * 8) {
    uint8_t *base = slot_bits + (size_t)lpage * SNAP_MODIFIED_SIZE;
    base[local_slot >> 3] |= (1u << (local_slot & 7));
  }
}

static inline void modified_bit_clear(uint8_t *slot_bits, uint32_t lpage,
                                      uint32_t local_slot) {
  if (local_slot < SNAP_MODIFIED_SIZE * 8) {
    uint8_t *base = slot_bits + (size_t)lpage * SNAP_MODIFIED_SIZE;
    base[local_slot >> 3] &= ~(1u << (local_slot & 7));
  }
}

static inline bool modified_bit_test(const uint8_t *slot_bits, uint32_t lpage,
                                     uint32_t local_slot) {
  if (local_slot >= SNAP_MODIFIED_SIZE * 8)
    return false;
  const uint8_t *base = slot_bits + (size_t)lpage * SNAP_MODIFIED_SIZE;
  return (base[local_slot >> 3] >> (local_slot & 7)) & 1;
}

static inline bool modified_any(const uint8_t *slot_bits, uint32_t lpage) {
  const uint64_t *w =
      (const uint64_t *)(slot_bits + (size_t)lpage * SNAP_MODIFIED_SIZE);
  return (w[0] | w[1]) != 0;
}

/* ── Page-slot helpers ──────────────────────────────────────────────── */

static inline uint32_t page_first_slot_offset(uint32_t lpage,
                                              const struct page_info *pages,
                                              uint32_t obj_size) {
  uint16_t offset_in_span = pages[lpage].offset_in_span;
  uint32_t page_start = (uint32_t)offset_in_span * (uint32_t)LPAGE_SIZE;
  uint32_t first_idx = (page_start + obj_size - 1) / obj_size;
  return first_idx * obj_size - page_start;
}

static inline uint32_t ptr_to_page_slot(const void *ptr, const void *data_base,
                                        const struct page_info *pages,
                                        uint32_t obj_size) {
  uint32_t lpage =
      (uint32_t)(((const char *)ptr - (const char *)data_base) >> LPAGE_SHIFT);
  uint32_t byte_in_page =
      (uint32_t)(((const char *)ptr - (const char *)data_base)) &
      (LPAGE_SIZE - 1);
  uint32_t first_off = page_first_slot_offset(lpage, pages, obj_size);
  if (byte_in_page < first_off)
    return INVALID;
  return (byte_in_page - first_off) / obj_size;
}

/* ── Helpers ──────────────────────────────────────────────────────────── */

static inline uint32_t buddy_math_ceil_log2(uint32_t n) {
  if (n <= 1)
    return 0;
  return 32 - (uint32_t)__builtin_clz(n - 1);
}

static inline void buddy_list_node_remove(struct buddy_allocator *b,
                                          uint32_t idx) {
  uint8_t ord = b->pages[idx].buddy_order;
  uint32_t prev = b->nodes[idx].prev;
  uint32_t next = b->nodes[idx].next;

  if (prev != BUDDY_NIL)
    b->nodes[prev].next = next;
  else
    b->free_heads[ord] = next;

  if (next != BUDDY_NIL)
    b->nodes[next].prev = prev;

  b->nodes[idx].next = BUDDY_NIL;
  b->nodes[idx].prev = BUDDY_NIL;
}

static inline void buddy_list_node_push(struct buddy_allocator *b, uint32_t idx,
                                        uint8_t ord) {
  b->pages[idx].buddy_order = ord;
  b->nodes[idx].prev = BUDDY_NIL;
  b->nodes[idx].next = b->free_heads[ord];

  if (b->free_heads[ord] != BUDDY_NIL)
    b->nodes[b->free_heads[ord]].prev = idx;

  b->free_heads[ord] = idx;
}

/* ── API ─────────────────────────────────────────────────────────────── */

static inline void buddy_alloc_init(struct buddy_allocator *b,
                                    struct page_info *pages,
                                    struct buddy_node *nodes, char *base,
                                    uint32_t nr_pages) {
  b->pages = pages;
  b->nodes = nodes;
  b->data_base = base;
  b->nr_pages = nr_pages;
  for (int i = 0; i < BUDDY_MAX_ORDER; i++)
    b->free_heads[i] = BUDDY_NIL;
}

static inline void buddy_block_free(struct buddy_allocator *b, uint32_t idx,
                                    uint32_t order) {
  uint32_t n = 1u << order;
  for (uint32_t i = 0; i < n; i++) {
    b->pages[idx + i].span = NULL;
    b->pages[idx + i].span_size = 0;
    b->pages[idx + i].offset_in_span = 0;
    b->pages[idx + i].free = true;
    b->pages[idx + i].buddy_order = 0;
  }
  b->nodes[idx].next = BUDDY_NIL;
  b->nodes[idx].prev = BUDDY_NIL;
  buddy_list_node_push(b, idx, (uint8_t)order);
}

static inline void buddy_span_free(struct buddy_allocator *b, uint32_t idx,
                                   uint32_t n) {
  if (n > 0 && (n & (n - 1)) == 0 && (idx & (n - 1)) == 0) {
    uint32_t order = (n == 1) ? 0 : (31 - (uint32_t)__builtin_clz(n));
    buddy_block_free(b, idx, order);
    return;
  }
  for (uint32_t i = 0; i < n; i++) {
    b->pages[idx + i].span = NULL;
    b->pages[idx + i].span_size = 0;
    b->pages[idx + i].offset_in_span = 0;
    b->pages[idx + i].free = true;
    b->pages[idx + i].buddy_order = 0;
  }
  uint32_t pos = idx, end = idx + n;
  while (pos < end) {
    uint32_t remaining = end - pos;
    uint32_t ord_size = 31 - (uint32_t)__builtin_clz(remaining);
    uint32_t ord_align = (pos == 0) ? ord_size : (uint32_t)__builtin_ctz(pos);
    uint32_t order = ord_size < ord_align ? ord_size : ord_align;
    if (order >= BUDDY_MAX_ORDER)
      order = BUDDY_MAX_ORDER - 1;
    b->nodes[pos].next = BUDDY_NIL;
    b->nodes[pos].prev = BUDDY_NIL;
    buddy_list_node_push(b, pos, (uint8_t)order);
    pos += (1u << order);
  }
}

static inline bool buddy_span_coalesce(struct buddy_allocator *b,
                                       uint32_t target_order) {
  for (uint32_t ord = 0; ord < target_order && ord + 1 < BUDDY_MAX_ORDER;
       ord++) {
    uint32_t idx = b->free_heads[ord];
    while (idx != BUDDY_NIL) {
      uint32_t next_idx = b->nodes[idx].next;
      uint32_t buddy_idx = idx ^ (1u << ord);
      if (buddy_idx < b->nr_pages && b->pages[buddy_idx].free &&
          b->pages[buddy_idx].buddy_order == ord) {
        uint32_t merged = idx & ~((1u << (ord + 1)) - 1);
        buddy_list_node_remove(b, idx);
        buddy_list_node_remove(b, buddy_idx);
        b->nodes[merged].next = BUDDY_NIL;
        b->nodes[merged].prev = BUDDY_NIL;
        buddy_list_node_push(b, merged, (uint8_t)(ord + 1));
        idx = b->free_heads[ord];
        continue;
      }
      idx = next_idx;
    }
    if (b->free_heads[target_order] != BUDDY_NIL)
      return true;
  }
  return b->free_heads[target_order] != BUDDY_NIL;
}

static inline bool buddy_span_check(struct buddy_allocator *b, uint32_t n) {
  if (n == 0)
    return false;
  uint32_t target_order = buddy_math_ceil_log2(n);
  if (target_order >= BUDDY_MAX_ORDER)
    return false;
  for (uint32_t ord = target_order; ord < BUDDY_MAX_ORDER; ord++) {
    if (b->free_heads[ord] != BUDDY_NIL)
      return true;
  }
  return buddy_span_coalesce(b, target_order);
}

static inline uint32_t buddy_span_alloc(struct buddy_allocator *b, uint32_t n) {
  if (n == 0)
    return INVALID;
  uint32_t order = buddy_math_ceil_log2(n);
  if (order >= BUDDY_MAX_ORDER)
    return INVALID;
  uint32_t found_order = order;
  while (found_order < BUDDY_MAX_ORDER &&
         b->free_heads[found_order] == BUDDY_NIL)
    found_order++;

  if (found_order >= BUDDY_MAX_ORDER)
    return INVALID;

  uint32_t idx = b->free_heads[found_order];
  buddy_list_node_remove(b, idx);
  b->pages[idx].free = false;
  while (found_order > order) {
    found_order--;
    uint32_t buddy_half = idx + (1u << found_order);
    b->pages[buddy_half].free = true;
    b->nodes[buddy_half].next = BUDDY_NIL;
    b->nodes[buddy_half].prev = BUDDY_NIL;
    buddy_list_node_push(b, buddy_half, (uint8_t)found_order);
  }
  uint32_t alloc_size = 1u << order;
  for (uint32_t i = 0; i < n; i++) {
    b->pages[idx + i].free = false;
    b->pages[idx + i].span_size = n;
    b->pages[idx + i].span = NULL;
    b->pages[idx + i].offset_in_span = (uint16_t)i;
    b->pages[idx + i].buddy_order = 0;
  }
  if (alloc_size > n) {
    buddy_span_free(b, idx + n, alloc_size - n);
  }
  return idx;
}

#endif /* MEM_BUDDY_H */
