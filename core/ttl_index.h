#ifndef TTL_INDEX_H
#define TTL_INDEX_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "object.h"

#define TTL_HEAP_INIT_CAP 256

struct ttl_node {
  uint64_t expire_ms;
  struct kv_obj *obj;
};

struct ttl_index {
  struct ttl_node *heap;
  uint32_t size;
  uint32_t cap;
};

static inline void ttl_node_swap(struct ttl_index *idx, uint32_t a,
                                 uint32_t b) {
  struct ttl_node tmp = idx->heap[a];
  idx->heap[a] = idx->heap[b];
  idx->heap[b] = tmp;
  idx->heap[a].obj->heap_idx = a;
  idx->heap[b].obj->heap_idx = b;
}

static inline void ttl_node_sift_up(struct ttl_index *idx, uint32_t i) {
  while (i > 0) {
    uint32_t p = (i - 1) / 2;
    if (idx->heap[p].expire_ms <= idx->heap[i].expire_ms)
      break;
    ttl_node_swap(idx, p, i);
    i = p;
  }
}

static inline void ttl_node_sift_down(struct ttl_index *idx, uint32_t i) {
  for (;;) {
    uint32_t s = i, l = 2 * i + 1, r = 2 * i + 2;
    if (l < idx->size && idx->heap[l].expire_ms < idx->heap[s].expire_ms)
      s = l;
    if (r < idx->size && idx->heap[r].expire_ms < idx->heap[s].expire_ms)
      s = r;
    if (s == i)
      break;
    ttl_node_swap(idx, i, s);
    i = s;
  }
}

static inline void ttl_node_fix(struct ttl_index *idx, uint32_t i) {
  if (i > 0 && idx->heap[i].expire_ms < idx->heap[(i - 1) / 2].expire_ms)
    ttl_node_sift_up(idx, i);
  else
    ttl_node_sift_down(idx, i);
}

static inline void ttl_index_init(struct ttl_index *idx) {
  memset(idx, 0, sizeof(*idx));
}
static inline void ttl_index_destroy(struct ttl_index *idx) {
  free(idx->heap);
  memset(idx, 0, sizeof(*idx));
}

static inline bool ttl_index_node_push(struct ttl_index *idx,
                                       struct kv_obj *obj, uint64_t expire_ms) {
  if (idx->size >= idx->cap) {
    uint32_t nc = idx->cap ? idx->cap * 2 : TTL_HEAP_INIT_CAP;
    struct ttl_node *na =
        (struct ttl_node *)realloc(idx->heap, nc * sizeof(struct ttl_node));
    if (!na)
      return false;
    idx->heap = na;
    idx->cap = nc;
  }
  uint32_t pos = idx->size++;
  idx->heap[pos].expire_ms = expire_ms;
  idx->heap[pos].obj = obj;
  obj->heap_idx = pos;
  ttl_node_sift_up(idx, pos);
  return true;
}

static inline void ttl_index_node_update(struct ttl_index *idx,
                                         struct kv_obj *obj,
                                         uint64_t expire_ms) {
  uint32_t pos = obj->heap_idx;
  if (pos == HEAP_IDX_NONE)
    return;
  idx->heap[pos].expire_ms = expire_ms;
  ttl_node_fix(idx, pos);
}

static inline void ttl_index_node_remove(struct ttl_index *idx,
                                         struct kv_obj *obj) {
  uint32_t pos = obj->heap_idx;
  if (pos == HEAP_IDX_NONE)
    return;
  idx->size--;
  if (pos < idx->size) {
    idx->heap[pos] = idx->heap[idx->size];
    idx->heap[pos].obj->heap_idx = pos;
    ttl_node_fix(idx, pos);
  }
  obj->heap_idx = HEAP_IDX_NONE;
}

static inline uint64_t ttl_index_node_peek(const struct ttl_index *idx) {
  return idx->size > 0 ? idx->heap[0].expire_ms : UINT64_MAX;
}
static inline uint32_t ttl_index_node_count(const struct ttl_index *idx) {
  return idx->size;
}

static inline struct kv_obj *ttl_index_node_pop(struct ttl_index *idx,
                                                uint64_t *out_expire_ms) {
  if (idx->size == 0)
    return NULL;
  if (out_expire_ms)
    *out_expire_ms = idx->heap[0].expire_ms;
  struct kv_obj *obj = idx->heap[0].obj;
  obj->heap_idx = HEAP_IDX_NONE;
  idx->size--;
  if (idx->size > 0) {
    idx->heap[0] = idx->heap[idx->size];
    idx->heap[0].obj->heap_idx = 0;
    ttl_node_sift_down(idx, 0);
  }
  return obj;
}

#endif /* TTL_INDEX_H */
