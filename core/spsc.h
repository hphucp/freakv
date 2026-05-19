#ifndef SPSC_H
#define SPSC_H

/*
 * Lock-free Expandable SPSC Queue (Segmented Circular Linked List)
 */

#include "message.h"
#include "../memory/slab.h"
#include <stdalign.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

#define SPSC_SEG_SIZE 4096
#define SPSC_SEG_MASK (SPSC_SEG_SIZE - 1)

struct spsc_segment {
  _Atomic(struct spsc_segment *) next;
  struct spsc_message slots[SPSC_SEG_SIZE];
};

struct spsc_queue {
  /* Cache-line 1: Producer Hot Data */
  alignas(64) _Atomic size_t head;
  struct spsc_segment *p_seg;      /* Producer-only cached pointer */
  struct slab_allocator *allocator; /* Producer's pool for expansion */

  /* Cache-line 2: Consumer Hot Data */
  alignas(64) _Atomic size_t tail;
  struct spsc_segment *c_seg;      /* Consumer-only cached pointer */

  /* Shared state for inter-shard visibility */
  alignas(64) _Atomic(struct spsc_segment *) c_seg_shared; /* Updated by Consumer, read by Producer */

  _Atomic int wake_edge;
};

/*
 * Initialize queue with at least one segment.
 * Note: 'alloc' should be the producer shard's pool for future expansions.
 */
static inline bool spsc_queue_init(struct spsc_queue *q, struct slab_allocator *alloc) {
  if (!q || !alloc) return false;

  struct spsc_segment *seg = (struct spsc_segment *)slab_obj_alloc(alloc, sizeof(struct spsc_segment));
  if (!seg) return false;

  atomic_init(&seg->next, seg); /* Circular link to itself */
  
  atomic_init(&q->head, 0);
  atomic_init(&q->tail, 0);
  q->p_seg = seg;
  q->c_seg = seg;
  q->allocator = alloc;
  atomic_init(&q->c_seg_shared, seg);
  atomic_init(&q->wake_edge, 0);
  return true;
}

/*
 * Destroy all segments in the circular list.
 */
static inline void spsc_queue_destroy(struct spsc_queue *q) {
  if (!q || !q->p_seg || !q->allocator) return;

  struct spsc_segment *start = q->p_seg;
  struct spsc_segment *curr = start;
  do {
    struct spsc_segment *next = atomic_load_explicit(&curr->next, memory_order_relaxed);
    slab_obj_free(q->allocator, curr);
    curr = next;
  } while (curr != start && curr != NULL);
  
  q->p_seg = NULL;
  q->c_seg = NULL;
}

static inline bool spsc_queue_push(struct spsc_queue *q,
                                   const struct spsc_message *msg) {
  size_t h = atomic_load_explicit(&q->head, memory_order_relaxed);
  
  /* Check if we need to move to the next segment */
  if (h > 0 && (h & SPSC_SEG_MASK) == 0) {
    struct spsc_segment *next_seg = atomic_load_explicit(&q->p_seg->next, memory_order_acquire);
    
    /* If the next segment is the one currently occupied by the consumer, we must expand */
    if (next_seg == atomic_load_explicit(&q->c_seg_shared, memory_order_acquire)) {
      struct spsc_segment *new_seg = (struct spsc_segment *)slab_obj_alloc(q->allocator, sizeof(struct spsc_segment));
      if (!new_seg) return false; /* OOM in control plane */
      
      atomic_init(&new_seg->next, next_seg);
      atomic_store_explicit(&q->p_seg->next, new_seg, memory_order_release);
      next_seg = new_seg;
    }
    q->p_seg = next_seg;
  }

  q->p_seg->slots[h & SPSC_SEG_MASK] = *msg;
  atomic_store_explicit(&q->head, h + 1, memory_order_release);
  return true;
}

static inline bool spsc_queue_pop(struct spsc_queue *q,
                                  struct spsc_message *out) {
  size_t t = atomic_load_explicit(&q->tail, memory_order_relaxed);
  size_t h = atomic_load_explicit(&q->head, memory_order_acquire);
  
  if (t == h) return false;

  /* Check if we need to move to the next segment */
  if (t > 0 && (t & SPSC_SEG_MASK) == 0) {
    q->c_seg = atomic_load_explicit(&q->c_seg->next, memory_order_acquire);
    /* Update shared pointer so producer knows this segment is free */
    atomic_store_explicit(&q->c_seg_shared, q->c_seg, memory_order_release);
  }

  *out = q->c_seg->slots[t & SPSC_SEG_MASK];
  atomic_store_explicit(&q->tail, t + 1, memory_order_release);
  return true;
}

static inline bool spsc_queue_empty(struct spsc_queue *q) {
  return atomic_load_explicit(&q->tail, memory_order_relaxed) ==
         atomic_load_explicit(&q->head, memory_order_acquire);
}

static inline bool spsc_queue_try_disarm_wake(struct spsc_queue *q) {
  atomic_store_explicit(&q->wake_edge, 0, memory_order_seq_cst);
  if (spsc_queue_empty(q))
    return true;
  atomic_store_explicit(&q->wake_edge, 1, memory_order_seq_cst);
  return false;
}

#endif /* SPSC_H */
