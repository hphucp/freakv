#ifndef SPSC_H
#define SPSC_H

/*
 * Lock-free SPSC Ring Buffer
 */

#include "message.h"
#include <stdalign.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

#define SPSC_DEFAULT_CAPACITY 4096

struct spsc_queue {
  alignas(64) _Atomic size_t head;
  alignas(64) _Atomic size_t tail;
  alignas(64) size_t capacity; /* Must be power of 2 */
  size_t mask;                 /* capacity - 1 */
  struct spsc_message *slots;  /* Allocated array of message slots */
};

static inline bool spsc_queue_init(struct spsc_queue *q, size_t capacity) {
  if (!q || capacity == 0 || (capacity & (capacity - 1)) != 0)
    return false;
  q->slots =
      (struct spsc_message *)calloc(capacity, sizeof(struct spsc_message));
  if (!q->slots)
    return false;
  atomic_store_explicit(&q->head, 0, memory_order_relaxed);
  atomic_store_explicit(&q->tail, 0, memory_order_relaxed);
  q->capacity = capacity;
  q->mask = capacity - 1;
  return true;
}

static inline void spsc_queue_destroy(struct spsc_queue *q) {
  if (q && q->slots) {
    free(q->slots);
    q->slots = NULL;
  }
}

static inline bool spsc_queue_push(struct spsc_queue *q,
                                   const struct spsc_message *msg) {
  size_t head = atomic_load_explicit(&q->head, memory_order_relaxed);
  size_t tail = atomic_load_explicit(&q->tail, memory_order_acquire);
  if (head - tail >= q->capacity)
    return false;
  q->slots[head & q->mask] = *msg;
  atomic_store_explicit(&q->head, head + 1, memory_order_release);
  return true;
}

static inline bool spsc_queue_pop(struct spsc_queue *q,
                                  struct spsc_message *out) {
  size_t tail = atomic_load_explicit(&q->tail, memory_order_relaxed);
  size_t head = atomic_load_explicit(&q->head, memory_order_acquire);
  if (tail == head)
    return false;
  *out = q->slots[tail & q->mask];
  atomic_store_explicit(&q->tail, tail + 1, memory_order_release);
  return true;
}

static inline bool spsc_queue_empty(struct spsc_queue *q) {
  return atomic_load_explicit(&q->tail, memory_order_relaxed) ==
         atomic_load_explicit(&q->head, memory_order_acquire);
}

#endif /* SPSC_H */
