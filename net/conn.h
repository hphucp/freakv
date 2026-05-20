#ifndef CONN_H
#define CONN_H

#include "../memory/slab.h"
#include "message.h"
#include "resp.h"

#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <sys/uio.h>

#define CONN_MAX_PAYLOAD (1024u * 1024u)
#define PIPELINE_INIT_CAP 128 /* must be power-of-2 */

/* ── Connection state machine (simplified) ──────────────────────────── */

enum net_state {
  CONN_ALIVE = 0, /* Normal bidirectional I/O */
  CONN_CLOSING =
      1, /* Client EOF (RDHUP/read 0). FD open, flushing wbuf/pipeline. */
  CONN_DEAD =
      2, /* Hard error (ERR/HUP). FD closed, waiting for pipeline drain. */
};

enum conn_pending_reason {
  CONN_PENDING_NONE = 0,
  CONN_PENDING_MSET_BP = 1,
};

/* ── struct net_buf ────────────────────────────────────────────────── */
struct net_buf {
  int refcount;
  size_t cap;
  uint8_t data[];
};

static inline struct net_buf *net_rbuf_alloc(struct slab_allocator *allocator,
                                            size_t cap) {
  struct net_buf *nb =
      (struct net_buf *)slab_obj_alloc(allocator, sizeof(struct net_buf) + cap);
  if (!nb)
    return NULL;
  nb->refcount = 1;
  nb->cap = cap;
  return nb;
}

static inline int net_rbuf_ref(struct net_buf *nb) {
  if (nb)
    return ++nb->refcount;
  return 0;
}

static inline int net_rbuf_unref(struct net_buf *nb) {
  if (nb)                          
    return --nb->refcount;
  return 0;
}

static inline bool net_rbuf_try_free(struct slab_allocator *allocator, struct net_buf *nb) {
  int ref = net_rbuf_unref(nb);
  if (nb && !ref)
    slab_obj_free(allocator, nb);
  return !ref;
}

/* ── struct net_pipeline_slot ────────────────────────────────────────── */

enum net_pipeline_state {
  PSLOT_EMPTY = 0,   /* unused                                    */
  PSLOT_LOCAL = 1,   /* handled locally, reply in reply_data      */
  PSLOT_PENDING = 2, /* sent to remote shard, awaiting reply      */
  PSLOT_DONE = 3,    /* remote reply received                     */
};

struct net_pipeline_slot {
  enum net_pipeline_state state;

  /* Single reply */
  uint8_t *reply_data; /* RESP reply (malloc'd if not from kv_obj_ptr) */
  uint32_t reply_len;
  void *kv_obj_ptr;   /* Ref'd object (for zero-copy) */
  uint8_t reply_type; /* enum msg_reply_type */
  int64_t reply_int;  /* Payload integer / counts     */

  /* Multi-key support (MGET, MSET) */
  bool is_multi;
  uint32_t multi_parts;
  uint32_t multi_replied;
  uint8_t **multi_replies;
  uint32_t *multi_reply_lens;
  void **multi_kv_objs;
  uint8_t *multi_owners;

  uint16_t parent_cmd_id; /* For MGET/MSET/DEL differentiation */
  uint8_t shard_owner;    /* owner shard id (for single) */
  struct net_buf *req_nb; /* input buffer refcount */
  struct resp_cmd cmd;    /* parsed command */
};

/* ── struct net_proxy_pipeline (circular queue) ─────────────────────── */

struct net_proxy_pipeline {
  struct net_pipeline_slot *slots;
  uint32_t cap; /* always power-of-2               */

  uint32_t in;  /* next sequence number to enqueue  */
  uint32_t out; /* next sequence number to flush    */
};

/* ── struct net_conn ────────────────────────────────────────────────── */

struct net_conn {
  int fd;
  enum net_state state;

  bool is_shared;      /* true if connection came from shared port */
  uint64_t generation; /* for stale msg detection */

  struct net_buf *rbuf_nb; /* owner of input buffer */
  size_t rbuf_len;         /* bytes read into buffer (source of truth) */

  /* Write buffer */
  uint8_t *wbuf;
  size_t wbuf_cap;
  size_t wbuf_len;
  size_t wbuf_sent;

  /* RESP parser */
  struct resp_parser resp;

  /* Proxy pipeline (circular queue) */
  struct net_proxy_pipeline proxy;

  /* Intrusive list linkage (belongs to either dirty_list or dead_list) */
  struct net_conn *next;
  struct net_conn *prev;
  uint8_t list_type; /* 0: None, 1: Dirty, 2: Dead */

  /* Connection-level scheduling pause.  When non-NONE, the reactor must not
   * parse further requests from this connection until the owner clears it. */
  enum conn_pending_reason pending_reason;
  bool pending_queued;
  struct net_conn *pending_next;
  uint32_t pending_pipeline_idx;
  struct net_buf *pending_req_nb;
  uint32_t pending_argc;
  char **pending_argv;
  size_t *pending_arglen;

  /* Allocator for buffers */
  struct slab_allocator *allocator;
};

/* ── Circular queue pipeline helpers ─────────────────────────────────── */

static inline bool net_pipeline_resize(struct net_proxy_pipeline *pp,
                                       struct slab_allocator *allocator) {
  uint32_t new_cap = pp->cap * 2;
  struct net_pipeline_slot *ns = (struct net_pipeline_slot *)slab_obj_calloc(
      allocator, new_cap, sizeof(*ns));
  if (!ns)
    return false;

  for (uint32_t seq = pp->out; seq != pp->in; seq++) {
    ns[seq & (new_cap - 1)] = pp->slots[seq & (pp->cap - 1)];
  }
  slab_obj_free(allocator, pp->slots);
  pp->slots = ns;
  pp->cap = new_cap;
  return true;
}

static inline uint32_t net_pipeline_enqueue(struct net_proxy_pipeline *pp,
                                            struct slab_allocator *allocator) {
  if (pp->in - pp->out == pp->cap) {
    if (!net_pipeline_resize(pp, allocator))
      return UINT32_MAX; /* OOM */
  }
  uint32_t seq = pp->in++;
  struct net_pipeline_slot *s = &pp->slots[seq & (pp->cap - 1)];
  memset(s, 0, sizeof(*s)); // TODO: check if memset is redundant here.
  return seq; /* return sequence number — used as pipeline_idx */
}

static inline void net_pipeline_init(struct net_proxy_pipeline *pp,
                                     struct slab_allocator *allocator) {
  pp->slots = (struct net_pipeline_slot *)slab_obj_calloc(
      allocator, PIPELINE_INIT_CAP, sizeof(struct net_pipeline_slot));
  pp->cap = (pp->slots != NULL) ? PIPELINE_INIT_CAP : 0;
  pp->in = 0;
  pp->out = 0;
}

#endif /* CONN_H */
