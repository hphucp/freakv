#include "reactor.h"
#include "conn.h"
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sched.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/timerfd.h>
#include <time.h>
#include <unistd.h>

#include "../memory/slab.h"
#include "message.h"
#include "mset_exec.h"
#include "routing.h"
#include "shard.h"
#include "snapshot.h"

/* timerfd interval: how often reactor_handle_timer() runs TTL expiry
 * (shard_ttl_drain) and the snapshot tick (snap_engine_tick).
 * Independent of EPOLL_TIMEOUT_MS — maintenance does not need sub-ms
 * scheduling. */
#define EXPIRY_INTERVAL_MS 10
#define EXPIRY_BATCH_SIZE 8192
#define EXPIRY_MAX_BATCHES 32
#define EXPIRY_ADAPTIVE_PCT 10

/* ── Helpers ──────────────────────────────────────────────────────────── */

static int net_fd_nodelay_set(int fd) {
  int one = 1;
  return setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
}

static inline uint64_t net_time_ms_get(void) {
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return (uint64_t)ts.tv_sec * 1000ULL + (uint64_t)ts.tv_nsec / 1000000ULL;
}

static inline void net_reactor_mark_dirty(struct reactor *r,
                                          struct net_conn *c);


static struct net_conn *net_conn_alloc(struct reactor *r) {
  struct net_conn *c = (struct net_conn *)slab_obj_alloc(
      r->shard->pool, sizeof(struct net_conn));
  if (!c)
    return NULL;
  memset(c, 0, sizeof(struct net_conn));
  c->fd = -1;
  c->state = CONN_ALIVE;
  c->generation = ++r->conn_generation;
  c->next = c->prev = NULL;
  c->list_type = 0;
  c->allocator = r->shard->pool;

  c->rbuf_nb = net_rbuf_alloc(r->shard->pool, 4096);
  if (!c->rbuf_nb) {
    slab_obj_free(r->shard->pool, c);
    return NULL;
  }
  c->rbuf_len = 0;
  c->resp.rbuf_parsed = 0;
  c->resp.cmd_start = 0;
  c->resp.allocator = r->shard->pool;

  net_pipeline_init(&c->proxy, r->shard->pool);
  return c;
}

/* Forward declaration — net_slot_reset needs reactor for obj release */
static void net_slot_reset(struct reactor *r, struct net_pipeline_slot *s);

static void net_conn_free(struct reactor *r, struct net_conn *c) {
  if (!c)
    return;
  mset_pending_conn_cancel(r->shard, c);
  if (c->rbuf_nb)
    net_rbuf_try_free(r->shard->pool, c->rbuf_nb);
  if (c->wbuf)
    slab_obj_free(r->shard->pool, c->wbuf);
  resp_parser_free(&c->resp);
  /* Cleanup all active slots in the circular queue */
  if (c->proxy.slots) {
    for (uint32_t seq = c->proxy.out; seq != c->proxy.in; seq++) {
      net_slot_reset(r, &c->proxy.slots[seq & (c->proxy.cap - 1)]);
    }
    slab_obj_free(r->shard->pool, c->proxy.slots);
    c->proxy.slots = NULL;
    c->proxy.cap = 0;
  }
  c->proxy.in = c->proxy.out = 0;
  slab_obj_free(r->shard->pool, c);
}

static inline void net_reactor_list_remove(struct reactor *r,
                                           struct net_conn *c) {
  if (c->list_type == 0)
    return;
  if (c->prev)
    c->prev->next = c->next;
  else {
    if (c->list_type == 1)
      r->dirty_head = c->next;
    else
      r->dead_head = c->next;
  }
  if (c->next)
    c->next->prev = c->prev;
  c->next = c->prev = NULL;
  c->list_type = 0;
}

static inline void net_reactor_mark_dirty_reason(struct reactor *r,
                                                 struct net_conn *c,
                                                 const char *reason) {
  (void)reason;
  if (c->list_type == 1) {
    return;
  }
  net_reactor_list_remove(r, c);
  c->next = r->dirty_head;
  if (r->dirty_head)
    r->dirty_head->prev = c;
  r->dirty_head = c;
  c->list_type = 1;
}

static inline void net_reactor_mark_dirty(struct reactor *r,
                                          struct net_conn *c) {
  net_reactor_mark_dirty_reason(r, c, "unknown");
}

static inline void net_reactor_mark_dead(struct reactor *r,
                                         struct net_conn *c) {
  if (c->list_type == 2)
    return;
  mset_pending_conn_cancel(r->shard, c);
  net_reactor_list_remove(r, c);

  /* Close FD immediately if it's still open */
  if (c->fd >= 0) {
    epoll_ctl(r->epoll_fd, EPOLL_CTL_DEL, c->fd, NULL);
    close(c->fd);
    c->fd = -1;
  }
  c->state = CONN_DEAD;

  c->next = r->dead_head;
  if (r->dead_head)
    r->dead_head->prev = c;
  r->dead_head = c;
  c->list_type = 2;
}

static void net_conn_write_done(struct reactor *r, struct net_conn *c) {
  struct epoll_event ev = {.events = EPOLLIN | EPOLLET, .data.ptr = c};
  epoll_ctl(r->epoll_fd, EPOLL_CTL_MOD, c->fd, &ev);
  c->wbuf_sent = 0;
  c->wbuf_len = 0;
}

static void net_conn_try_free(struct reactor *r, struct net_conn *c) {
  if (!c)
    return;
  struct net_proxy_pipeline *pp = &c->proxy;

  /* Can only free if pipeline is fully drained */
  if (pp->in != pp->out)
    return;

  /* If FD is open (CONN_CLOSING), must also wait for wbuf flush */
  if (c->state == CONN_CLOSING && c->wbuf_sent < c->wbuf_len)
    return;

  /* Safe to destroy */
  if (c->fd >= 0) {
    epoll_ctl(r->epoll_fd, EPOLL_CTL_DEL, c->fd, NULL);
    close(c->fd);
    c->fd = -1;
  }
  net_reactor_list_remove(r, c);
  net_conn_free(r, c);
}

static void net_conn_mark_closing(struct reactor *r, struct net_conn *c) {
  if (!c || c->state != CONN_ALIVE)
    return;
  c->state = CONN_CLOSING;
  net_reactor_mark_dirty_reason(r, c, "closing");
}

/* ── Remote object release ───────────────────────────────────────────── */

static void net_remote_obj_release(struct reactor *r, uint8_t owner_shard,
                                   void *kv_obj_ptr, void *reply_buf) {
  if (!kv_obj_ptr && !reply_buf)
    return;
  if (owner_shard == r->shard->id) {
    if (kv_obj_ptr)
      shard_obj_unref(r->shard, (struct kv_obj *)kv_obj_ptr);
    if (reply_buf)
      slab_obj_free(r->shard->pool, reply_buf);
    return;
  }

  struct spsc_message msg = {.op = MSG_PACK_OP(MSG_SYS_RELEASE, 0),
                             .shard_owner = (uint8_t)r->shard->id,
                             .u.release = {
                                 .kv_obj_ptr = kv_obj_ptr,
                                 .reply_buf = reply_buf,
                             }};
  shard_send_msg_deferred(r->engine, r->shard->id, owner_shard, &msg,
                          &r->proxy_wake_mask);
}

/* ── Per-slot reset (cleanup resources) ──────────────────────────────── */

static void net_slot_reset(struct reactor *r, struct net_pipeline_slot *s) {
  /* reply_data is only a heap pointer when reply_type == REPLY_TYPE_BUF.
   * For REPLY_TYPE_OK/NIL/INT/ERR the reply_data points to a static string
   * literal from proxy_exec — do NOT free it. */
  void *heap_buf = (s->reply_type == REPLY_TYPE_BUF) ? s->reply_data : NULL;
  if (s->kv_obj_ptr || heap_buf) {
    net_remote_obj_release(r, s->shard_owner, s->kv_obj_ptr, heap_buf);
    s->kv_obj_ptr = NULL;
    s->reply_data = NULL;
  }
  if (s->is_multi) {
    if (s->multi_kv_objs || s->multi_replies) {
      for (uint32_t j = 0; j < s->multi_parts; j++) {
        void *kv = s->multi_kv_objs ? s->multi_kv_objs[j] : NULL;
        void *rep = s->multi_replies ? s->multi_replies[j] : NULL;
        if (kv || rep) {
          uint8_t owner = s->multi_owners ? s->multi_owners[j] : (uint8_t)j;
          net_remote_obj_release(r, owner, kv, rep);
        }
      }
    }
  }

  /* Free multi arrays (these are allocated locally by the reactor) */
  if (s->is_multi) {
    if (s->multi_replies) {
      slab_obj_free(r->shard->pool, s->multi_replies);
      s->multi_replies = NULL;
    }
    if (s->multi_reply_lens) {
      slab_obj_free(r->shard->pool, s->multi_reply_lens);
      s->multi_reply_lens = NULL;
    }
    if (s->multi_kv_objs) {
      slab_obj_free(r->shard->pool, s->multi_kv_objs);
      s->multi_kv_objs = NULL;
    }
    if (s->multi_owners) {
      slab_obj_free(r->shard->pool, s->multi_owners);
      s->multi_owners = NULL;
    }
  }

  /* Release input buffer ref */
  if (s->req_nb) {
    net_rbuf_try_free(r->shard->pool, s->req_nb);
    s->req_nb = NULL;
  }

  /* Free resp_cmd dynamic arrays (always heap allocated now) */
  if (s->cmd.argv)
    slab_obj_free(r->shard->pool, s->cmd.argv);
  if (s->cmd.arglen)
    slab_obj_free(r->shard->pool, s->cmd.arglen);

  /* Zero out and reset invariant */
  memset(s, 0, sizeof(*s));
}

/* ── Slot write to wbuf (serialize reply) ────────────────────────────── */

static int resp_wbuf_append(struct reactor *r, struct net_conn *c,
                            const void *data, size_t len) {
  size_t needed = c->wbuf_len + len;
  if (needed > c->wbuf_cap) {
    size_t newcap = c->wbuf_cap ? c->wbuf_cap * 2 : 256;
    while (newcap < needed)
      newcap *= 2;
    uint8_t *nb = (uint8_t *)slab_obj_realloc(r->shard->pool, c->wbuf, newcap);
    if (!nb)
      return -1;
    c->wbuf = nb;
    c->wbuf_cap = newcap;
  }
  memcpy(c->wbuf + c->wbuf_len, data, len);
  c->wbuf_len += len;
  return 0;
}

static inline size_t itoa_fast(size_t val, char *buf) {
  if (val == 0) {
    buf[0] = '0';
    return 1;
  }
  char tmp[24];
  int i = 0;
  while (val > 0) {
    tmp[i++] = '0' + (val % 10);
    val /= 10;
  }
  size_t len = i;
  for (int j = 0; j < i; j++)
    buf[j] = tmp[i - 1 - j];
  return len;
}

static int resp_wbuf_append_kv(struct reactor *r, struct net_conn *c,
                               struct kv_obj *o) {
  size_t vlen = obj_val_len_get(o);
  char hdr[32];
  hdr[0] = '$';
  size_t hl = 1 + itoa_fast(vlen, hdr + 1);
  hdr[hl++] = '\r';
  hdr[hl++] = '\n';

  size_t needed = c->wbuf_len + hl + vlen + 2;
  if (needed > c->wbuf_cap) {
    size_t newcap = c->wbuf_cap ? c->wbuf_cap * 2 : 256;
    while (newcap < needed)
      newcap *= 2;
    uint8_t *nb = (uint8_t *)slab_obj_realloc(r->shard->pool, c->wbuf, newcap);
    if (!nb)
      return -1;
    c->wbuf = nb;
    c->wbuf_cap = newcap;
  }

  uint8_t *dst = c->wbuf + c->wbuf_len;
  memcpy(dst, hdr, hl);
  dst += hl;
  memcpy(dst, obj_val_get(o), vlen);
  dst += vlen;
  dst[0] = '\r';
  dst[1] = '\n';
  c->wbuf_len = needed;
  return 0;
}

static void net_slot_write_to_wbuf(struct reactor *r, struct net_conn *c,
                                   struct net_pipeline_slot *s) {
  if (s->is_multi) {
    if (s->parent_cmd_id == MSG_CMD_MSET_PART) {
      /* MSET: single +OK\r\n */
      if (s->reply_data)
        resp_wbuf_append(r, c, s->reply_data, s->reply_len);
    } else if (s->parent_cmd_id == MSG_CMD_DEL_PART) {
      /* DEL: sum per-key delete counts (each stored as ":0\r\n" or ":1\r\n")
       */
      long long total = 0;
      for (uint32_t j = 0; j < s->multi_parts; j++) {
        if (s->multi_replies[j] && s->multi_reply_lens[j] > 3) {
          char tmp[32] = {0};
          size_t vlen = s->multi_reply_lens[j] - 3; /* skip ':', '\r', '\n' */
          if (vlen > 31)
            vlen = 31;
          memcpy(tmp, (const char *)s->multi_replies[j] + 1, vlen);
          total += strtoll(tmp, NULL, 10);
        }
      }
      char buf[32];
      buf[0] = ':';
      size_t bl = 1 + itoa_fast((size_t)total, buf + 1);
      buf[bl++] = '\r';
      buf[bl++] = '\n';
      resp_wbuf_append(r, c, buf, bl);
      return;
    } else if (s->parent_cmd_id == MSG_CMD_DBSIZE) {
      /* DBSIZE: sum all per-shard counts */
      long long total = 0;
      for (uint32_t j = 0; j < s->multi_parts; j++) {
        if (s->multi_replies[j] && s->multi_reply_lens[j] > 3) {
          char tmp[32] = {0};
          size_t vlen = s->multi_reply_lens[j] - 3;
          if (vlen > 31)
            vlen = 31;
          memcpy(tmp, (const char *)s->multi_replies[j] + 1, vlen);
          total += strtoll(tmp, NULL, 10);
        } else if (j == (uint32_t)r->shard->id) {
          /* Fallback to local table count if reply buffer is missing */
          total += (long long)r->shard->table->used;
        }
      }
      char buf[64];
      buf[0] = ':';
      size_t bl = 1 + itoa_fast((size_t)total, buf + 1);
      buf[bl++] = '\r';
      buf[bl++] = '\n';
      resp_wbuf_append(r, c, buf, bl);
      return;
    } else {
      /* MGET: array header + per-part replies */
      if (s->reply_data)
        resp_wbuf_append(r, c, s->reply_data, s->reply_len);
      for (uint32_t j = 0; j < s->multi_parts; j++) {
        if (s->multi_replies[j]) {
          resp_wbuf_append(r, c, s->multi_replies[j], s->multi_reply_lens[j]);
        } else if (s->multi_kv_objs[j]) {
          resp_wbuf_append_kv(r, c, (struct kv_obj *)s->multi_kv_objs[j]);
        } else {
          resp_wbuf_append(r, c, "$-1\r\n", 5);
        }
      }
    }
  } else {
    /* ── Zero-malloc fast path: use reply_type to format RESP inline ── */
    switch (s->reply_type) {
    case REPLY_TYPE_OK:
      resp_wbuf_append(r, c, "+OK\r\n", 5);
      break;
    case REPLY_TYPE_NIL:
      resp_wbuf_append(r, c, "$-1\r\n", 5);
      break;
    case REPLY_TYPE_PONG:
      resp_wbuf_append(r, c, "+PONG\r\n", 7);
      break;
    case REPLY_TYPE_INT: {
      char buf[32];
      buf[0] = ':';
      size_t i = 1;
      long long val = (long long)s->reply_int;
      int neg = (val < 0);
      uint64_t u = neg ? (uint64_t)(-(val + 1)) + 1 : (uint64_t)val;
      if (neg)
        buf[i++] = '-';
      i += itoa_fast((size_t)u, buf + i);
      buf[i++] = '\r';
      buf[i++] = '\n';
      resp_wbuf_append(r, c, buf, i);
      break;
    }
    case REPLY_TYPE_ERR:
      resp_wbuf_append(r, c, "-ERR ", 5);
      if (s->reply_data && s->reply_len > 0)
        resp_wbuf_append(r, c, s->reply_data, s->reply_len);
      resp_wbuf_append(r, c, "\r\n", 2);
      break;
    case REPLY_TYPE_BUF:
    case REPLY_TYPE_BUF_STATIC:
      if (s->reply_data && s->reply_len > 0)
        resp_wbuf_append(r, c, s->reply_data, s->reply_len);
      break;
    case REPLY_TYPE_KV_OBJ:
      if (s->kv_obj_ptr) {
        resp_wbuf_append_kv(r, c, (struct kv_obj *)s->kv_obj_ptr);
      }
      break;
    case REPLY_TYPE_NONE:
    default:
      break;
    }
  }
}

/* ── Streaming flush — core new logic ────────────────────────────────── */

static void net_read_handler(struct reactor *r, struct net_conn *c);
static void net_write_handler(struct reactor *r, struct net_conn *c);
static void net_pipeline_try_flush(struct reactor *r, struct net_conn *c);
static void net_reactor_proxy_reply_callback(void *ctx,
                                             const struct spsc_message *msg);

static void net_reactor_mark_conn_dirty_callback(void *ctx, struct net_conn *c,
                                                 const char *reason) {
  struct reactor *r = (struct reactor *)ctx;
  if (!r || !c)
    return;
  net_reactor_mark_dirty_reason(r, c, reason ? reason : "mset_ready");
}

#include "resp_handler.h"

static void net_write_handler(struct reactor *r, struct net_conn *c) {
  for (;;) {
    /* If wbuf is fully sent, try pulling more data from pipeline */
    if (c->wbuf_sent >= c->wbuf_len) {
      c->wbuf_len = c->wbuf_sent = 0;
      net_pipeline_try_flush(r, c);
      if (c->wbuf_sent >= c->wbuf_len) {
        /* Pipeline is also empty of DONE replies */
        net_conn_write_done(r, c);

        if (c->state == CONN_ALIVE) {
          /* Stay alive, just stop EPOLLOUT for now */
          net_reactor_list_remove(r, c);
        } else if (c->state == CONN_CLOSING) {
          /* Fully flushed everything during a graceful close */
          net_conn_try_free(r, c);
        }
        return;
      }
    }

    /* We have data in wbuf to write */
    ssize_t n =
        write(c->fd, c->wbuf + c->wbuf_sent, c->wbuf_len - c->wbuf_sent);
    if (n < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        /* Socket buffer full: must wait for Master Flush or next EPOLLOUT */
        net_reactor_mark_dirty_reason(r, c, "socket_eagain");
        return;
      }
      /* Hard error: mark as dead to close FD immediately */
      net_reactor_mark_dead(r, c);
      return;
    }
    c->wbuf_sent += (size_t)n;
  }
}

static void net_pipeline_try_flush(struct reactor *r, struct net_conn *c) {
  if (!c)
    return;
  /* Run even when fd is closed (c->fd < 0): this drains PSLOT_DONE slots,
   * advances pp->out, and frees per-slot resources via net_slot_reset.
   * net_slot_write_to_wbuf writes to c->wbuf (heap), not to the fd —
   * safe here; the wbuf is discarded when net_conn_free runs. */
  struct net_proxy_pipeline *pp = &c->proxy;

  while (pp->out != pp->in) {
    struct net_pipeline_slot *s = &pp->slots[pp->out & (pp->cap - 1)];

    /* Stop at first incomplete slot (head-of-line, but per-slot not per-batch)
     */
    if (s->state != PSLOT_LOCAL && s->state != PSLOT_DONE) {
      break;
    }

    /* Serialize reply into wbuf */
    net_slot_write_to_wbuf(r, c, s);

    /* Release refs, free reply_data, reset slot */
    net_slot_reset(r, s);
    pp->out++;
  }

}

// Move resp_handler.h to top

/* ── Handlers ────────────────────────────────────────────────────────── */

static void net_accept_handler(struct reactor *r) {
  for (;;) {
    struct sockaddr_in addr;
    socklen_t addr_len = sizeof(addr);
    int cfd = accept4(r->listen_fd, (struct sockaddr *)&addr, &addr_len,
                      SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (cfd < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK)
        break;
      if (errno == EINTR)
        continue;
      break;
    }
    net_fd_nodelay_set(cfd);
    struct net_conn *c = net_conn_alloc(r);
    if (!c) {
      close(cfd);
      continue;
    }
    c->fd = cfd;
    c->is_shared = false;
    struct epoll_event ev = {.events = EPOLLIN | EPOLLET | EPOLLRDHUP,
                             .data.ptr = c};
    if (epoll_ctl(r->epoll_fd, EPOLL_CTL_ADD, cfd, &ev) < 0)
      net_reactor_mark_dead(r, c);
  }
}

static void net_accept_shared_handler(struct reactor *r) {
  for (;;) {
    struct sockaddr_in addr;
    socklen_t addr_len = sizeof(addr);
    int cfd = accept4(r->shared_listen_fd, (struct sockaddr *)&addr, &addr_len,
                      SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (cfd < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK)
        break;
      if (errno == EINTR)
        continue;
      break;
    }
    net_fd_nodelay_set(cfd);
    struct net_conn *c = net_conn_alloc(r);
    if (!c) {
      close(cfd);
      continue;
    }
    c->fd = cfd;
    c->is_shared = true;
    struct epoll_event ev = {.events = EPOLLIN | EPOLLET | EPOLLRDHUP,
                             .data.ptr = c};
    if (epoll_ctl(r->epoll_fd, EPOLL_CTL_ADD, cfd, &ev) < 0)
      net_reactor_mark_dead(r, c);
  }
}

static void net_read_handler(struct reactor *r, struct net_conn *c) {
  if (!c)
    return;
  if (c->pending_reason != CONN_PENDING_NONE)
    return;
  handle_read_resp(r, c);
  net_reactor_mark_dirty_reason(r, c, "read_handler");
}

static void net_reactor_proxy_reply_callback(void *ctx,
                                             const struct spsc_message *msg) {
  struct reactor *r = (struct reactor *)ctx;

  /* ── Intercept MSET ACK/FIN_ACK — handled by mset_exec, not pipeline ──── */
  uint16_t cmd_id = MSG_GET_CMD(msg->op);
  if (cmd_id == MSG_CMD_MSET_ACK) {
    mset_on_ack(r->engine, r->shard, (struct spsc_message *)msg);
    mset_pending_conn_drain(r, 8);
    /* Mark dirty: mset_on_ack may have set +OK reply locally (if local
     * coordinator processed the last FIN key).  If FINs are still pending,
     * the slot is PSLOT_PENDING and the flush is a safe no-op. */
    struct net_conn *c = (struct net_conn *)msg->u.mset_ack.conn_ptr;
    if (c)
      net_reactor_mark_dirty_reason(r, c, "mset_ack");
    return;
  }

  if (cmd_id == MSG_CMD_MSET_FIN_ACK) {
    mset_on_fin_ack(r->engine, r->shard, (struct spsc_message *)msg);
    mset_pending_conn_drain(r, 8);
    /* +OK reply is now set in the pipeline slot — flush to client */
    struct mset_stat *stat = msg->u.mset_stat.stat;
    struct net_conn *c = stat ? (struct net_conn *)stat->conn_ptr : NULL;
    if (c && c->generation == stat->conn_generation)
      net_reactor_mark_dirty_reason(r, c, "mset_fin_ack");
    return;
  }
  
  struct net_conn *c = (struct net_conn *)msg->u.reply.conn_ptr;
  if (!c) {
    net_remote_obj_release(r, msg->shard_owner, msg->u.reply.kv_obj_ptr,
                           msg->u.reply.reply_buf);
    return;
  }

  struct net_proxy_pipeline *pp = &c->proxy;
  uint32_t seq = msg->u.reply.pipeline_idx;

  /* Bounds check: seq must be in [out, in) using signed diff for uint32 wrap */
  if ((int32_t)(seq - pp->out) < 0 || (int32_t)(seq - pp->in) >= 0) {
    net_remote_obj_release(r, msg->shard_owner, msg->u.reply.kv_obj_ptr,
                           msg->u.reply.reply_buf);
    return;
  }

  struct net_pipeline_slot *slot = &pp->slots[seq & (pp->cap - 1)];
  if (slot->is_multi) {
    uint32_t sidx = msg->u.reply.sub_idx;
    if (sidx < slot->multi_parts) {
      slot->multi_replies[sidx] = (uint8_t *)msg->u.reply.reply_buf;
      slot->multi_reply_lens[sidx] = msg->u.reply.reply_len;
      if (slot->multi_kv_objs)
        slot->multi_kv_objs[sidx] = msg->u.reply.kv_obj_ptr;
      if (slot->multi_owners)
        slot->multi_owners[sidx] = msg->shard_owner;
      slot->multi_replied++;
      if (slot->multi_replied >= slot->multi_parts) {
        slot->state = PSLOT_DONE;
      }
    }
  } else {
    /* Zero-malloc path: copy reply type/int directly into slot,
     * only reply_data for ERR/BUF which point to static literals. */
    slot->reply_type = msg->u.reply.reply_type;
    slot->reply_int = msg->u.reply.reply_int;
    slot->reply_data =
        (uint8_t *)msg->u.reply.reply_buf; /* static literal or NULL */
    slot->reply_len = msg->u.reply.reply_len;
    slot->kv_obj_ptr = msg->u.reply.kv_obj_ptr;
    slot->shard_owner = msg->shard_owner;
    slot->state = PSLOT_DONE;
  }

  /* Queue for batch flush — do not call syscalls inside spsc drain loop */
  net_reactor_mark_dirty_reason(r, c, "proxy_reply");
}

/* ── Public API ───────────────────────────────────────────────────────── */

static int net_listen_socket_create(uint16_t port, bool reuse_port) {
  int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  if (fd < 0)
    return -1;
  int y = 1;
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &y, sizeof(y));
  if (reuse_port)
    setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &y, sizeof(y));
  struct sockaddr_in addr = {.sin_family = AF_INET,
                             .sin_port = htons(port),
                             .sin_addr.s_addr = INADDR_ANY};
  if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
    close(fd);
    return -1;
  }
  if (listen(fd, 4096) < 0) {
    close(fd);
    return -1;
  }
  return fd;
}

int net_reactor_init(struct reactor *r, struct shard_engine *engine,
                     struct shard *shard, uint16_t base_port,
                     uint16_t shard_port, const char *advertise_ip) {
  memset(r, 0, sizeof(*r));
  r->engine = engine;
  r->shard = shard;
  shard->proxy_cb = net_reactor_proxy_reply_callback;
  shard->mark_conn_dirty_cb = net_reactor_mark_conn_dirty_callback;
  shard->mark_conn_dirty_ctx = r;
  r->port = shard_port;
  r->base_port = base_port;
  r->listen_fd = -1;
  r->shared_listen_fd = -1;
  r->wake_fd = -1;
  r->timer_fd = -1;
  r->conn_generation = 0;
  r->epoll_fd = epoll_create1(EPOLL_CLOEXEC);
  if (r->epoll_fd < 0)
    return -1;
  r->wake_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (r->wake_fd < 0)
    goto fail;
  struct epoll_event ev;
  memset(&ev, 0, sizeof(ev));
  ev.events = EPOLLIN | EPOLLET;

  ev.data.u64 = EV_PACK(EV_TYPE_WAKE, r->wake_fd);
  if (epoll_ctl(r->epoll_fd, EPOLL_CTL_ADD, r->wake_fd, &ev) < 0)
    goto fail;

  r->timer_fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
  if (r->timer_fd < 0)
    goto fail;
  struct itimerspec its = {.it_interval = {0, EXPIRY_INTERVAL_MS * 1000000},
                           .it_value = {0, EXPIRY_INTERVAL_MS * 1000000}};
  timerfd_settime(r->timer_fd, 0, &its, NULL);
  ev.data.u64 = EV_PACK(EV_TYPE_TIMER, r->timer_fd);
  if (epoll_ctl(r->epoll_fd, EPOLL_CTL_ADD, r->timer_fd, &ev) < 0)
    goto fail;

  r->listen_fd = net_listen_socket_create(shard_port, false);
  if (r->listen_fd < 0)
    goto fail;
  ev.data.u64 = EV_PACK(EV_TYPE_LISTEN, r->listen_fd);
  if (epoll_ctl(r->epoll_fd, EPOLL_CTL_ADD, r->listen_fd, &ev) < 0)
    goto fail;

  r->shared_listen_fd = net_listen_socket_create(base_port, true);
  if (r->shared_listen_fd < 0)
    goto fail;
  ev.data.u64 = EV_PACK(EV_TYPE_SHARED, r->shared_listen_fd);
  if (epoll_ctl(r->epoll_fd, EPOLL_CTL_ADD, r->shared_listen_fd, &ev) < 0)
    goto fail;

  /* Register wake_fd in engine for cross-shard messaging */
  r->engine->wake_fds[r->shard->id] = r->wake_fd;
  r->dirty_head = NULL;
  return 0;
fail:
  if (r->shared_listen_fd >= 0)
    close(r->shared_listen_fd);
  if (r->listen_fd >= 0)
    close(r->listen_fd);
  if (r->wake_fd >= 0)
    close(r->wake_fd);
  if (r->epoll_fd >= 0)
    close(r->epoll_fd);
  return -1;
}

static void reactor_handle_timer(struct reactor *r) {
  uint64_t expirations;
  if (read(r->timer_fd, &expirations, sizeof(expirations)) < 0) {
  }
  uint64_t cur = net_time_ms_get();
  for (int batch = 0; batch < EXPIRY_MAX_BATCHES; batch++) {
    int n = shard_ttl_drain(r->shard, cur, EXPIRY_BATCH_SIZE);
    if (n < (EXPIRY_BATCH_SIZE * EXPIRY_ADAPTIVE_PCT / 100))
      break;
  }
  snap_engine_tick(&r->shard->snap, r->shard);
}

void net_reactor_run(struct reactor *r) {
  struct epoll_event events[MAX_EVENTS];
  while (r->shard->running) {
    /* 1. Sync cross-shard replies into pipeline slots */
    shard_msg_drain(r->engine, r->shard, net_reactor_proxy_reply_callback, r);
    mset_pending_conn_drain(r, 8);

    /* 2. Wait for network events */
    int nev = epoll_wait(r->epoll_fd, events, MAX_EVENTS, EPOLL_TIMEOUT_MS);

    for (int i = 0; i < nev; i++) {
      struct epoll_event *ev = &events[i];

      if (ev->data.u64 & EV_MARK_SPECIAL) {
        uint32_t type = EV_GET_TYPE(ev->data.u64);
        if (type == EV_TYPE_LISTEN) {
          net_accept_handler(r);
          continue;
        }
        if (type == EV_TYPE_SHARED) {
          net_accept_shared_handler(r);
          continue;
        }
        if (type == EV_TYPE_TIMER) {
          reactor_handle_timer(r);
          continue;
        }
        if (type == EV_TYPE_WAKE) {
          uint64_t cnt;
          if (read(r->wake_fd, &cnt, sizeof(cnt)) < 0) {
          }
          continue;
        }
        continue;
      }

      struct net_conn *c = (struct net_conn *)ev->data.ptr;
      if (!c || c->state == CONN_DEAD)
        continue;

      /* PRIORITY 1: Hard Errors / Hangup (FD gone, cannot write) */
      if (ev->events & (EPOLLERR | EPOLLHUP)) {
        net_reactor_mark_dead(r, c);
        continue;
      }

      /* PRIORITY 2: Data available to read */
      if (ev->events & EPOLLIN) {
        net_read_handler(r, c);
        if (c->state == CONN_DEAD)
          continue;
      }

      /* PRIORITY 3: Client initiated graceful close (EOF) */
      if (ev->events & EPOLLRDHUP) {
        net_conn_mark_closing(r, c);
      }

      /* PRIORITY 4: Socket buffer space available for write */
      if (ev->events & EPOLLOUT) {
        net_write_handler(r, c);
      }
    }

    /* 3. Master Flush — dynamically drain all dirty connections */
    while (r->dirty_head) {
      struct net_conn *curr = r->dirty_head;
      net_reactor_list_remove(r, curr);

      if (curr->fd >= 0 && curr->pending_reason == CONN_PENDING_NONE &&
          curr->resp.rbuf_parsed < curr->rbuf_len) {
        handle_read_resp(r, curr);
        if (curr->state == CONN_DEAD)
          continue;
      }

      /* Drain the pipeline and attempt write if FD is still open */
      net_pipeline_try_flush(r, curr);

      if (curr->fd >= 0 && (curr->wbuf_len > curr->wbuf_sent)) {
        net_write_handler(r, curr);
      }

      /* Final check: if closing and everything is sent, try freeing */
      if (curr->state == CONN_CLOSING) {
        net_conn_try_free(r, curr);
      }
    }

    /* 4. Dead List Cleanup — reclaim memory for defunct connections */
    struct net_conn *dead = r->dead_head;
    while (dead) {
      struct net_conn *next = dead->next;
      net_conn_try_free(r, dead);
      dead = next;
    }

    mset_pending_conn_drain(r, 8);
  }
}

void net_reactor_destroy(struct reactor *r) {
  if (!r)
    return;
  if (r->listen_fd >= 0)
    close(r->listen_fd);
  if (r->shared_listen_fd >= 0)
    close(r->shared_listen_fd);
  if (r->wake_fd >= 0)
    close(r->wake_fd);
  if (r->timer_fd >= 0)
    close(r->timer_fd);
  if (r->epoll_fd >= 0)
    close(r->epoll_fd);
}
