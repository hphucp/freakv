#ifndef RESP_HANDLER_H
#define RESP_HANDLER_H

#include "resp.h"

#include "../core/message.h"
#include "../core/mset_exec.h"
#include "../core/routing.h"
#include "../core/shard.h"
#include "../memory/slab.h"
#include "../net/conn.h"
#include "../net/reactor.h"

#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* Forward declarations */
static void net_conn_mark_closing(struct reactor *r, struct net_conn *c);
static void net_reactor_mark_dead(struct reactor *r, struct net_conn *c);
static void net_write_handler(struct reactor *r, struct net_conn *c);
static inline uint64_t net_time_ms_get(void);
static int resp_wbuf_append(struct reactor *r, struct net_conn *c,
                            const void *data, size_t len);
static void net_pipeline_try_flush(struct reactor *r, struct net_conn *c);
static void net_reactor_proxy_reply_callback(void *ctx,
                                             const struct spsc_message *msg);



/* ── Buffer grow (partial copy to preserve the in-progress command) ──── */

static int net_rbuf_grow(struct net_conn *c, size_t need) {
  size_t required = c->rbuf_len + need;
  if (required <= c->rbuf_nb->cap)
    return 0;

  /* Only need to carry forward the part from cmd_start onward */
  size_t partial_len = c->rbuf_len - c->resp.cmd_start;
  size_t min_cap = partial_len + need;
  size_t newcap = c->rbuf_nb->cap ? c->rbuf_nb->cap * 2 : RESP_RBUF_INIT;
  while (newcap < min_cap)
    newcap *= 2;

  struct net_buf *nb = net_buf_alloc(c->allocator, newcap);
  if (!nb)
    return -1;

  uint8_t *partial = c->rbuf_nb->data + c->resp.cmd_start;
  if (partial_len > 0)
    memcpy(nb->data, partial, partial_len);

  /* Fixup argv pointers of the command currently being parsed */
  ptrdiff_t delta = (char *)nb->data - (char *)partial;
  for (int i = 0; i < c->resp.argc_got; i++)
    if (c->resp.argv[i])
      c->resp.argv[i] += delta;

  net_buf_unref(c->allocator, c->rbuf_nb);
  c->rbuf_nb = nb;
  c->rbuf_len = partial_len;
  c->resp.rbuf_parsed -= c->resp.cmd_start;
  c->resp.cmd_start = 0;
  return 0;
}

/* ── Parser helpers ───────────────────────────────────────────────────── */

static void resp_parser_reset(struct net_conn *c) {
  struct resp_parser *p = &c->resp;
  if (p->rbuf_parsed >= c->rbuf_len) {
    /* Buffer fully consumed */
    c->rbuf_len = 0;
    p->rbuf_parsed = 0;
    p->cmd_start = 0;
  } else {
    /* More bytes remain — next command starts here */
    p->cmd_start = p->rbuf_parsed;
  }
  p->parse_state = RESP_PARSE_ARRAY_LEN;
  p->argc_expected = 0;
  p->argc_got = 0;
  p->bulk_len = 0;
  p->bulk_data_read = 0;
}

#define RESP_APPEND(r, c, lit)                                                 \
  resp_wbuf_append((r), (c), (lit), sizeof(lit) - 1)
static void resp_reply_ok(struct reactor *r, struct net_conn *c) {
  RESP_APPEND(r, c, "+OK\r\n");
}
static void resp_reply_pong(struct reactor *r, struct net_conn *c) {
  RESP_APPEND(r, c, "+PONG\r\n");
}
static void resp_reply_nil(struct reactor *r, struct net_conn *c) {
  RESP_APPEND(r, c, "$-1\r\n");
}
static void resp_reply_err(struct reactor *r, struct net_conn *c,
                           const char *msg) {
  RESP_APPEND(r, c, "-ERR ");
  resp_wbuf_append(r, c, msg, strlen(msg));
  RESP_APPEND(r, c, "\r\n");
}

static inline int resp_u64dec(char *buf, uint64_t v) {
  if (v == 0) {
    buf[0] = '0';
    return 1;
  }
  char tmp[20];
  int i = 0;
  while (v) {
    tmp[i++] = (char)('0' + (v % 10));
    v /= 10;
  }
  for (int a = 0, b = i - 1; a < b; a++, b--) {
    char t = tmp[a];
    tmp[a] = tmp[b];
    tmp[b] = t;
  }
  memcpy(buf, tmp, (size_t)i);
  return i;
}

static void resp_reply_int(struct reactor *r, struct net_conn *c, long long v) {
  char buf[24];
  buf[0] = ':';
  int neg = (v < 0);
  uint64_t u = neg ? (uint64_t)(-(v + 1)) + 1 : (uint64_t)v;
  int n = 1;
  if (neg)
    buf[n++] = '-';
  n += resp_u64dec(buf + n, u);
  buf[n] = '\r';
  buf[n + 1] = '\n';
  resp_wbuf_append(r, c, buf, (size_t)(n + 2));
}

/* ── Phase 1: Slot-filling helpers (Deferred Serialization) ── */

static inline void resp_reply_ok_slot(struct net_pipeline_slot *s) {
  s->reply_type = REPLY_TYPE_OK;
}

static inline void resp_reply_pong_slot(struct net_pipeline_slot *s) {
  s->reply_type = REPLY_TYPE_PONG;
}

static inline void resp_reply_nil_slot(struct net_pipeline_slot *s) {
  s->reply_type = REPLY_TYPE_NIL;
}

static inline void resp_reply_int_slot(struct net_pipeline_slot *s,
                                       long long v) {
  s->reply_type = REPLY_TYPE_INT;
  s->reply_int = v;
}

static inline void resp_reply_err_slot(struct net_pipeline_slot *s,
                                       const char *msg) {
  s->reply_type = REPLY_TYPE_ERR;
  s->reply_data = (uint8_t *)msg;
  s->reply_len = (uint32_t)strlen(msg);
}

static inline void resp_reply_kv_slot(struct net_pipeline_slot *s, void *obj) {
  if (!obj) {
    resp_reply_nil_slot(s);
    return;
  }
  s->kv_obj_ptr = obj;
  s->reply_type = REPLY_TYPE_KV_OBJ;
}

static void resp_reply_bulk(struct reactor *r, struct net_conn *c,
                            const void *data, size_t len) {
  char hdr[24];
  hdr[0] = '$';
  int n = 1 + resp_u64dec(hdr + 1, (uint64_t)len);
  hdr[n] = '\r';
  hdr[n + 1] = '\n';
  resp_wbuf_append(r, c, hdr, (size_t)(n + 2));
  resp_wbuf_append(r, c, data, len);
  RESP_APPEND(r, c, "\r\n");
}

/* ── Command dispatcher (local execution) ────────────────────────────── */

static void resp_dispatch(struct reactor *r, struct net_conn *c,
                          struct net_pipeline_slot *s) {
  struct resp_parser *p = &c->resp;
  if (p->argc_got < 1) {
    resp_reply_err_slot(s, "empty command");
    return;
  }
  char cmd[32] = {0};
  size_t cmdlen = p->arglen[0] < 31 ? p->arglen[0] : 31;
  for (size_t i = 0; i < cmdlen; i++)
    cmd[i] = (char)toupper((unsigned char)p->argv[0][i]);

  uint64_t now = net_time_ms_get();

  if (strcmp(cmd, "PING") == 0) {
    if (p->argc_got >= 2)
      resp_reply_bulk(r, c, p->argv[1], p->arglen[1]);
    else
      resp_reply_pong_slot(s);
    return;
  }

  if (strcmp(cmd, "ECHO") == 0) {
    if (p->argc_got >= 2)
      resp_reply_bulk(r, c, p->argv[1], p->arglen[1]);
    else
      resp_reply_err(r, c, "wrong number of arguments for 'echo' command");
    return;
  }

  if (strcmp(cmd, "SET") == 0) {
    if (p->argc_got < 3) {
      resp_reply_err(r, c, "wrong number of args for SET");
      return;
    }
    uint32_t put_flags = HT_PUT_NONE;
    bool keepttl = false;
    uint64_t expire_ms = 0;
    for (int i = 3; i < p->argc_got; i++) {
      char opt[16] = {0};
      size_t ol = p->arglen[i] < 15 ? p->arglen[i] : 15;
      for (size_t j = 0; j < ol; j++)
        opt[j] = (char)toupper((unsigned char)p->argv[i][j]);
      if (strcmp(opt, "NX") == 0)
        put_flags = HT_PUT_NX;
      else if (strcmp(opt, "XX") == 0)
        put_flags = HT_PUT_XX;
      else if (strcmp(opt, "KEEPTTL") == 0)
        keepttl = true;
      else if (strcmp(opt, "EX") == 0 && i + 1 < p->argc_got) {
        expire_ms = now + (uint64_t)strtoll(p->argv[++i], NULL, 10) * 1000ULL;
      } else if (strcmp(opt, "PX") == 0 && i + 1 < p->argc_got) {
        expire_ms = now + (uint64_t)strtoll(p->argv[++i], NULL, 10);
      }
    }
    if (keepttl) {
      uint64_t old_exp =
          shard_key_expire_get(r->shard, p->argv[1], p->arglen[1]);
      if (old_exp != (uint64_t)-1 && old_exp > 0)
        expire_ms = old_exp;
    }
    bool ok = shard_key_set(r->shard, p->argv[1], p->arglen[1], p->argv[2],
                            p->arglen[2], expire_ms, put_flags);
    if (ok)
      resp_reply_ok_slot(s);
    else if (put_flags != HT_PUT_NONE)
      resp_reply_nil_slot(s);
    else
      resp_reply_err_slot(s, "OOM");
    return;
  }

  if (strcmp(cmd, "PERSIST") == 0) {
    if (p->argc_got < 2) {
      resp_reply_err_slot(s, "wrong number of args");
      return;
    }
    resp_reply_int_slot(
        s, shard_key_expire(r->shard, p->argv[1], p->arglen[1], 0) ? 1 : 0);
    return;
  }
  if (strcmp(cmd, "EXPIREAT") == 0 || strcmp(cmd, "PEXPIREAT") == 0) {
    if (p->argc_got < 3) {
      resp_reply_err_slot(s, "wrong number of args");
      return;
    }
    char tmp[32] = {0};
    size_t tl = p->arglen[2] < 31 ? p->arglen[2] : 31;
    memcpy(tmp, p->argv[2], tl);
    long long n = strtoll(tmp, NULL, 10);
    uint64_t exp =
        strcmp(cmd, "EXPIREAT") == 0 ? (uint64_t)n * 1000ULL : (uint64_t)n;
    resp_reply_int_slot(
        s, shard_key_expire(r->shard, p->argv[1], p->arglen[1], exp) ? 1 : 0);
    return;
  }

  if (strcmp(cmd, "GET") == 0) {
    if (p->argc_got != 2) {
      resp_reply_err_slot(s, "wrong number of args for GET");
      return;
    }
    struct kv_obj *v = shard_key_get(r->shard, p->argv[1], p->arglen[1], now);
    if (v)
      shard_obj_ref(v);
    resp_reply_kv_slot(s, v);
    return;
  }

  if (strcmp(cmd, "DEL") == 0) {
    if (p->argc_got < 2) {
      resp_reply_err_slot(s, "wrong number of args for DEL");
      return;
    }
    long long deleted = 0;
    for (int i = 1; i < p->argc_got; i++)
      deleted += shard_key_delete(r->shard, p->argv[i], p->arglen[i]) ? 1 : 0;
    resp_reply_int_slot(s, deleted);
    return;
  }

  if (strcmp(cmd, "EXISTS") == 0) {
    if (p->argc_got < 2) {
      resp_reply_err_slot(s, "wrong number of args for EXISTS");
      return;
    }
    struct kv_obj *v = shard_key_get(r->shard, p->argv[1], p->arglen[1], now);
    resp_reply_int_slot(s, v ? 1 : 0);
    return;
  }

  if (strcmp(cmd, "STRLEN") == 0) {
    if (p->argc_got < 2) {
      resp_reply_err_slot(s, "wrong number of args for STRLEN");
      return;
    }
    struct kv_obj *v = shard_key_get(r->shard, p->argv[1], p->arglen[1], now);
    resp_reply_int_slot(s, v ? (long long)obj_val_len_get(v) : 0);
    return;
  }

  if (strcmp(cmd, "EXPIRE") == 0 || strcmp(cmd, "PEXPIRE") == 0) {
    if (p->argc_got < 3) {
      resp_reply_err_slot(s, "wrong number of args");
      return;
    }
    char tmp[32] = {0};
    size_t tl = p->arglen[2] < 31 ? p->arglen[2] : 31;
    memcpy(tmp, p->argv[2], tl);
    long long n = strtoll(tmp, NULL, 10);
    uint64_t exp =
        n > 0 ? (strcmp(cmd, "EXPIRE") == 0 ? now + (uint64_t)n * 1000ULL
                                            : now + (uint64_t)n)
              : 1;
    resp_reply_int_slot(
        s, shard_key_expire(r->shard, p->argv[1], p->arglen[1], exp) ? 1 : 0);
    return;
  }

  if (strcmp(cmd, "TTL") == 0 || strcmp(cmd, "PTTL") == 0) {
    if (p->argc_got < 2) {
      resp_reply_err_slot(s, "wrong number of args");
      return;
    }
    resp_reply_int_slot(s, shard_key_ttl(r->shard, p->argv[1], p->arglen[1],
                                         now, strcmp(cmd, "PTTL") == 0));
    return;
  }

  if (strcmp(cmd, "TYPE") == 0) {
    if (p->argc_got < 2) {
      resp_reply_err_slot(s, "wrong number of args for TYPE");
      return;
    }
    struct kv_obj *v = shard_key_get(r->shard, p->argv[1], p->arglen[1], now);
    if (v) {
      s->reply_data = (uint8_t *)"+string\r\n";
      s->reply_len = 9;
      s->reply_type = REPLY_TYPE_BUF_STATIC;
    } else {
      s->reply_data = (uint8_t *)"+none\r\n";
      s->reply_len = 7;
      s->reply_type = REPLY_TYPE_BUF_STATIC;
    }
    return;
  }

  if (strcmp(cmd, "CLUSTER") == 0) {
    if (p->argc_got < 2) {
      resp_reply_err(r, c, "wrong args for CLUSTER");
      return;
    }
    char subcmd[32] = {0};
    size_t sclen = p->arglen[1] < 31 ? p->arglen[1] : 31;
    for (size_t i = 0; i < sclen; i++)
      subcmd[i] = (char)toupper((unsigned char)p->argv[1][i]);
    if (strcmp(subcmd, "SLOTS") == 0 || strcmp(subcmd, "SHARDS") == 0) {
      uint32_t n = r->engine->num_shards;
      uint16_t base_port = r->base_port;
      char buf[8192];
      int pos = 0;
      pos += snprintf(buf + pos, sizeof(buf) - pos, "*%u\r\n", n);
      for (uint32_t sid = 0; sid < n; sid++) {
        uint32_t slot_start = (16384u * sid) / n;
        uint32_t slot_end = (16384u * (sid + 1)) / n - 1;
        uint16_t port = base_port + (uint16_t)sid + 1;
        pos += snprintf(buf + pos, sizeof(buf) - pos,
                        "*3\r\n:%u\r\n:%u\r\n*3\r\n$9\r\n127.0.0.1\r\n:%u\r\n$"
                        "40\r\n%040x\r\n",
                        slot_start, slot_end, port, sid);
      }
      resp_wbuf_append(r, c, buf, (size_t)pos);
      return;
    }
    resp_reply_ok(r, c);
    return;
  }

  if (strcmp(cmd, "INFO") == 0) {
    const char *info = "# Server\r\nvalkyd_version:0.1.0\r\n# "
                       "Cluster\r\ncluster_enabled:1\r\n";
    resp_reply_bulk(r, c, info, strlen(info));
    return;
  }

  if (strcmp(cmd, "DBSIZE") == 0) {
    resp_reply_int(r, c, (long long)r->shard->table->used);
    return;
  }
  if (strcmp(cmd, "FLUSHDB") == 0 || strcmp(cmd, "FLUSHALL") == 0) {
    shard_table_flush(r->shard);
    resp_reply_ok(r, c);
    return;
  }
  if (strcmp(cmd, "COMMAND") == 0) {
    RESP_APPEND(r, c, "*0\r\n");
    return;
  }
  if (strcmp(cmd, "CLIENT") == 0 || strcmp(cmd, "SELECT") == 0) {
    resp_reply_ok(r, c);
    return;
  }
  if (strcmp(cmd, "HELLO") == 0) {
    RESP_APPEND(r, c,
                "*14\r\n$6\r\nserver\r\n$7\r\nvalkyd\r\n$7\r\nversion\r\n$"
                "5\r\n0.1.0\r\n$5\r\nproto\r\n:2\r\n$2\r\nid\r\n:1\r\n$"
                "4\r\nmode\r\n$7\r\ncluster\r\n$4\r\nrole\r\n$6\r\nmaster\r\n$"
                "7\r\nmodules\r\n*0\r\n");
    return;
  }

  resp_reply_err(r, c, "unknown command");
}

/* ── Proxy dispatch ──────────────────────────────────────────────────── */

static bool proxy_send_req(struct reactor *r, struct net_conn *c,
                           uint32_t target_shard, uint16_t cmd_op,
                           uint32_t pipeline_seq, struct resp_cmd *cmd) {
  struct shard_engine *engine = r->engine;
  uint32_t my_id = r->shard->id;
  struct spsc_queue *q =
      &engine->queues[my_id * engine->num_shards + target_shard];
  struct spsc_message msg = {.op = MSG_PACK_OP(MSG_SYS_REQ, cmd_op),
                             .shard_owner = (uint8_t)my_id,
                             .u.regular_req = {
                                 .cmd = *cmd,
                                 .conn_ptr = c,
                                 .req_nb = c->rbuf_nb,
                                 .pipeline_idx = pipeline_seq,
                             }};
#if FREAKV_PROFILE || FREAKV_MIXED_PROFILE
  msg.sent_cycles = PROF_NOW();
#endif
  if (msg.u.regular_req.req_nb)
    net_buf_ref(msg.u.regular_req.req_nb);
  /* Spin until space is available. The queue is large (SPSC_CAPACITY slots)
   * and rarely fills under normal load. If the target shard stalls (eviction,
   * snapshot I/O), this busy-wait will add latency to this shard's reactor.
   * TODO: consider bounded retry with backpressure to the client (e.g. -ERR
   * BUSY) if the queue remains full after N yields. */
  spsc_queue_push(q, &msg);
  r->shard->cross_shard_sent++;
  r->proxy_wake_mask |= (1U << target_shard);
  return true;
}

static bool resp_mixed_profile_enabled(void) {
  static int cached = -1;
  if (cached < 0) {
    const char *v = getenv("MSET_MIXED_PROFILE");
    cached = (v && v[0] && strcmp(v, "0") != 0) ? 1 : 0;
  }
  return cached != 0;
}

static uint64_t resp_mixed_profile_slow_cycles(void) {
  static uint64_t cached = 0;
  if (!cached) {
    const char *v = getenv("MSET_MIXED_SLOW_US");
    uint64_t us = v && v[0] ? strtoull(v, NULL, 10) : 1000;
    if (!us)
      us = 1000;
    cached = us * 3000ULL;
  }
  return cached;
}

static int proxy_exec_local_key(struct reactor *r, struct net_conn *c,
                                uint16_t cmd_op, uint32_t pipeline_seq,
                                struct resp_parser *p,
                                struct net_pipeline_slot *s,
                                uint64_t now) {
  s->state = PSLOT_PENDING;
  s->shard_owner = r->shard->id;
  s->cmd.argc = (uint32_t)p->argc_got;
  s->cmd.argv =
      (char **)slab_obj_alloc(r->shard->pool, s->cmd.argc * sizeof(char *));
  s->cmd.arglen =
      (size_t *)slab_obj_alloc(r->shard->pool, s->cmd.argc * sizeof(size_t));
  if (!s->cmd.argv || !s->cmd.arglen) {
    if (s->cmd.argv)
      slab_obj_free(r->shard->pool, s->cmd.argv);
    if (s->cmd.arglen)
      slab_obj_free(r->shard->pool, s->cmd.arglen);
    s->cmd.argv = NULL;
    s->cmd.arglen = NULL;
    resp_reply_err(r, c, "OOM");
    return 0;
  }
  for (uint32_t i = 0; i < s->cmd.argc; i++) {
    s->cmd.argv[i] = p->argv[i];
    s->cmd.arglen[i] = p->arglen[i];
  }

  if (!s->req_nb) {
    net_buf_ref(c->rbuf_nb);
    s->req_nb = c->rbuf_nb;
  }

  struct spsc_message msg = {
      .op = MSG_PACK_OP(MSG_SYS_REQ, cmd_op),
      .shard_owner = (uint8_t)r->shard->id,
      .u.regular_req = {
          .cmd = s->cmd,
          .conn_ptr = c,
          .req_nb = c->rbuf_nb,
          .pipeline_idx = pipeline_seq,
      },
  };
  bool prof = resp_mixed_profile_enabled();
  uint64_t t_exec = prof ? cycles_now() : 0;
  enum proxy_exec_result exec_rc = proxy_exec(r->shard, &msg, now);
  if (prof) {
    uint64_t dur = cycles_now() - t_exec;
    if (dur >= resp_mixed_profile_slow_cycles() ||
        exec_rc == PROXY_EXEC_DEFERRED) {
      fprintf(stderr,
              "[MSET_MIXED_LOCAL] shard=%u cmd=%u pidx=%u durcy=%lu "
              "deferred=%d\n",
              r->shard->id, cmd_op, pipeline_seq, (unsigned long)dur,
              exec_rc == PROXY_EXEC_DEFERRED);
    }
  }
  if (exec_rc == PROXY_EXEC_DEFERRED)
    return 1;

  s->reply_type = msg.u.reply.reply_type;
  s->reply_int = msg.u.reply.reply_int;
  s->reply_data = (uint8_t *)msg.u.reply.reply_buf;
  s->reply_len = msg.u.reply.reply_len;
  s->kv_obj_ptr = msg.u.reply.kv_obj_ptr;
  s->state = PSLOT_DONE;
  return 2;
}

static int resp_dispatch_proxy(struct reactor *r, struct net_conn *c,
                               uint32_t pipeline_seq, uint64_t now) {
  struct resp_parser *p = &c->resp;
  if (p->argc_got < 1)
    return 0;
  struct net_pipeline_slot *s =
      &c->proxy.slots[pipeline_seq & (c->proxy.cap - 1)];

  char c0 = (char)toupper((unsigned char)p->argv[0][0]);
  char c1 = p->arglen[0] > 1 ? (char)toupper((unsigned char)p->argv[0][1]) : 0;
  char c2 = p->arglen[0] > 2 ? (char)toupper((unsigned char)p->argv[0][2]) : 0;
  size_t alen = p->arglen[0];

  /* Sub-microsecond Ultra-Fast Path */
  if (alen == 3 && c0 == 'G' && c1 == 'E' && c2 == 'T') {
    if (p->argc_got != 2 || !c->is_shared) {
      resp_dispatch(r, c, s);
      return 0;
    }
    uint32_t owner =
        shard_for_key(p->argv[1], p->arglen[1], r->engine->num_shards);
    if (owner == r->shard->id) {
      if (ht_bucket_is_locked(r->shard->table, p->argv[1], p->arglen[1],
                              VAL_TYPE_STRING)) {
        return proxy_exec_local_key(r, c, MSG_CMD_GET, pipeline_seq, p, s, now);
      }
      resp_dispatch(r, c, s);
      return 0;
    }

    s->state = PSLOT_PENDING;
    net_buf_ref(c->rbuf_nb);
    s->req_nb = c->rbuf_nb;

    struct resp_cmd r_cmd = {
        .argc = p->argc_got,
        .argv = slab_obj_alloc(r->shard->pool, p->argc_got * sizeof(char *)),
        .arglen = slab_obj_alloc(r->shard->pool, p->argc_got * sizeof(size_t))};
    if (!r_cmd.argv || !r_cmd.arglen) {
      if (r_cmd.argv)
        slab_obj_free(r->shard->pool, r_cmd.argv);
      if (r_cmd.arglen)
        slab_obj_free(r->shard->pool, r_cmd.arglen);
      resp_reply_err(r, c, "OOM");
      return 0;
    }
    for (int i = 0; i < (int)p->argc_got; i++) {
      r_cmd.argv[i] = p->argv[i];
      r_cmd.arglen[i] = p->arglen[i];
    }

    proxy_send_req(r, c, owner, MSG_CMD_GET, pipeline_seq, &r_cmd);
    return 1;
  }

  if (alen == 3 && c0 == 'S' && c1 == 'E' && c2 == 'T') {
    if (p->argc_got < 3 || p->argc_got > 6 || !c->is_shared) {
      resp_dispatch(r, c, s);
      return 0;
    }
    uint32_t owner =
        shard_for_key(p->argv[1], p->arglen[1], r->engine->num_shards);
    if (owner == r->shard->id) {
      if (ht_bucket_is_locked(r->shard->table, p->argv[1], p->arglen[1],
                              VAL_TYPE_STRING)) {
        uint16_t cmd_op = MSG_CMD_SET;
        for (int i = 3; i < p->argc_got; i++) {
          char opt0 =
              p->arglen[i] > 0 ? (char)toupper((unsigned char)p->argv[i][0]) : 0;
          char opt1 =
              p->arglen[i] > 1 ? (char)toupper((unsigned char)p->argv[i][1]) : 0;
          if (p->arglen[i] == 2 && opt0 == 'N' && opt1 == 'X')
            cmd_op = MSG_CMD_SET_NX;
          else if (p->arglen[i] == 2 && opt0 == 'X' && opt1 == 'X')
            cmd_op = MSG_CMD_SET_XX;
        }
        return proxy_exec_local_key(r, c, cmd_op, pipeline_seq, p, s, now);
      }
      resp_dispatch(r, c, s);
      return 0;
    }

    s->state = PSLOT_PENDING;
    net_buf_ref(c->rbuf_nb);
    s->req_nb = c->rbuf_nb;

    uint16_t cmd_op = MSG_CMD_SET;
    for (int i = 3; i < p->argc_got; i++) {
      char opt0 =
          p->arglen[i] > 0 ? (char)toupper((unsigned char)p->argv[i][0]) : 0;
      char opt1 =
          p->arglen[i] > 1 ? (char)toupper((unsigned char)p->argv[i][1]) : 0;
      if (p->arglen[i] == 2 && opt0 == 'N' && opt1 == 'X')
        cmd_op = MSG_CMD_SET_NX;
      else if (p->arglen[i] == 2 && opt0 == 'X' && opt1 == 'X')
        cmd_op = MSG_CMD_SET_XX;
    }

    struct resp_cmd r_cmd = {
        .argc = p->argc_got,
        .argv = slab_obj_alloc(r->shard->pool, p->argc_got * sizeof(char *)),
        .arglen = slab_obj_alloc(r->shard->pool, p->argc_got * sizeof(size_t))};
    if (!r_cmd.argv || !r_cmd.arglen) {
      if (r_cmd.argv)
        slab_obj_free(r->shard->pool, r_cmd.argv);
      if (r_cmd.arglen)
        slab_obj_free(r->shard->pool, r_cmd.arglen);
      resp_reply_err(r, c, "OOM");
      return 0;
    }
    for (int i = 0; i < (int)p->argc_got; i++) {
      r_cmd.argv[i] = p->argv[i];
      r_cmd.arglen[i] = p->arglen[i];
    }

    proxy_send_req(r, c, owner, cmd_op, pipeline_seq, &r_cmd);
    return 1;
  }

  /* Fallback full-string parsing */
  char cmd[32] = {0};
  size_t cl = alen < 31 ? alen : 31;
  for (size_t i = 0; i < cl; i++)
    cmd[i] = (char)toupper((unsigned char)p->argv[0][i]);

  if (strcmp(cmd, "PING") == 0 || strcmp(cmd, "ECHO") == 0 ||
      strcmp(cmd, "COMMAND") == 0 || strcmp(cmd, "CLUSTER") == 0 ||
      strcmp(cmd, "FLUSHDB") == 0 || strcmp(cmd, "FLUSHALL") == 0 ||
      strcmp(cmd, "SELECT") == 0 || strcmp(cmd, "CONFIG") == 0 ||
      strcmp(cmd, "CLIENT") == 0 || strcmp(cmd, "INFO") == 0 ||
      strcmp(cmd, "HELLO") == 0 || strcmp(cmd, "RESET") == 0) {
    resp_dispatch(r, c, s);
    return 0;
  }

  if (strcmp(cmd, "DBSIZE") == 0) {
    if (!c->is_shared) {
      resp_dispatch(r, c, s);
      return 0;
    }
    s->state = PSLOT_PENDING;
    s->shard_owner = r->shard->id;
    s->is_multi = true;
    s->parent_cmd_id = MSG_CMD_DBSIZE;
    s->multi_parts = r->engine->num_shards;
    s->multi_replies = (uint8_t **)slab_obj_calloc(
        r->shard->pool, s->multi_parts, sizeof(uint8_t *));
    s->multi_reply_lens = (uint32_t *)slab_obj_calloc(
        r->shard->pool, s->multi_parts, sizeof(uint32_t));
    s->multi_replied = 0;
    s->multi_kv_objs = NULL;
    s->multi_owners = NULL;

    for (uint32_t i = 0; i < s->multi_parts; i++) {
      if (i == (uint32_t)r->shard->id) {
        /* Local part for self-shard: no SPSC needed */
        s->multi_replied++;
        s->multi_reply_lens[i] = (uint32_t)snprintf(
            NULL, 0, ":%lld\r\n", (long long)r->shard->table->used);
        char *lrb =
            (char *)slab_obj_alloc(r->shard->pool, s->multi_reply_lens[i] + 1);
        snprintf(lrb, s->multi_reply_lens[i] + 1, ":%lld\r\n",
                 (long long)r->shard->table->used);
        s->multi_replies[i] = (uint8_t *)lrb;
        continue;
      }
      struct spsc_queue *q =
          &r->engine->queues[r->shard->id * r->engine->num_shards + i];
      struct spsc_message msg = {.op = MSG_PACK_OP(MSG_SYS_REQ, MSG_CMD_DBSIZE),
                                 .shard_owner = (uint8_t)r->shard->id,
                                 .u.dbsize_req = {
                                     .conn_ptr = c,
                                     .pipeline_idx = pipeline_seq,
                                     .sub_idx = i,
                                 }};
#if FREAKV_PROFILE || FREAKV_MIXED_PROFILE
      msg.sent_cycles = PROF_NOW();
#endif
      /* Spin until space is available. The queue is large (SPSC_CAPACITY slots)
       * and rarely fills under normal load. If the target shard stalls
       * (eviction, snapshot I/O), this busy-wait will add latency to this
       * shard's reactor.
       * TODO: consider bounded retry with backpressure to the client (e.g. -ERR
       * BUSY) if the queue remains full after N yields. */
      spsc_queue_push(q, &msg);
      r->proxy_wake_mask |= (1U << i);
    }
    if (s->multi_replied >= s->multi_parts)
      s->state = PSLOT_DONE;
    return 1;
  }

  if (p->argc_got < 2 || !c->is_shared) {
    resp_dispatch(r, c, s);
    return 0;
  }

  uint32_t owner =
      shard_for_key(p->argv[1], p->arglen[1], r->engine->num_shards);

  if (owner == r->shard->id && strcmp(cmd, "MGET") != 0 &&
      strcmp(cmd, "MSET") != 0 && strcmp(cmd, "DEL") != 0 &&
      !ht_bucket_is_locked(r->shard->table, p->argv[1], p->arglen[1],
                           VAL_TYPE_STRING)) {
    resp_dispatch(r, c, s);
    return 0;
  }

  uint16_t cmd_op = 0;
  if (strcmp(cmd, "GET") == 0) {
    if (p->argc_got != 2) {
      resp_dispatch(r, c, s);
      return 0;
    }
    cmd_op = MSG_CMD_GET;
  } else if (strcmp(cmd, "SET") == 0) {
    if (p->argc_got < 3) {
      resp_dispatch(r, c, s);
      return 0;
    }
    cmd_op = MSG_CMD_SET;
    for (int i = 3; i < p->argc_got; i++) {
      char opt[16] = {0};
      size_t ol = p->arglen[i] < 15 ? p->arglen[i] : 15;
      for (size_t j = 0; j < ol; j++)
        opt[j] = (char)toupper((unsigned char)p->argv[i][j]);
      if (strcmp(opt, "NX") == 0)
        cmd_op = MSG_CMD_SET_NX;
      else if (strcmp(opt, "XX") == 0)
        cmd_op = MSG_CMD_SET_XX;
    }
  } else if (strcmp(cmd, "DEL") == 0) {
    if (p->argc_got < 2) {
      resp_dispatch(r, c, s);
      return 0;
    }
    cmd_op = MSG_CMD_DEL_PART;
  } else if (strcmp(cmd, "EXISTS") == 0) {
    if (p->argc_got != 2) {
      resp_dispatch(r, c, s);
      return 0;
    }
    cmd_op = MSG_CMD_EXISTS;
  } else if (strcmp(cmd, "TTL") == 0) {
    if (p->argc_got != 2) {
      resp_dispatch(r, c, s);
      return 0;
    }
    cmd_op = MSG_CMD_TTL;
  } else if (strcmp(cmd, "PTTL") == 0) {
    if (p->argc_got != 2) {
      resp_dispatch(r, c, s);
      return 0;
    }
    cmd_op = MSG_CMD_PTTL;
  } else if (strcmp(cmd, "TYPE") == 0) {
    if (p->argc_got != 2) {
      resp_dispatch(r, c, s);
      return 0;
    }
    cmd_op = MSG_CMD_TYPE;
  } else if (strcmp(cmd, "STRLEN") == 0)
    cmd_op = MSG_CMD_STRLEN;
  else if (strcmp(cmd, "EXPIRE") == 0)
    cmd_op = MSG_CMD_EXPIRE;
  else if (strcmp(cmd, "PEXPIRE") == 0)
    cmd_op = MSG_CMD_PEXPIRE;
  else if (strcmp(cmd, "PERSIST") == 0)
    cmd_op = MSG_CMD_PERSIST;
  else if (strcmp(cmd, "EXPIREAT") == 0)
    cmd_op = MSG_CMD_EXPIREAT;
  else if (strcmp(cmd, "PEXPIREAT") == 0)
    cmd_op = MSG_CMD_PEXPIREAT;
  else if (strcmp(cmd, "MGET") == 0)
    cmd_op = MSG_CMD_MGET_PART;
  else if (strcmp(cmd, "MSET") == 0)
    cmd_op = MSG_CMD_MSET_PART;

  if (cmd_op == 0) {
    resp_dispatch(r, c, s);
    return 0;
  }

  s->state = PSLOT_PENDING;
  net_buf_ref(c->rbuf_nb);
  s->req_nb = c->rbuf_nb;

  if (strcmp(cmd, "MGET") == 0) {
    uint32_t nkeys = (uint32_t)p->argc_got - 1;
    uint32_t my_id = r->shard->id;
    uint32_t nshards = r->engine->num_shards;
    struct shard_engine *engine = r->engine;

    s->shard_owner = my_id;
    s->is_multi = true;
    s->parent_cmd_id = MSG_CMD_MGET_PART;
    s->multi_parts = nkeys;
    s->multi_replies =
        slab_obj_calloc(r->shard->pool, nkeys, sizeof(uint8_t *));
    s->multi_reply_lens =
        slab_obj_calloc(r->shard->pool, nkeys, sizeof(uint32_t));
    s->multi_kv_objs = slab_obj_calloc(r->shard->pool, nkeys, sizeof(void *));
    s->multi_owners = slab_obj_calloc(r->shard->pool, nkeys, sizeof(uint8_t));

    /* RESP array header: *<N>\r\n */
    char hdr[32];
    int hl = snprintf(hdr, sizeof(hdr), "*%u\r\n", nkeys);
    s->reply_data = slab_obj_alloc(r->shard->pool, (size_t)hl);
    if (s->reply_data) {
      memcpy(s->reply_data, hdr, (size_t)hl);
      s->reply_len = (uint32_t)hl;
    }

    /* ── Phase 1: Send MGET_PART to each remote shard ───────────── */
    uint32_t remote_count = 0;
    uint32_t remote_wake_mask = 0;

    for (uint32_t i = 0; i < nkeys; i++) {
      const char *pk = p->argv[i + 1];
      size_t pkl = p->arglen[i + 1];
      uint32_t kowner = shard_for_key(pk, pkl, nshards);

      if (kowner == my_id) {
        struct spsc_message msg = {
            .op = MSG_PACK_OP(MSG_SYS_REQ, MSG_CMD_MGET_PART),
            .shard_owner = (uint8_t)my_id,
            .u.key_part_req = {
                .key_ptr = (void *)pk,
                .conn_ptr = c,
                .req_nb = c->rbuf_nb,
                .key_len = (uint32_t)pkl,
                .pipeline_idx = pipeline_seq,
                .sub_idx = i,
            }};
#if FREAKV_PROFILE || FREAKV_MIXED_PROFILE
        msg.sent_cycles = PROF_NOW();
#endif
        enum proxy_exec_result exec_rc = proxy_exec(r->shard, &msg, now);
        if (exec_rc == PROXY_EXEC_DONE) {
          s->multi_replies[i] = (uint8_t *)msg.u.reply.reply_buf;
          s->multi_reply_lens[i] = msg.u.reply.reply_len;
          if (s->multi_kv_objs)
            s->multi_kv_objs[i] = msg.u.reply.kv_obj_ptr;
          if (s->multi_owners)
            s->multi_owners[i] = (uint8_t)my_id;
          s->multi_replied++;
        }
      } else {
        struct spsc_queue *q = &engine->queues[my_id * nshards + kowner];
        struct spsc_message msg = {
            .op = MSG_PACK_OP(MSG_SYS_REQ, MSG_CMD_MGET_PART),
            .shard_owner = (uint8_t)my_id,
            .u.key_part_req = {
                .key_ptr = (void *)pk,
                .conn_ptr = c,
                .req_nb = c->rbuf_nb,
                .key_len = (uint32_t)pkl,
                .pipeline_idx = pipeline_seq,
                .sub_idx = i,
            }};
#if FREAKV_PROFILE || FREAKV_MIXED_PROFILE
        msg.sent_cycles = PROF_NOW();
#endif
        if (msg.u.key_part_req.req_nb)
          net_buf_ref(msg.u.key_part_req.req_nb);
        spsc_queue_push(q, &msg);
        remote_wake_mask |= (1U << kowner);
        remote_count++;
      }
    }

    /* Wake remote shards */
    if (remote_wake_mask && engine->wake_fds) {
      uint32_t mask = remote_wake_mask;
      uint32_t sid = 0;
      while (mask) {
        if ((mask & 1) && engine->wake_fds[sid] >= 0) {
          uint64_t one = 1;
          if (write(engine->wake_fds[sid], &one, sizeof(one)) < 0) {
          }
        }
        mask >>= 1;
        sid++;
      }
    }

    /* ── Phase 2: Wait synchronously for all remote replies ────── */
    uint32_t remote_received = 0;

    while (remote_received < remote_count) {
      for (uint32_t sender = 0; sender < nshards; sender++) {
        if (sender == my_id)
          continue;
        struct spsc_queue *q = r->shard->inboxes[sender];
        if (!q)
          continue;

        struct spsc_message msg;
        while (spsc_queue_pop(q, &msg)) {
          uint8_t sys_op = MSG_GET_SYS(msg.op);
          uint16_t cmd_id = MSG_GET_CMD(msg.op);

          if (sys_op == MSG_SYS_RELEASE) {
            if (msg.u.release.kv_obj_ptr)
              shard_obj_unref(r->shard,
                              (struct kv_obj *)msg.u.release.kv_obj_ptr);
            if (msg.u.release.reply_buf)
              slab_obj_free(r->shard->pool, msg.u.release.reply_buf);
            continue;
          }

          if (sys_op == MSG_SYS_REPLY) {
            if (cmd_id == MSG_CMD_MGET_PART && msg.u.reply.conn_ptr == c &&
                msg.u.reply.pipeline_idx == pipeline_seq) {
              /* This is one of our MGET replies */
              uint32_t sidx = msg.u.reply.sub_idx;
              if (sidx < nkeys) {
                s->multi_replies[sidx] = (uint8_t *)msg.u.reply.reply_buf;
                s->multi_reply_lens[sidx] = msg.u.reply.reply_len;
                if (s->multi_kv_objs)
                  s->multi_kv_objs[sidx] = msg.u.reply.kv_obj_ptr;
                if (s->multi_owners)
                  s->multi_owners[sidx] = msg.shard_owner;
                s->multi_replied++;
              }
              remote_received++;
              if (msg.u.reply.req_nb)
                net_buf_unref(r->shard->pool, msg.u.reply.req_nb);
              continue;
            }
            /* Non-MGET reply — forward normally */
            net_reactor_proxy_reply_callback(r, &msg);
            continue;
          }

          /* Normal REQ during lock hold — process it */
          if (sys_op == MSG_SYS_REQ) {
            uint64_t cur = net_time_ms_get();
            enum proxy_exec_result exec_rc = proxy_exec(r->shard, &msg, cur);
            r->shard->cross_shard_received++;
            if (exec_rc == PROXY_EXEC_DEFERRED) {
              uint16_t deferred_cmd = MSG_GET_CMD(msg.op);
              struct net_buf *req_nb =
                  (deferred_cmd == MSG_CMD_MGET_PART ||
                   deferred_cmd == MSG_CMD_DEL_PART)
                      ? msg.u.key_part_req.req_nb
                      : msg.u.regular_req.req_nb;
              if (req_nb)
                net_buf_unref(r->shard->pool, req_nb);
              continue;
            }
            r->shard->ops_completed++;

            struct spsc_queue *rq =
                &engine->queues[my_id * nshards + msg.shard_owner];
            struct spsc_message reply = {
                .op = MSG_PACK_OP(MSG_SYS_REPLY, MSG_GET_CMD(msg.op)),
                .shard_owner = (uint8_t)my_id,
                .u.reply = msg.u.reply};
            spsc_queue_push(rq, &reply);
            if (engine->wake_fds && engine->wake_fds[msg.shard_owner] >= 0) {
              uint64_t one = 1;
              if (write(engine->wake_fds[msg.shard_owner], &one, sizeof(one)) <
                  0) {
              }
            }
          }
        }
      }
      if (remote_received < remote_count)
        sched_yield();
    }

    if (s->multi_replied >= s->multi_parts)
      s->state = PSLOT_DONE;
    return 1;
  }

  if (strcmp(cmd, "MSET") == 0) {
    if (p->argc_got < 3 || (p->argc_got % 2) == 0) {
      resp_reply_err(r, c, "wrong args for MSET");
      return 0;
    }

    /* Async MSET 2PC dispatch.
     *
     * IMPORTANT ordering:
     *  1. slot is already zero-initialized by net_pipeline_enqueue (called
     *     by the caller before resp_dispatch_proxy).
     *  2. We set kv_obj_ptr = stat AFTER mset_coordinator_dispatch returns,
     *     so it is never overwritten by the caller's ret==0 path (we return 1).
     *  3. Return 1 (PSLOT_PENDING) so the caller does NOT overwrite s->state.
     */
    s->state = PSLOT_PENDING;
    net_buf_ref(c->rbuf_nb);
    s->req_nb = c->rbuf_nb;

    int rc = mset_coordinator_dispatch(r, c, p, pipeline_seq);
    if (rc < 0) {
      s->state = PSLOT_EMPTY;
      net_buf_unref(c->allocator, c->rbuf_nb);
      s->req_nb = NULL;
      resp_reply_err(r, c, "OOM");
      return 0;
    }

    /* BUG-FIX: stat pointer is set inside mset_coordinator_dispatch and
     * stored via ps->kv_obj_ptr = stat there. But because we return 1,
     * the caller's "ret==0 → s->state = PSLOT_LOCAL" branch is skipped,
     * so kv_obj_ptr is NOT overwritten. Verify here for safety. */
    /* Async MSET dispatched */

    return rc == 1 ? 3 : 1;  /* 3: connection-level pending */
  }

  if (strcmp(cmd, "DEL") == 0) {
    uint32_t nkeys = (uint32_t)p->argc_got - 1;
    s->shard_owner = r->shard->id;
    s->is_multi = true;
    s->parent_cmd_id = MSG_CMD_DEL_PART;
    s->multi_parts = nkeys;
    s->multi_replies =
        slab_obj_calloc(r->shard->pool, nkeys, sizeof(uint8_t *));
    s->multi_reply_lens =
        slab_obj_calloc(r->shard->pool, nkeys, sizeof(uint32_t));
    s->multi_kv_objs = NULL;
    s->multi_owners = slab_obj_calloc(r->shard->pool, nkeys, sizeof(uint8_t));

    for (uint32_t i = 0; i < nkeys; i++) {
      const char *pk = p->argv[i + 1];
      size_t pkl = p->arglen[i + 1];
      uint32_t kowner = shard_for_key(pk, pkl, r->engine->num_shards);

      if (kowner == r->shard->id) {
        struct spsc_message msg = {
            .op = MSG_PACK_OP(MSG_SYS_REQ, MSG_CMD_DEL_PART),
            .shard_owner = (uint8_t)r->shard->id,
            .u.key_part_req = {
                .key_ptr = (void *)pk,
                .conn_ptr = c,
                .req_nb = c->rbuf_nb,
                .key_len = (uint32_t)pkl,
                .pipeline_idx = pipeline_seq,
                .sub_idx = i,
            }};
        enum proxy_exec_result exec_rc = proxy_exec(r->shard, &msg, now);
        if (exec_rc == PROXY_EXEC_DONE) {
          s->multi_replies[i] = (uint8_t *)msg.u.reply.reply_buf;
          s->multi_reply_lens[i] = msg.u.reply.reply_len;
          s->multi_owners[i] = (uint8_t)r->shard->id;
          s->multi_replied++;
        }
      } else {
        struct spsc_queue *q =
            &r->engine->queues[r->shard->id * r->engine->num_shards + kowner];
        struct spsc_message msg = {
            .op = MSG_PACK_OP(MSG_SYS_REQ, MSG_CMD_DEL_PART),
            .shard_owner = (uint8_t)r->shard->id,
            .u.key_part_req = {
                .key_ptr = (void *)pk,
                .conn_ptr = c,
                .req_nb = c->rbuf_nb,
                .key_len = (uint32_t)pkl,
                .pipeline_idx = pipeline_seq,
                .sub_idx = i,
            }};
        if (msg.u.key_part_req.req_nb)
          net_buf_ref(msg.u.key_part_req.req_nb);
        /* Spin until space is available. The queue is large (SPSC_CAPACITY
         * slots) and rarely fills under normal load. If the target shard stalls
         * (eviction, snapshot I/O), this busy-wait will add latency to this
         * shard's reactor.
         * TODO: consider bounded retry with backpressure to the client (e.g.
         * -ERR BUSY) if the queue remains full after N yields. */
        spsc_queue_push(q, &msg);
        r->proxy_wake_mask |= (1U << kowner);
      }
    }
    if (s->multi_replied >= s->multi_parts)
      s->state = PSLOT_DONE;
    return 1;
  }

  if (owner == r->shard->id)
    return proxy_exec_local_key(r, c, cmd_op, pipeline_seq, p, s, now);

  s->shard_owner = (uint8_t)owner;
  /* Copy pre-parsed arguments to slot for the target shard */
  s->cmd.argc = (uint32_t)p->argc_got;
  s->cmd.argv =
      (char **)slab_obj_alloc(r->shard->pool, s->cmd.argc * sizeof(char *));
  s->cmd.arglen =
      (size_t *)slab_obj_alloc(r->shard->pool, s->cmd.argc * sizeof(size_t));
  if (!s->cmd.argv || !s->cmd.arglen) {
    if (s->cmd.argv)
      slab_obj_free(r->shard->pool, s->cmd.argv);
    if (s->cmd.arglen)
      slab_obj_free(r->shard->pool, s->cmd.arglen);
    s->cmd.argv = NULL;
    s->cmd.arglen = NULL;
    resp_reply_err(r, c, "OOM");
    return 0;
  }

  char **dst_argv = s->cmd.argv;
  size_t *dst_arglen = s->cmd.arglen;
  for (uint32_t i = 0; i < s->cmd.argc; i++) {
    dst_argv[i] = p->argv[i];
    dst_arglen[i] = p->arglen[i];
  }

  if (!proxy_send_req(r, c, owner, cmd_op, pipeline_seq, &s->cmd)) {
    s->state = PSLOT_EMPTY;
    net_buf_unref(c->allocator, c->rbuf_nb);
    s->req_nb = NULL;
    resp_reply_err(r, c, "shard queue full");
    return 0;
  }
  return 1;
}

/* ── Main read + parse handler ───────────────────────────────────────── */

static void handle_read_resp(struct reactor *r, struct net_conn *c) {
  struct resp_parser *p = &c->resp;
  uint64_t batch_now_ms = net_time_ms_get();

  /* 1. Read loop — drain socket until EAGAIN */
  for (;;) {
    uint8_t *in = c->rbuf_nb->data + c->rbuf_len;
    size_t av = c->rbuf_nb->cap - c->rbuf_len;
    if (av < 1024) {
      if (net_rbuf_grow(c, 4096) < 0) {
        net_reactor_mark_dead(r, c);
        return;
      }
      in = c->rbuf_nb->data + c->rbuf_len;
      av = c->rbuf_nb->cap - c->rbuf_len;
    }
    ssize_t n = read(c->fd, in, av);
    if (n < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK)
        break;
      net_reactor_mark_dead(r, c);
      return;
    }
    if (n == 0) {
      net_conn_mark_closing(r, c);
      break;
    }
    c->rbuf_len += (size_t)n;
  }

  /* 2. Parse loop */
  uint8_t *buf = c->rbuf_nb->data;
  while (p->rbuf_parsed < c->rbuf_len) {
    size_t avail = c->rbuf_len - p->rbuf_parsed;
    if (avail == 0)
      break;

    if (p->parse_state == RESP_PARSE_ARRAY_LEN) {
      /* Redis dictates that we must ignore empty lines between requests */
      if (avail >= 2 && buf[p->rbuf_parsed] == '\r' &&
          buf[p->rbuf_parsed + 1] == '\n') {
        p->rbuf_parsed += 2;
        continue;
      } else if (buf[p->rbuf_parsed] == '\r') {
        break; /* wait for \n */
      }

      p->cmd_start = p->rbuf_parsed; /* Mark start of new command */
      if (buf[p->rbuf_parsed] != '*') {
        /* Basic inline support for PING */
        if (avail >= 4 &&
            strncasecmp((const char *)buf + p->rbuf_parsed, "PING", 4) == 0) {
          /* Inline PING: write +PONG directly into wbuf.
           * We also need a dummy pipeline slot so Master Flush
           * knows this connection has data to send. */
          uint32_t seq = net_pipeline_enqueue(&c->proxy, r->shard->pool);
          if (seq != UINT32_MAX) {
            struct net_pipeline_slot *ps =
                &c->proxy.slots[seq & (c->proxy.cap - 1)];
            ps->state = PSLOT_LOCAL;
            size_t w0 = c->wbuf_len;
            resp_reply_pong(r, c);
            ps->reply_len = (uint32_t)(c->wbuf_len - w0);
            if (ps->reply_len > 0) {
              ps->reply_data = slab_obj_alloc(r->shard->pool, ps->reply_len);
              if (ps->reply_data)
                memcpy(ps->reply_data, c->wbuf + w0, ps->reply_len);
              ps->reply_type = REPLY_TYPE_BUF;
            }
            c->wbuf_len = w0; /* rollback direct write — slot owns it */
          } else {
            resp_reply_pong(r, c); /* fallback: write directly if no slot */
          }
          p->rbuf_parsed += 4;
          while (p->rbuf_parsed < c->rbuf_len &&
                 (buf[p->rbuf_parsed] == '\r' || buf[p->rbuf_parsed] == '\n'))
            p->rbuf_parsed++;
          net_pipeline_try_flush(r, c);
          continue;
        }
        /* Invalid protocol: close silently */
        net_reactor_mark_dead(r, c);
        return;
      }
      const uint8_t *pos = buf + p->rbuf_parsed + 1;
      const uint8_t *end = buf + c->rbuf_len - 1;
      int val = 0;
      while (pos < end && *pos >= '0') {
        val = val * 10 + (*pos - '0');
        pos++;
      }
      if (pos >= end || pos[0] != '\r' || pos[1] != '\n')
        break;
      p->rbuf_parsed = (size_t)(pos + 2 - buf);
      p->argc_expected = val;
      p->argc_got = 0;
      if (!resp_argv_ensure(p, val)) {
        net_reactor_mark_dead(r, c);
        return;
      }
      p->parse_state = RESP_PARSE_BULK_LEN;
      continue;
    }
    if (p->parse_state == RESP_PARSE_BULK_LEN) {
      if (buf[p->rbuf_parsed] != '$') {
        net_reactor_mark_dead(r, c);
        return;
      }
      const uint8_t *pos = buf + p->rbuf_parsed + 1;
      const uint8_t *end = buf + c->rbuf_len - 1;
      int val = 0;
      while (pos < end && *pos >= '0') {
        val = val * 10 + (*pos - '0');
        pos++;
      }
      if (pos >= end || pos[0] != '\r' || pos[1] != '\n')
        break;
      p->rbuf_parsed = (size_t)(pos + 2 - buf);
      p->bulk_len = val;
      p->parse_state = RESP_PARSE_BULK_DATA;
      continue;
    }
    if (p->parse_state == RESP_PARSE_BULK_DATA) {
      size_t need = (size_t)p->bulk_len + 2;
      if (avail < need)
        break;
      p->argv[p->argc_got] = (char *)(buf + p->rbuf_parsed);
      p->arglen[p->argc_got] = (size_t)p->bulk_len;
      p->argc_got++;
      p->rbuf_parsed += need;
      if (p->argc_got >= p->argc_expected) {
        /* Command complete — enqueue into circular queue */
        uint32_t seq = net_pipeline_enqueue(&c->proxy, r->shard->pool);
        if (seq == UINT32_MAX) {
          net_reactor_mark_dead(r, c);
          return;
        }

        struct net_pipeline_slot *s = &c->proxy.slots[seq & (c->proxy.cap - 1)];
        size_t w0 = c->wbuf_len;

        int ret = resp_dispatch_proxy(r, c, seq, batch_now_ms);

        if (ret == 0) {
          /* Local command — rollback wbuf, save reply into slot */
          s->state = PSLOT_LOCAL;
          s->shard_owner = r->shard->id;
          size_t reply_len = c->wbuf_len - w0;
          if (reply_len > 0) {
            s->reply_len = (uint32_t)reply_len;
            s->reply_data = slab_obj_alloc(r->shard->pool, s->reply_len);
            if (s->reply_data)
              memcpy(s->reply_data, c->wbuf + w0, s->reply_len);
            s->reply_type = REPLY_TYPE_BUF;
            c->wbuf_len = w0; /* rollback only if we copied */
          }
        } else if (ret == 2) {
          /* Fast-path local command (zero-malloc proxy_exec) finished */
        } else if (ret == 3) {
          resp_parser_reset(c);
          net_reactor_mark_dirty(r, c);
          return;
        } else if (ret < 0) {
          net_reactor_mark_dead(r, c);
          return;
        }
        /* ret == 1: remote proxy, slot is PSLOT_PENDING */

        resp_parser_reset(c);
        buf = c->rbuf_nb->data; /* buf may change after reset */
        continue;
      }
      p->parse_state = RESP_PARSE_BULK_LEN;
      continue;
    }
  }

  if (c->state != CONN_DEAD) {
    net_pipeline_try_flush(r, c);
    if (c->proxy.out != c->proxy.in || c->wbuf_len > 0) {
      net_reactor_mark_dirty(r, c);
    }
  }

  /* 5. Batch wake up shards that received cross-shard traffic */
  if (r->proxy_wake_mask) {
    uint32_t mask = r->proxy_wake_mask;
    r->proxy_wake_mask = 0;
    if (r->engine->wake_fds) {
      uint32_t s = 0;
      while (mask) {
        if (mask & 1) {
          if (r->engine->wake_fds[s] >= 0) {
            uint64_t one = 1;
            if (write(r->engine->wake_fds[s], &one, sizeof(one)) <
                0) { /* handle warn silently */
            }
          }
        }
        mask >>= 1;
        s++;
      }
    }
  }
}

#endif /* RESP_HANDLER_H */
