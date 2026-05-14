#include "mset_exec.h"
#include "lock_manager.h"
#include "txn.h"

#include "../core/shard.h"
#include "../core/object.h"
#include "../core/message.h"
#include "../core/routing.h"
#include "../core/snapshot.h"
#include "../hashtable/htable.h"
#include "../mem/mem_api.h"
#include "../mem/mem_pool.h"
#include "../net/conn.h"
#include "../net/reactor.h"

#include <sched.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* ── Forward declarations ────────────────────────────────────────────── */

extern enum proxy_exec_result proxy_exec(struct shard *s,
                                         struct spsc_message *msg,
                                         uint64_t cur);

/* ── Low-level helpers ───────────────────────────────────────────────── */

static inline void send_to_shard(struct shard_engine *engine, uint32_t from,
                                 uint32_t to, struct spsc_message *msg) {
  msg->sent_cycles = cycles_now();
  struct spsc_queue *q = &engine->queues[from * engine->num_shards + to];
  while (!spsc_queue_push(q, msg))
    sched_yield();
  if (engine->wake_fds && engine->wake_fds[to] >= 0) {
    uint64_t one = 1;
    (void)write(engine->wake_fds[to], &one, sizeof(one));
  }
}

static inline uint64_t now_ms(void) {
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return (uint64_t)ts.tv_sec * 1000ULL + (uint64_t)ts.tv_nsec / 1000000ULL;
}

static bool mset_debug_enabled(void) {
  static int cached = -1;
  if (cached < 0) {
    const char *v = getenv("MSET_DEBUG_STUCK");
    cached = (v && v[0] && strcmp(v, "0") != 0) ? 1 : 0;
  }
  return cached != 0;
}

static bool mixed_profile_enabled(void) {
  static int cached = -1;
  if (cached < 0) {
    const char *v = getenv("MSET_MIXED_PROFILE");
    cached = (v && v[0] && strcmp(v, "0") != 0) ? 1 : 0;
  }
  return cached != 0;
}

static uint64_t mixed_profile_slow_cycles(void) {
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

static bool mixed_is_set_cmd(uint16_t cmd) {
  return cmd == MSG_CMD_SET || cmd == MSG_CMD_SET_NX || cmd == MSG_CMD_SET_XX;
}

static uint64_t mixed_early_commit_count[MAX_SHARDS];

static const char *mixed_cmd_name(uint16_t cmd) {
  switch (cmd) {
  case MSG_CMD_GET:
    return "GET";
  case MSG_CMD_SET:
    return "SET";
  case MSG_CMD_SET_NX:
    return "SET_NX";
  case MSG_CMD_SET_XX:
    return "SET_XX";
  case MSG_CMD_DEL:
    return "DEL";
  case MSG_CMD_MGET_PART:
    return "MGET_PART";
  case MSG_CMD_DEL_PART:
    return "DEL_PART";
  default:
    return "OTHER";
  }
}

static bool mset_debug_trace_enabled(void) {
  static int cached = -1;
  if (cached < 0) {
    const char *v = getenv("MSET_DEBUG_TRACE");
    cached = (v && v[0] && strcmp(v, "0") != 0) ? 1 : 0;
  }
  return cached != 0;
}

static uint64_t mset_debug_stuck_ms(void) {
  static uint64_t cached = 0;
  if (!cached) {
    const char *v = getenv("MSET_DEBUG_STUCK_MS");
    cached = v && v[0] ? strtoull(v, NULL, 10) : 1000;
    if (!cached)
      cached = 1000;
  }
  return cached;
}

static uint64_t mset_debug_slow_ms(void) {
  static uint64_t cached = 0;
  if (!cached) {
    const char *v = getenv("MSET_DEBUG_SLOW_MS");
    cached = v && v[0] ? strtoull(v, NULL, 10) : 100;
    if (!cached)
      cached = 100;
  }
  return cached;
}

static uint64_t mset_stat_age_ms(struct mset_stat *stat, uint64_t cur_ms) {
  return stat && stat->start_ms && cur_ms >= stat->start_ms
             ? cur_ms - stat->start_ms
             : 0;
}

static uint32_t mset_debug_count_local_heads(struct mset_stat *stat,
                                             uint32_t shard_id) {
  uint32_t n = 0;
  struct cmd_info *cmd = stat->shard_heads[shard_id];
  while (cmd && n < 1000000) {
    n++;
    cmd = cmd->next_in_mset;
  }
  return n;
}

static void mset_debug_dump_stat(struct shard *shard, struct mset_stat *stat,
                                 uint64_t cur_ms, const char *where) {
  if (!mset_debug_enabled() || !stat)
    return;

  uint64_t age_ms = stat->start_ms ? cur_ms - stat->start_ms : 0;
  uint64_t threshold = mset_debug_stuck_ms();
  if (age_ms < threshold || cur_ms - stat->last_debug_ms < threshold)
    return;

  stat->last_debug_ms = cur_ms;
  uint32_t ack = atomic_load_explicit(&stat->ack, memory_order_acquire);
  uint32_t fin_ack = atomic_load_explicit(&stat->fin_ack, memory_order_acquire);
  uint32_t local_heads = mset_debug_count_local_heads(stat, shard->id);
  uint32_t heads_total = 0;
  char heads_buf[256];
  size_t off = 0;
  heads_buf[0] = '\0';
  for (uint32_t sid = 0; sid < shard->engine->num_shards; sid++) {
    uint32_t n = mset_debug_count_local_heads(stat, sid);
    heads_total += n;
    if (off < sizeof(heads_buf)) {
      int wrote = snprintf(heads_buf + off, sizeof(heads_buf) - off, "%s%u:%u",
                           sid ? "," : "", sid, n);
      if (wrote > 0)
        off += (size_t)wrote;
    }
  }

  fprintf(stderr,
          "[MSET_STUCK] ts_ms=%lu where=%s shard=%u stat=%p age_ms=%lu coord=%u "
          "pidx=%u total=%u ack=%u fin_ack=%u inflight=%u local_heads=%u "
          "heads_total=%u heads_by_shard=%s conn=%p gen=%lu lm_live=%u "
          "lm_deleted=%u\n",
          (unsigned long)cur_ms, where, shard->id, (void *)stat,
          (unsigned long)age_ms,
          stat->coordinator_id, stat->pipeline_idx, stat->total, ack, fin_ack,
          shard->mset_inflight, local_heads, heads_total, heads_buf,
          stat->conn_ptr,
          (unsigned long)stat->conn_generation, shard->lm.count,
          shard->lm.deleted_count);

  uint32_t printed = 0;
  for (struct cmd_info *cmd = stat->shard_heads[shard->id];
       cmd && printed < 20; cmd = cmd->next_in_mset, printed++) {
    struct lock_entry *le = cmd->le;
    fprintf(stderr,
            "[MSET_STUCK_CMD] ts_ms=%lu shard=%u stat=%p cmd=%p sub=%u total_acks=%u "
            "le=%p le_holder=%p is_holder=%d old=%p new=%p le_obj=%p "
            "hash56=%lx qh=%p qt=%p\n",
            (unsigned long)cur_ms, shard->id, (void *)stat, (void *)cmd,
            cmd->sub_idx,
            cmd->total_acks, (void *)le, le ? (void *)le->holder : NULL,
            le && le->holder == cmd, (void *)cmd->old_obj,
            (void *)cmd->new_obj, le ? (void *)le->obj_ptr : NULL,
            (unsigned long)cmd->hash56, le ? (void *)le->queue_head : NULL,
            le ? (void *)le->queue_tail : NULL);
  }
}

static void mset_debug_backpressure(struct reactor *r, struct net_conn *c,
                                    uint64_t cur_ms) {
  struct shard *shard = r->shard;
  if (!mset_debug_enabled())
    return;

  uint32_t pending_slots = 0;
  uint32_t pending_mset_slots = 0;
  uint32_t pending_mset_with_stat = 0;
  uint32_t first_seq = 0;
  enum net_pipeline_state first_state = PSLOT_EMPTY;
  uint16_t first_cmd = 0;
  struct mset_stat *first_stat = NULL;
  uint64_t first_age = 0;

  if (c && c->proxy.slots && c->proxy.out != c->proxy.in) {
    struct net_pipeline_slot *first =
        &c->proxy.slots[c->proxy.out & (c->proxy.cap - 1)];
    first_seq = c->proxy.out;
    first_state = first->state;
    first_cmd = first->parent_cmd_id;
    if (first->parent_cmd_id == MSG_CMD_MSET_PART && first->kv_obj_ptr) {
      first_stat = (struct mset_stat *)first->kv_obj_ptr;
      first_age = mset_stat_age_ms(first_stat, cur_ms);
    } else if (first->parent_cmd_id == MSG_CMD_MSET_PART &&
               first->debug_mset_stat) {
      first_stat = (struct mset_stat *)first->debug_mset_stat;
      first_age = first->debug_mset_start_ms &&
                          cur_ms >= first->debug_mset_start_ms
                      ? cur_ms - first->debug_mset_start_ms
                      : 0;
    }

    for (uint32_t seq = c->proxy.out; seq != c->proxy.in; seq++) {
      struct net_pipeline_slot *slot =
          &c->proxy.slots[seq & (c->proxy.cap - 1)];
      if (slot->state == PSLOT_PENDING)
        pending_slots++;
      if (slot->state == PSLOT_PENDING &&
          slot->parent_cmd_id == MSG_CMD_MSET_PART) {
        pending_mset_slots++;
        if (slot->kv_obj_ptr || slot->debug_mset_stat)
          pending_mset_with_stat++;
      }
    }
  }

  if (cur_ms - shard->mset_debug_last_ms >= 1000) {
    shard->mset_debug_last_ms = cur_ms;
    fprintf(stderr,
            "[MSET_SNAPSHOT] ts_ms=%lu shard=%u inflight=%u max=%u dispatch=%lu "
            "prepare=%lu ack=%lu fin=%lu fin_ack=%lu drain=%lu e2e=%lu "
            "ops=%lu xrecv=%lu lm_live=%u lm_deleted=%u entries=%zu "
            "pipe_out=%u pipe_in=%u pending=%u pending_mset=%u "
            "pending_mset_stat=%u first_seq=%u first_state=%u first_cmd=%u "
            "first_stat=%p first_age_ms=%lu\n",
            (unsigned long)cur_ms, shard->id, shard->mset_inflight,
            shard->mset_max_concurrent,
            shard->prof.mset_dispatch.count, shard->prof.mset_prepare.count,
            shard->prof.mset_ack.count, shard->prof.mset_fin.count,
            shard->prof.mset_fin_ack.count, shard->prof.mset_drain.count,
            shard->prof.mset_total_e2e.count, shard->ops_completed,
            shard->cross_shard_received, shard->lm.count,
            shard->lm.deleted_count, shard->table ? shard->table->used : 0,
            c ? c->proxy.out : 0, c ? c->proxy.in : 0, pending_slots,
            pending_mset_slots, pending_mset_with_stat, first_seq,
            (uint32_t)first_state, first_cmd, (void *)first_stat,
            (unsigned long)first_age);
  }

  if (!c || !c->proxy.slots)
    return;

  for (uint32_t seq = c->proxy.out; seq != c->proxy.in; seq++) {
    struct net_pipeline_slot *slot = &c->proxy.slots[seq & (c->proxy.cap - 1)];
    if (slot->state != PSLOT_PENDING ||
        slot->parent_cmd_id != MSG_CMD_MSET_PART ||
        (!slot->kv_obj_ptr && !slot->debug_mset_stat))
      continue;
    struct mset_stat *stat = slot->kv_obj_ptr
                                 ? (struct mset_stat *)slot->kv_obj_ptr
                                 : (struct mset_stat *)slot->debug_mset_stat;
    if (!stat)
      continue;
    mset_debug_dump_stat(shard, stat, cur_ms,
                         "backpressure");
  }
}

static void mset_debug_progress(struct shard *shard, uint64_t cur_ms,
                                const char *where) {
  if (!mset_debug_enabled())
    return;

  uint64_t dispatch = shard->prof.mset_dispatch.count;
  uint64_t e2e = shard->prof.mset_total_e2e.count;
  bool complete = dispatch > 0 && dispatch == e2e && shard->mset_inflight == 0;
  bool changed = dispatch != shard->mset_debug_last_dispatch ||
                 e2e != shard->mset_debug_last_e2e;
  bool due = cur_ms - shard->mset_debug_progress_last_ms >= 1000;

  if ((changed && due) ||
      (complete && !shard->mset_debug_all_complete_logged)) {
    fprintf(stderr,
            "[MSET_PROGRESS] ts_ms=%lu where=%s shard=%u dispatch=%lu e2e=%lu "
            "inflight=%u prepare=%lu ack=%lu fin=%lu fin_ack=%lu "
            "lm_live=%u lm_deleted=%u entries=%zu all_complete=%d\n",
            (unsigned long)cur_ms, where, shard->id, dispatch, e2e,
            shard->mset_inflight,
            shard->prof.mset_prepare.count, shard->prof.mset_ack.count,
            shard->prof.mset_fin.count, shard->prof.mset_fin_ack.count,
            shard->lm.count, shard->lm.deleted_count,
            shard->table ? shard->table->used : 0,
            complete);
    shard->mset_debug_progress_last_ms = cur_ms;
    shard->mset_debug_last_dispatch = dispatch;
    shard->mset_debug_last_e2e = e2e;
    shard->mset_debug_all_complete_logged = complete;
  } else if (!complete) {
    shard->mset_debug_all_complete_logged = false;
  }
}

static void mset_debug_reply_ready(struct shard *shard, struct net_conn *c,
                                   struct mset_stat *stat,
                                   const char *where) {
  if (!c || !stat)
    return;
  uint64_t cur_ms = now_ms();
  if (c->generation == stat->conn_generation && c->proxy.slots) {
    struct net_pipeline_slot *ps =
        &c->proxy.slots[stat->pipeline_idx & (c->proxy.cap - 1)];
    ps->debug_mset_stat = stat;
    ps->debug_mset_start_ms = stat->start_ms;
    ps->debug_mset_reply_ready_ms = cur_ms;
  }
  if (!mset_debug_enabled())
    return;
  uint64_t age_ms = mset_stat_age_ms(stat, cur_ms);
  if (age_ms < mset_debug_slow_ms())
    return;
  fprintf(stderr,
          "[MSET_REPLY_READY] ts_ms=%lu where=%s shard=%u conn=%p fd=%d "
          "stat=%p age_ms=%lu pidx=%u total=%u ack=%u fin_ack=%u "
          "pipe_out=%u pipe_in=%u inflight=%u\n",
          (unsigned long)cur_ms, where, shard->id, (void *)c, c->fd,
          (void *)stat, (unsigned long)age_ms, stat->pipeline_idx, stat->total,
          atomic_load_explicit(&stat->ack, memory_order_acquire),
          atomic_load_explicit(&stat->fin_ack, memory_order_acquire),
          c->proxy.out, c->proxy.in, shard->mset_inflight);
}

static bool mset_set_ok_reply(struct shard *shard, struct mset_stat *stat,
                              const char *reason) {
  struct net_conn *c = (struct net_conn *)stat->conn_ptr;
  if (!c || c->generation != stat->conn_generation)
    return false;

  struct net_pipeline_slot *ps =
      &c->proxy.slots[stat->pipeline_idx & (c->proxy.cap - 1)];
  ps->state = PSLOT_LOCAL;
  ps->reply_type = REPLY_TYPE_BUF_STATIC;
  ps->reply_data = (uint8_t *)"+OK\r\n";
  ps->reply_len = 5;
  ps->kv_obj_ptr = NULL;
  mset_debug_reply_ready(shard, c, stat, reason);

  if (shard->mark_conn_dirty_cb)
    shard->mark_conn_dirty_cb(shard->mark_conn_dirty_ctx, c, reason);
  return true;
}

static void mset_complete_regular_reply(struct shard_engine *engine,
                                        struct shard *shard,
                                        struct cmd_info *ci) {
  uint16_t cmd_op = MSG_GET_CMD(ci->msg.op);

  if (ci->origin_shard == shard->id) {
    struct net_conn *c = (struct net_conn *)ci->msg.conn_ptr;
    if (!c || c->generation != ci->msg.conn_generation) {
      if (ci->msg.kv_obj_ptr)
        shard_obj_unref(shard, (struct kv_obj *)ci->msg.kv_obj_ptr);
      if (ci->msg.reply_type == REPLY_TYPE_BUF && ci->msg.reply_buf)
        slab_obj_free(shard->pool, ci->msg.reply_buf);
      return;
    }

    struct net_pipeline_slot *slot =
        &c->proxy.slots[ci->msg.pipeline_idx & (c->proxy.cap - 1)];
    if (slot->is_multi) {
      uint32_t sidx = ci->msg.sub_idx;
      if (sidx < slot->multi_parts) {
        slot->multi_replies[sidx] = (uint8_t *)ci->msg.reply_buf;
        slot->multi_reply_lens[sidx] = ci->msg.reply_len;
        if (slot->multi_kv_objs)
          slot->multi_kv_objs[sidx] = ci->msg.kv_obj_ptr;
        if (slot->multi_owners)
          slot->multi_owners[sidx] = (uint8_t)shard->id;
        slot->multi_replied++;
        if (slot->multi_replied >= slot->multi_parts)
          slot->state = PSLOT_DONE;
      }
    } else {
      slot->reply_type = ci->msg.reply_type;
      slot->reply_int = ci->msg.reply_int;
      slot->reply_data = (uint8_t *)ci->msg.reply_buf;
      slot->reply_len = ci->msg.reply_len;
      slot->kv_obj_ptr = ci->msg.kv_obj_ptr;
      slot->shard_owner = (uint8_t)shard->id;
      slot->state = PSLOT_DONE;
    }

    if (shard->mark_conn_dirty_cb)
      shard->mark_conn_dirty_cb(shard->mark_conn_dirty_ctx, c,
                                "deferred_regular");
    return;
  }

  struct spsc_message reply = {
      .op = MSG_PACK_OP(MSG_SYS_REPLY, cmd_op),
      .shard_owner = (uint8_t)shard->id,
      .conn_ptr = ci->msg.conn_ptr,
      .conn_generation = ci->msg.conn_generation,
      .pipeline_idx = ci->msg.pipeline_idx,
      .sub_idx = ci->msg.sub_idx,
      .kv_obj_ptr = ci->msg.kv_obj_ptr,
      .old_obj_ptr = ci->msg.old_obj_ptr,
      .reply_buf = ci->msg.reply_buf,
      .reply_len = ci->msg.reply_len,
      .reply_type = ci->msg.reply_type,
      .reply_int = ci->msg.reply_int,
      .req_nb = ci->msg.req_nb,
  };
  send_to_shard(engine, shard->id, ci->origin_shard, &reply);
  ci->req_nb = NULL; /* reply now owns the request-buffer ref */
}

static void mset_mixed_log_set_drain(struct shard *shard, struct cmd_info *ci,
                                     enum proxy_exec_result exec_rc,
                                     uint64_t wait_cycles,
                                     uint64_t exec_cycles) {
  if (!mixed_profile_enabled())
    return;

  uint16_t cmd_op = MSG_GET_CMD(ci->msg.op);
  if (!mixed_is_set_cmd(cmd_op))
    return;

  static uint64_t set_drain_count[MAX_SHARDS];
  static uint64_t set_drain_deferred_again[MAX_SHARDS];
  static uint64_t set_drain_wait_sum[MAX_SHARDS];
  static uint64_t set_drain_wait_max[MAX_SHARDS];
  static uint64_t set_drain_exec_max[MAX_SHARDS];
  static uint64_t set_drain_last_summary_ms[MAX_SHARDS];

  uint32_t sid = shard->id;
  set_drain_count[sid]++;
  set_drain_wait_sum[sid] += wait_cycles;
  if (wait_cycles > set_drain_wait_max[sid])
    set_drain_wait_max[sid] = wait_cycles;
  if (exec_cycles > set_drain_exec_max[sid])
    set_drain_exec_max[sid] = exec_cycles;
  if (exec_rc == PROXY_EXEC_DEFERRED)
    set_drain_deferred_again[sid]++;

  uint64_t slow = mixed_profile_slow_cycles();
  if (wait_cycles >= slow || exec_cycles >= slow ||
      exec_rc == PROXY_EXEC_DEFERRED) {
    fprintf(stderr,
            "[MSET_MIXED_SET_DRAIN] ts_ms=%lu shard=%u cmd=%s origin=%u "
            "pidx=%u waitcy=%lu execcy=%lu deferred_again=%d\n",
            (unsigned long)now_ms(), sid, mixed_cmd_name(cmd_op),
            ci->origin_shard, ci->msg.pipeline_idx,
            (unsigned long)wait_cycles, (unsigned long)exec_cycles,
            exec_rc == PROXY_EXEC_DEFERRED);
  }

  uint64_t cur_ms = now_ms();
  if (cur_ms - set_drain_last_summary_ms[sid] >= 1000) {
    set_drain_last_summary_ms[sid] = cur_ms;
    uint64_t count = set_drain_count[sid];
    uint64_t avg = count ? set_drain_wait_sum[sid] / count : 0;
    fprintf(stderr,
            "[MSET_MIXED_SET_DRAIN_SUMMARY] ts_ms=%lu shard=%u count=%lu "
            "deferred_again=%lu wait_avgcy=%lu wait_maxcy=%lu "
            "exec_maxcy=%lu\n",
            (unsigned long)cur_ms, sid, (unsigned long)count,
            (unsigned long)set_drain_deferred_again[sid], (unsigned long)avg,
            (unsigned long)set_drain_wait_max[sid],
            (unsigned long)set_drain_exec_max[sid]);
  }
}

static void mset_reanchor_lock_after_regular(struct shard *shard,
                                             struct lock_entry *le,
                                             struct cmd_info *ci) {
  if (!le || !ci)
    return;

  struct spsc_message *msg = &ci->msg;
  uint16_t cmd_op = MSG_GET_CMD(msg->op);
  const void *key = NULL;
  size_t klen = 0;

  if (cmd_op == MSG_CMD_MGET_PART || cmd_op == MSG_CMD_MSET_PART ||
      cmd_op == MSG_CMD_DEL_PART) {
    key = msg->key_ptr;
    klen = msg->key_len;
  } else if (msg->cmd.argc > 1) {
    key = msg->cmd.argv[1];
    klen = msg->cmd.arglen[1];
  }

  if (!key || !klen)
    return;

  struct kv_obj *cur = ht_bucket_get(shard->table, key, klen, VAL_TYPE_STRING, 0);
  if (cur) {
    le->obj_ptr = cur;
    ht_bucket_set_lock_status(shard->table, key, klen, VAL_TYPE_STRING, true);
  }
}

static void mset_debug_lifecycle(struct shard *shard, struct mset_stat *stat,
                                 const char *event, uint64_t cur_ms,
                                 uint32_t extra) {
  if (!mset_debug_enabled() || !stat)
    return;
  uint64_t age_ms = mset_stat_age_ms(stat, cur_ms);
  if (age_ms < mset_debug_slow_ms() && !mset_debug_trace_enabled())
    return;
  fprintf(stderr,
          "[MSET_LIFECYCLE] ts_ms=%lu event=%s shard=%u stat=%p age_ms=%lu "
          "coord=%u pidx=%u total=%u ack=%u fin_ack=%u inflight=%u "
          "extra=%u\n",
          (unsigned long)cur_ms, event, shard->id, (void *)stat,
          (unsigned long)age_ms, stat->coordinator_id, stat->pipeline_idx,
          stat->total, atomic_load_explicit(&stat->ack, memory_order_acquire),
          atomic_load_explicit(&stat->fin_ack, memory_order_acquire),
          shard->mset_inflight, extra);
}

static void mset_debug_print_timeline(const char *tag, struct shard *shard,
                                      struct mset_stat *stat,
                                      uint64_t cur_ms) {
  if (!mset_debug_enabled() || !stat)
    return;
  uint64_t start = stat->start_ms;
  uint64_t ack_last =
      atomic_load_explicit(&stat->dbg_ack_last_ms, memory_order_relaxed);
  uint64_t on_ack =
      atomic_load_explicit(&stat->dbg_on_ack_ms, memory_order_relaxed);
  uint64_t broadcast =
      atomic_load_explicit(&stat->dbg_broadcast_fin_ms, memory_order_relaxed);
  uint64_t run_fin =
      atomic_load_explicit(&stat->dbg_run_fin_ms, memory_order_relaxed);
  uint64_t send_fin =
      atomic_load_explicit(&stat->dbg_send_fin_remote_ms, memory_order_relaxed);
  uint64_t on_fin =
      atomic_load_explicit(&stat->dbg_on_fin_ms, memory_order_relaxed);
  uint64_t send_fack =
      atomic_load_explicit(&stat->dbg_send_fin_ack_ms, memory_order_relaxed);
  uint64_t on_fack =
      atomic_load_explicit(&stat->dbg_on_fin_ack_ms, memory_order_relaxed);
  uint64_t bfs_enqueue =
      atomic_load_explicit(&stat->dbg_bfs_enqueue_ms, memory_order_relaxed);
  uint64_t bfs_pop =
      atomic_load_explicit(&stat->dbg_bfs_pop_ms, memory_order_relaxed);
  uint64_t bfs_done =
      atomic_load_explicit(&stat->dbg_bfs_done_ms, memory_order_relaxed);
  uint32_t bfs_processed =
      atomic_load_explicit(&stat->dbg_bfs_processed, memory_order_relaxed);
  uint32_t bfs_cascade =
      atomic_load_explicit(&stat->dbg_bfs_cascade, memory_order_relaxed);

  fprintf(stderr,
          "[MSET_TIMELINE] ts_ms=%lu tag=%s shard=%u stat=%p age_ms=%lu "
          "coord=%u pidx=%u total=%u ack=%u fin_ack=%u "
          "start=%lu ack_last=%lu d_ack=%lld on_ack=%lu d_on_ack=%lld "
          "broadcast=%lu d_bcast=%lld run_fin=%lu d_run=%lld "
          "send_fin=%lu d_send_fin=%lld on_fin=%lu d_on_fin=%lld "
          "send_fack=%lu d_send_fack=%lld on_fack=%lu d_on_fack=%lld "
          "bfs_enq=%lu d_bfs_enq=%lld bfs_pop=%lu d_bfs_pop=%lld "
          "bfs_done=%lu d_bfs_done=%lld bfs_processed=%u "
          "bfs_cascade=%u local_heads=%u\n",
          (unsigned long)cur_ms, tag, shard->id, (void *)stat,
          (unsigned long)mset_stat_age_ms(stat, cur_ms), stat->coordinator_id,
          stat->pipeline_idx, stat->total,
          atomic_load_explicit(&stat->ack, memory_order_acquire),
          atomic_load_explicit(&stat->fin_ack, memory_order_acquire),
          (unsigned long)start, (unsigned long)ack_last,
          ack_last && start ? (long long)(ack_last - start) : -1LL,
          (unsigned long)on_ack,
          on_ack && start ? (long long)(on_ack - start) : -1LL,
          (unsigned long)broadcast,
          broadcast && start ? (long long)(broadcast - start) : -1LL,
          (unsigned long)run_fin,
          run_fin && start ? (long long)(run_fin - start) : -1LL,
          (unsigned long)send_fin,
          send_fin && start ? (long long)(send_fin - start) : -1LL,
          (unsigned long)on_fin,
          on_fin && start ? (long long)(on_fin - start) : -1LL,
          (unsigned long)send_fack,
          send_fack && start ? (long long)(send_fack - start) : -1LL,
          (unsigned long)on_fack,
          on_fack && start ? (long long)(on_fack - start) : -1LL,
          (unsigned long)bfs_enqueue,
          bfs_enqueue && start ? (long long)(bfs_enqueue - start) : -1LL,
          (unsigned long)bfs_pop,
          bfs_pop && start ? (long long)(bfs_pop - start) : -1LL,
          (unsigned long)bfs_done,
          bfs_done && start ? (long long)(bfs_done - start) : -1LL,
          bfs_processed, bfs_cascade,
          mset_debug_count_local_heads(stat, shard->id));
}

static void mset_debug_cascade_event(struct shard *shard, const char *event,
                                     struct mset_stat *stat,
                                     struct mset_stat *source,
                                     uint32_t local_before,
                                     uint32_t local_after,
                                     uint32_t processed,
                                     uint64_t cur_ms, bool anomaly) {
  if (!mset_debug_enabled() || !stat)
    return;
  bool cascade =
      atomic_load_explicit(&stat->dbg_bfs_cascade, memory_order_relaxed) != 0;
  uint64_t age_ms = mset_stat_age_ms(stat, cur_ms);
  if (!cascade && !anomaly && age_ms < mset_debug_slow_ms() &&
      !mset_debug_trace_enabled())
    return;

  fprintf(stderr,
          "[MSET_CASCADE] ts_ms=%lu event=%s shard=%u stat=%p age_ms=%lu "
          "coord=%u pidx=%u total=%u ack=%u fin_ack=%u source=%p "
          "source_pidx=%u local_before=%u local_after=%u processed=%u "
          "anomaly=%d cascade=%d\n",
          (unsigned long)cur_ms, event, shard->id, (void *)stat,
          (unsigned long)age_ms, stat->coordinator_id, stat->pipeline_idx,
          stat->total, atomic_load_explicit(&stat->ack, memory_order_acquire),
          atomic_load_explicit(&stat->fin_ack, memory_order_acquire),
          (void *)source, source ? source->pipeline_idx : 0, local_before,
          local_after, processed, anomaly, cascade);
}

static void mset_debug_bfs_event(struct shard *shard, const char *event,
                                 struct mset_stat *stat,
                                 struct mset_stat *head,
                                 struct mset_stat *tail,
                                 struct mset_stat *other,
                                 uint32_t n_processed,
                                 uint64_t cur_ms, bool suspicious) {
  if (!mset_debug_enabled() || !stat)
    return;
  uint64_t age_ms = mset_stat_age_ms(stat, cur_ms);
  if (!suspicious && age_ms < mset_debug_slow_ms() &&
      !mset_debug_trace_enabled())
    return;

  fprintf(stderr,
          "[MSET_BFS] ts_ms=%lu event=%s shard=%u stat=%p age_ms=%lu "
          "coord=%u pidx=%u total=%u ack=%u fin_ack=%u processed=%u "
          "local_heads=%u head=%p tail=%p other=%p suspicious=%d\n",
          (unsigned long)cur_ms, event, shard->id, (void *)stat,
          (unsigned long)age_ms, stat->coordinator_id, stat->pipeline_idx,
          stat->total, atomic_load_explicit(&stat->ack, memory_order_acquire),
          atomic_load_explicit(&stat->fin_ack, memory_order_acquire),
          n_processed, mset_debug_count_local_heads(stat, shard->id),
          (void *)head, (void *)tail, (void *)other, suspicious);
}

/*
 * Allocate a new kv_obj with key+value data.
 * NOT inserted into HT — deferred to FIN.
 */
static struct kv_obj *alloc_new_obj(const char *key, size_t klen,
                                    const char *val, size_t vlen) {
  size_t total = sizeof(struct kv_obj) + klen + vlen + 1;
  struct kv_obj *o = mem_malloc(total);
  if (!o)
    return NULL;

  o->heap_idx = HEAP_IDX_NONE;
  o->pool_idx =
      total <= SMALL_ALLOC_MAX ? pool_idx_get(total) : NR_SMALL_POOLS;
  o->lru_prev = NULL;
  o->lru_next = NULL;
  o->key_len = (uint16_t)klen;
  o->val_len = (uint32_t)vlen;
  o->expire_ms = 0;
  atomic_init(&o->refcount, g_snapshot_active ? 2 : 1);
  o->marked_deletion = 0;
  atomic_init(&o->snap_flag, SNAP_FLAG_LIVE_FRESH);

  memcpy(o->data, key, klen);
  memcpy(o->data + klen, val, vlen);
  o->data[klen + vlen] = '\0';
  return o;
}

/*
 * Increment ack and determine if FIN should be driven locally.
 * Returns mset_stat* if this shard is the coordinator and all keys are ready.
 * Returns NULL if not ready yet, or if ACK was sent to a remote coordinator.
 */
static struct mset_stat *try_send_ack(struct shard_engine *engine,
                                      struct shard *shard,
                                      struct mset_stat *stat) {
  uint32_t prev =
      atomic_fetch_add_explicit(&stat->ack, 1, memory_order_acq_rel);

  if (prev + 1 != stat->total)
    return NULL;

  atomic_store_explicit(&stat->dbg_ack_last_ms, now_ms(),
                        memory_order_relaxed);
  mset_debug_lifecycle(shard, stat,
                       stat->coordinator_id == shard->id ? "ack_last_local"
                                                         : "ack_last_remote",
                       now_ms(), prev + 1);

  if (stat->coordinator_id == shard->id) {
    return stat;
  }

  /* Remote coordinator — send SPSC ACK */
  struct spsc_message ack = {
      .op = MSG_PACK_OP(MSG_SYS_REPLY, MSG_CMD_MSET_ACK),
      .shard_owner = (uint8_t)shard->id,
      .conn_ptr = stat->conn_ptr,
      .conn_generation = stat->conn_generation,
      .pipeline_idx = stat->pipeline_idx,
      .kv_obj_ptr = stat,
  };
  send_to_shard(engine, shard->id, stat->coordinator_id, &ack);
  
  /* Wake up coordinator shard */
  if (engine->wake_fds && engine->wake_fds[stat->coordinator_id] >= 0) {
    uint64_t one = 1;
    (void)write(engine->wake_fds[stat->coordinator_id], &one, sizeof(one));
  }
  return NULL;
}

/*
 * Low-level free for mset_stat. Only called by the owner shard.
 */
static inline void stat_free_real(struct shard_engine *engine,
                                   struct shard *shard,
                                   struct mset_stat *stat) {
  if (mset_debug_enabled()) {
    uint64_t cur_ms = now_ms();
    uint64_t age_ms = mset_stat_age_ms(stat, cur_ms);
    if (age_ms >= mset_debug_slow_ms()) {
      mset_debug_print_timeline("inflight_dec_slow", shard, stat, cur_ms);
      fprintf(stderr,
              "[MSET_INFLIGHT_DEC_SLOW] ts_ms=%lu shard=%u stat=%p age_ms=%lu "
              "coord=%u pidx=%u total=%u ack=%u fin_ack=%u inflight_before=%u "
              "lm_live=%u lm_deleted=%u\n",
              (unsigned long)cur_ms, shard->id, (void *)stat,
              (unsigned long)age_ms,
              stat->coordinator_id, stat->pipeline_idx, stat->total,
              atomic_load_explicit(&stat->ack, memory_order_acquire),
              atomic_load_explicit(&stat->fin_ack, memory_order_acquire),
              shard->mset_inflight, shard->lm.count, shard->lm.deleted_count);
    }
  }
  struct net_conn *c = (struct net_conn *)stat->conn_ptr;
  if (c && c->generation == stat->conn_generation && c->proxy.slots) {
    struct net_pipeline_slot *ps =
        &c->proxy.slots[stat->pipeline_idx & (c->proxy.cap - 1)];
    if (ps->debug_mset_stat == stat) {
      ps->debug_mset_stat = NULL;
      ps->debug_mset_start_ms = 0;
    }
  }
  shard->mset_inflight--;
  mset_debug_progress(shard, now_ms(), "stat_free");
  struct mset_batch *b = stat->batch_cleanup_head;
  while (b) {
    struct mset_batch *next = b->cleanup_next;
    slab_obj_free(shard->pool, b);
    b = next;
  }
  if (stat->coord_rbuf_nb) {
    net_buf_unref(shard->pool, stat->coord_rbuf_nb);
    stat->coord_rbuf_nb = NULL;
  }
  slab_obj_free(shard->pool, stat);
}

/*
 * Send MSG_CMD_MSET_FIN to every remote shard that has keys for this stat.
 * Only called by the coordinator shard.
 */
static void mset_send_fin_to_remotes(struct shard_engine *engine,
                                     struct shard *shard,
                                     struct mset_stat *stat) {
  if (stat->coordinator_id != (uint8_t)shard->id)
    return;

  uint32_t my_id = shard->id;
  uint32_t nshards = engine->num_shards;
  uint32_t wake_mask = 0;

  for (uint32_t sid = 0; sid < nshards; sid++) {
    if (sid == my_id || !stat->shard_heads[sid])
      continue;
    struct spsc_message fin = {
        .op = MSG_PACK_OP(MSG_SYS_REQ, MSG_CMD_MSET_FIN),
        .shard_owner = (uint8_t)my_id,
        .pipeline_idx = stat->pipeline_idx,
        .kv_obj_ptr = stat,
    };
    atomic_store_explicit(&stat->dbg_send_fin_remote_ms, now_ms(),
                          memory_order_relaxed);
    mset_debug_lifecycle(shard, stat, "send_fin_remote", now_ms(), sid);
    send_to_shard(engine, my_id, sid, &fin);
    wake_mask |= (1U << sid);
  }

  if (wake_mask && engine->wake_fds) {
    for (uint32_t sid = 0; sid < nshards; sid++) {
      if ((wake_mask & (1U << sid)) && engine->wake_fds[sid] >= 0) {
        uint64_t one = 1;
        (void)write(engine->wake_fds[sid], &one, sizeof(one));
      }
    }
  }
}

static void mset_run_fin(struct shard_engine *engine, struct shard *shard,
                         struct mset_stat *initial_stat, uint64_t cur_ms);

static bool mset_try_early_commit(struct shard_engine *engine,
                                   struct shard *shard,
                                   struct lock_entry *le,
                                   uint64_t cur_ms);

/*
 * Detach stat from its pipeline slot, then drive the full FIN phase:
 * broadcasts FIN to remote shards and runs local FIN processing.
 * Safe to call from both mset_on_prepare (coordinator-is-last path)
 * and mset_on_ack (normal ACK-received path).
 */
static void mset_broadcast_fin(struct shard_engine *engine, struct shard *shard,
                               struct mset_stat *stat) {
  atomic_store_explicit(&stat->dbg_broadcast_fin_ms, now_ms(),
                        memory_order_relaxed);
  mset_debug_lifecycle(shard, stat, "broadcast_fin", now_ms(), 0);

  if (mset_debug_enabled()) {
    uint64_t cur_ms = now_ms();
    uint64_t age_ms = mset_stat_age_ms(stat, cur_ms);
    if (age_ms >= mset_debug_slow_ms()) {
      fprintf(stderr,
              "[MSET_ACK_READY_SLOW] ts_ms=%lu shard=%u stat=%p age_ms=%lu coord=%u "
              "pidx=%u total=%u ack=%u fin_ack=%u inflight=%u\n",
              (unsigned long)cur_ms, shard->id, (void *)stat,
              (unsigned long)age_ms,
              stat->coordinator_id, stat->pipeline_idx, stat->total,
              atomic_load_explicit(&stat->ack, memory_order_acquire),
              atomic_load_explicit(&stat->fin_ack, memory_order_acquire),
              shard->mset_inflight);
    }
  }

  /* Measure PREPARE phase: start_cycles → all ACKs received. */
  PROF_RECORD_HIST(shard->prof.mset_prepare_rtt, shard->prof.mset_prepare_rtt_hist,
                   stat->start_cycles);

  /* Detach from pipeline slot so a late mset_on_ack is a safe no-op. */
  struct net_conn *c = (struct net_conn *)stat->conn_ptr;
  if (c && c->generation == stat->conn_generation) {
    struct net_pipeline_slot *ps =
        &c->proxy.slots[stat->pipeline_idx & (c->proxy.cap - 1)];
    ps->kv_obj_ptr = NULL;
  }
  mset_run_fin(engine, shard, stat, now_ms());
}

/* ── Wait-queue drain ────────────────────────────────────────────────── */

/*
 * Drain the wait queue for a just-released key.
 * Executes queued regular commands immediately (proxy_exec + SPSC reply).
 * Stops at the first MSET anchor, promotes it to new lock holder.
 * Returns the newly-ready mset_stat if the promoted MSET is now ready for FIN,
 * or NULL if the queue is empty / the promoted MSET is still waiting.
 */
static struct mset_stat *mset_drain_waitqueue(struct shard_engine *engine,
                                              struct shard *shard,
                                              struct lock_entry *le,
                                              uint64_t cur_ms) {
  uint64_t start_cycles = cycles_now();

  for (;;) {
    /* Priority 1: closed groups (sorted, head = highest priority) */
    if (le->queue_head) {
      struct wait_group *wg = le->queue_head;
      struct cmd_info *ci = wg->head;

      while (ci) {
        struct cmd_info *next = ci->next;

        if (ci->is_mset) {
          /* Promote MSET anchor to holder and stop drain */
          le->holder = ci;
          ci->next = NULL;
          wg->head = next;
          if (!next)
            wg->tail = NULL;

          /* 
           * CRITICAL: Update old_obj to the current stable value in HT.
           * This ensures that when this MSET eventually commits, it frees
           * the object it actually replaced, not a stale one from PREPARE.
           */
          ci->old_obj =
              ht_bucket_get(shard->table, obj_key_get(ci->new_obj),
                            ci->new_obj->key_len, VAL_TYPE_STRING, 0);

          uint32_t ack_before = 0;
          if (mset_debug_trace_enabled())
            ack_before = atomic_load_explicit(&ci->stat->ack,
                                              memory_order_acquire);
          struct mset_stat *ready = try_send_ack(engine, shard, ci->stat);
          if (mset_debug_trace_enabled()) {
            uint32_t ack_after = atomic_load_explicit(&ci->stat->ack,
                                                      memory_order_acquire);
            fprintf(stderr,
                    "[MSET_PROMOTE] shard=%u le=%p cmd=%p stat=%p "
                    "total_acks=%u ack_before=%u ack_after=%u total=%u "
                    "old_obj=%p le_obj=%p new_obj=%p ready=%d\n",
                    shard->id, (void *)le, (void *)ci, (void *)ci->stat,
                    ci->total_acks, ack_before, ack_after, ci->stat->total,
                    (void *)ci->old_obj, (void *)le->obj_ptr,
                    (void *)ci->new_obj, ready != NULL);
          }

          /* If this was the last command in group, free it */
          if (!wg->head) {
            le->queue_head = wg->queue_next;
            if (le->queue_head)
              le->queue_head->queue_prev = NULL;
            else
              le->queue_tail = NULL;
            wait_group_free(shard->pool, wg);
          }

          PROF_RECORD(shard->prof.mset_drain, start_cycles);
          return ready;
        }

        /* Regular command: execute and reply */
        uint64_t exec_start = cycles_now();
        uint64_t wait_cycles = ci->defer_cycles ? exec_start - ci->defer_cycles : 0;
        enum proxy_exec_result exec_rc = proxy_exec(shard, &ci->msg, cur_ms);
        uint64_t exec_cycles = cycles_now() - exec_start;
        mset_mixed_log_set_drain(shard, ci, exec_rc, wait_cycles, exec_cycles);
        if (exec_rc == PROXY_EXEC_DONE) {
          shard->ops_completed++;
          mset_reanchor_lock_after_regular(shard, le, ci);
          mset_complete_regular_reply(engine, shard, ci);
        }
        cmd_info_free(shard->pool, ci);
        ci = next;
      }

      /* Group exhausted (all regular commands done) */
      le->queue_head = wg->queue_next;
      if (le->queue_head)
        le->queue_head->queue_prev = NULL;
      else
        le->queue_tail = NULL;
      wait_group_free(shard->pool, wg);
      continue;
    }

    /* Priority 2: open groups (regular commands only) */
    bool found_open = false;
    for (uint32_t j = 0; j < le->num_shards; j++) {
      struct wait_group *og = le->open_groups[j];
      if (!og)
        continue;
      le->open_groups[j] = NULL;
      found_open = true;

      struct cmd_info *ci = og->head;
      while (ci) {
        struct cmd_info *next = ci->next;
        uint64_t exec_start = cycles_now();
        uint64_t wait_cycles = ci->defer_cycles ? exec_start - ci->defer_cycles : 0;
        enum proxy_exec_result exec_rc = proxy_exec(shard, &ci->msg, cur_ms);
        uint64_t exec_cycles = cycles_now() - exec_start;
        mset_mixed_log_set_drain(shard, ci, exec_rc, wait_cycles, exec_cycles);
        if (exec_rc == PROXY_EXEC_DONE) {
          shard->ops_completed++;
          mset_reanchor_lock_after_regular(shard, le, ci);
          mset_complete_regular_reply(engine, shard, ci);
        }
        cmd_info_free(shard->pool, ci);
        ci = next;
      }
      wait_group_free(shard->pool, og);
    }

    if (found_open)
      continue;

    break;
  }

  PROF_RECORD(shard->prof.mset_drain, start_cycles);
  return NULL;
}

/* ── FIN: single-cmd worker ─────────────────────────────────────────── */

/*
 * Commit one cmd_info during FIN phase:
 *   1. Swap new_obj into the HT (if not already there).
 *   2. Update TTL index, LRU list, snapshot.
 *   3. Release the key lock (le->holder = NULL).
 *   4. Drain the wait queue — returns any newly-ready mset_stat.
 *   5. Update the HT lock bit.
 *   6. Free cmd_info.
 *
 * Does NOT touch fin_ack — mset_run_fin batches the increment after
 * processing all cmds for a given stat, so it can detect "was last" safely.
 */
static struct mset_stat *mset_fin_one_cmd(struct shard_engine *engine,
                                          struct shard *shard,
                                          struct cmd_info *cmd,
                                          uint64_t cur_ms) {
  uint64_t start_cycles = cycles_now();
  struct lock_entry *le = cmd->le;
  struct kv_obj *new_obj = cmd->new_obj;
  uint64_t hash56 = cmd->hash56;

  /* Refcount safety: prevent destruction during swap */
  shard_obj_ref(new_obj);

  /* 1. Reconcile HT state unconditionally.
   *
   * For new keys with no contention: current_in_ht == new_obj → no-op.
   * For existing keys: current_in_ht == old value → swap.
   * For preempted-then-woken new keys: current_in_ht == preemptor's object
   *   → swap ours in and destroy theirs safely.
   */
  {
    struct kv_obj *current_in_ht =
        ht_bucket_get(shard->table, obj_key_get(new_obj), new_obj->key_len,
                      VAL_TYPE_STRING, 0);

    /*
     * We only swap if we aren't already the stable object in HT.
     * Note: If we were a Wound-winner on a new key, we didn't touch HT
     * in PREPARE, so we MUST swap now.
     */
    if (current_in_ht != new_obj) {
      uint64_t t_put = cycles_now();
      struct kv_obj *replaced =
          ht_bucket_put(shard->table, new_obj, VAL_TYPE_STRING, 0, HT_PUT_NONE);
      PROF_RECORD(shard->prof.ht_put_internal, t_put);
      (void)replaced;

      le->obj_ptr = new_obj;

      /*
       * SAFETY: We do NOT destroy the 'replaced' object from HT.
       * It might be a tentative object belonging to a waiting MSET.
       * Instead, we destroy the 'old_obj' that this command officially
       * replaces. If old_obj is NULL, it's a new key; nothing to free.
       */
      if (cmd->old_obj) {
        shard_obj_destroy(shard, cmd->old_obj);
        cmd->old_obj = NULL;
      }
    }
  }

  /* 2. TTL index, LRU, snapshot */
  if (new_obj->expire_ms && new_obj->heap_idx == HEAP_IDX_NONE)
    ttl_index_node_push(&shard->ttl_idx, new_obj, new_obj->expire_ms);
  lru_node_prepend(shard, new_obj);
  if (g_snapshot_active) {
    size_t total = sizeof(struct kv_obj) + new_obj->key_len + new_obj->val_len + 1;
    uint32_t obj_size =
        (total <= SMALL_ALLOC_MAX) ? POOL_CLASSES[pool_idx_get(total)] : 0;
    snap_obj_mark(&shard->snap, shard->mem, new_obj, obj_size);
  }

  /* 3. Release lock */
  if (le->holder == cmd) {
    le->holder = NULL;
  }

  /* 4. Drain wait queue — may promote a waiting MSET to new holder */
  struct mset_stat *next_ready = mset_drain_waitqueue(engine, shard, le, cur_ms);

  /* 5. Update HT lock bit */
  if (!le->holder) {
    bool has_waiters = (le->queue_head != NULL);
    if (!has_waiters) {
      for (uint32_t j = 0; j < le->num_shards && !has_waiters; j++)
        if (le->open_groups[j]) has_waiters = true;
    }
    if (!has_waiters) {
      ht_bucket_set_lock_status(shard->table, obj_key_get(new_obj),
                                new_obj->key_len, VAL_TYPE_STRING, false);
      lock_manager_remove(&shard->lm, le->obj_ptr, hash56);
    }
  } else {
    ht_bucket_set_lock_status(shard->table, obj_key_get(new_obj),
                              new_obj->key_len, VAL_TYPE_STRING, true);
  }

  /* 6. Release safety ref, free cmd */
  shard_obj_unref(shard, new_obj);
  slab_obj_free(shard->pool, cmd);

  PROF_RECORD(shard->prof.mset_fin, start_cycles);
  return next_ready;
}

/* ── FIN: iterative breadth-first driver ────────────────────────────── */

/*
 * Drive the FIN phase for initial_stat and any MSETs that become ready
 * as wait queues drain.
 *
 * For each stat entering the BFS queue, FIN is broadcast to its remote
 * participant shards (coordinator-only). Local cmds are processed here.
 * fin_ack is batch-incremented after all local cmds for a stat are done.
 *
 * The shard that makes fin_ack == total is the last globally:
 *   - coordinator: sets reply + frees stat
 *   - remote shard: sends ONE FIN_ACK to coordinator
 * All other shards do nothing further (no partial FIN_ACKs).
 */
static void mset_run_fin(struct shard_engine *engine, struct shard *shard,
                         struct mset_stat *initial_stat, uint64_t cur_ms) {
  atomic_store_explicit(&initial_stat->dbg_run_fin_ms, cur_ms,
                        memory_order_relaxed);
  mset_debug_lifecycle(shard, initial_stat, "run_fin_start", cur_ms, 0);

  /* Broadcast FIN to remote participant shards for the initial stat. */
  mset_send_fin_to_remotes(engine, shard, initial_stat);

  enum { MSET_FIN_QUEUE_INLINE = 256 };
  struct mset_stat *inline_queue[MSET_FIN_QUEUE_INLINE];
  struct mset_stat **queue = inline_queue;
  size_t queue_cap = MSET_FIN_QUEUE_INLINE;
  size_t queue_head = 0;
  size_t queue_tail = 0;

  queue[queue_tail++] = initial_stat;
  atomic_store_explicit(&initial_stat->dbg_bfs_enqueue_ms, cur_ms,
                        memory_order_relaxed);

  while (queue_head < queue_tail) {
    struct mset_stat *stat = queue[queue_head++];
    uint32_t n_processed = 0;
    uint32_t local_before =
        mset_debug_enabled() ? mset_debug_count_local_heads(stat, shard->id) : 0;
    atomic_store_explicit(&stat->dbg_bfs_pop_ms, cur_ms, memory_order_relaxed);
    mset_debug_cascade_event(shard, "pop", stat, NULL, local_before,
                             local_before, 0, cur_ms, false);
    struct mset_stat *queue_next =
        queue_head < queue_tail ? queue[queue_head] : NULL;
    struct mset_stat *queue_last =
        queue_tail > queue_head ? queue[queue_tail - 1] : NULL;
    mset_debug_bfs_event(shard, "pop", stat, queue_next, queue_last,
                         NULL, 0, cur_ms, false);

    struct cmd_info *cmd;
    while ((cmd = stat->shard_heads[shard->id]) != NULL) {
      stat->shard_heads[shard->id] = cmd->next_in_mset;

      uint32_t acks = cmd->total_acks;
      struct mset_stat *next_ready = mset_fin_one_cmd(engine, shard, cmd, cur_ms);
      n_processed += acks;

      if (next_ready) {
        /* try_send_ack only returns non-NULL for the coordinator, so
         * next_ready->coordinator_id == shard->id is always true here. */
        bool suspicious = false;
        if (mset_debug_enabled()) {
          suspicious = next_ready == stat;
          for (size_t i = queue_head; i < queue_tail; i++) {
            if (queue[i] == next_ready) {
              suspicious = true;
              break;
            }
          }
        }
        uint32_t next_local_heads = mset_debug_enabled()
                                        ? mset_debug_count_local_heads(
                                              next_ready, shard->id)
                                        : 0;
        atomic_store_explicit(&next_ready->dbg_bfs_cascade, 1,
                              memory_order_relaxed);
        mset_debug_cascade_event(shard, "enqueue", next_ready, stat,
                                 next_local_heads, next_local_heads, 0,
                                 cur_ms, suspicious);
        queue_next = queue_head < queue_tail ? queue[queue_head] : NULL;
        queue_last = queue_tail > queue_head ? queue[queue_tail - 1] : NULL;
        mset_debug_bfs_event(shard, "enqueue_before", next_ready, queue_next,
                             queue_last,
                             stat, 0, cur_ms, suspicious);
        mset_send_fin_to_remotes(engine, shard, next_ready);
        if (queue_tail == queue_cap) {
          size_t new_cap = queue_cap * 2;
          struct mset_stat **new_queue =
              (struct mset_stat **)malloc(new_cap * sizeof(*new_queue));
          if (!new_queue) {
            fprintf(stderr,
                    "[MSET_FIN_QUEUE_OOM] shard=%u cap=%zu stat=%p next=%p\n",
                    shard->id, queue_cap, (void *)stat, (void *)next_ready);
            abort();
          }
          memcpy(new_queue, queue, queue_tail * sizeof(*new_queue));
          if (queue != inline_queue)
            free(queue);
          queue = new_queue;
          queue_cap = new_cap;
        }
        queue[queue_tail++] = next_ready;
        atomic_store_explicit(&next_ready->dbg_bfs_enqueue_ms, cur_ms,
                              memory_order_relaxed);
        queue_next = queue_head < queue_tail ? queue[queue_head] : NULL;
        queue_last = queue_tail > queue_head ? queue[queue_tail - 1] : NULL;
        mset_debug_bfs_event(shard, "enqueue_after", next_ready, queue_next,
                             queue_last,
                             stat, 0, cur_ms, suspicious);
      }
    }

    struct mset_stat *next_in_queue =
        queue_head < queue_tail ? queue[queue_head] : NULL;
    atomic_store_explicit(&stat->dbg_bfs_done_ms, cur_ms, memory_order_relaxed);
    atomic_store_explicit(&stat->dbg_bfs_processed, n_processed,
                          memory_order_relaxed);
    uint32_t local_after =
        mset_debug_enabled() ? mset_debug_count_local_heads(stat, shard->id) : 0;
    mset_debug_cascade_event(shard, "done", stat, next_in_queue, local_before,
                             local_after, n_processed, cur_ms,
                             local_after > 0);
    queue_last = queue_tail > queue_head ? queue[queue_tail - 1] : NULL;
    mset_debug_bfs_event(shard, "done", stat, next_in_queue, queue_last,
                         next_in_queue,
                         n_processed, cur_ms,
                         local_after > 0);

    if (n_processed > 0) {
      uint32_t ack_now = 0;
      uint32_t fin_before_dbg = 0;
      if (mset_debug_trace_enabled()) {
        ack_now = atomic_load_explicit(&stat->ack, memory_order_acquire);
        fin_before_dbg =
            atomic_load_explicit(&stat->fin_ack, memory_order_acquire);
      }
      uint32_t prev = atomic_fetch_add_explicit(&stat->fin_ack, n_processed,
                                                memory_order_acq_rel);
      if (mset_debug_trace_enabled()) {
        uint32_t fin_after_dbg = prev + n_processed;
        fprintf(stderr,
                "[MSET_FIN_STAT] shard=%u stat=%p n_processed=%u total=%u "
                "ack=%u fin_before=%u fin_after=%u coord=%u local=%d "
                "next=%p\n",
                shard->id, (void *)stat, n_processed, stat->total, ack_now,
                fin_before_dbg, fin_after_dbg, stat->coordinator_id,
                stat->coordinator_id == (uint8_t)shard->id,
                (void *)next_in_queue);
      }
      if (prev + n_processed == stat->total) {
        /* This shard is the last to finish FIN globally. */
        if (stat->coordinator_id == (uint8_t)shard->id) {
          if (mset_debug_enabled()) {
            uint64_t cur_dbg_ms = now_ms();
            uint64_t age_ms = mset_stat_age_ms(stat, cur_dbg_ms);
            if (age_ms >= mset_debug_slow_ms()) {
              mset_debug_print_timeline("fin_local_slow", shard, stat,
                                        cur_dbg_ms);
              fprintf(stderr,
                      "[MSET_FIN_LOCAL_SLOW] ts_ms=%lu shard=%u stat=%p age_ms=%lu "
                      "pidx=%u total=%u ack=%u fin_ack_after=%u "
                      "n_processed=%u inflight=%u\n",
                      (unsigned long)cur_dbg_ms, shard->id, (void *)stat,
                      (unsigned long)age_ms,
                      stat->pipeline_idx, stat->total,
                      atomic_load_explicit(&stat->ack, memory_order_acquire),
                      prev + n_processed, n_processed, shard->mset_inflight);
            }
          }
          mset_set_ok_reply(shard, stat, "fin_local");
          uint64_t e2e = stat->start_cycles;
          PROF_RECORD_HIST(shard->prof.mset_total_e2e, shard->prof.mset_e2e_hist, e2e);
          stat_free_real(engine, shard, stat);
        } else {
          if (mset_debug_enabled()) {
            uint64_t cur_dbg_ms = now_ms();
            uint64_t age_ms = mset_stat_age_ms(stat, cur_dbg_ms);
            if (age_ms >= mset_debug_slow_ms()) {
              mset_debug_print_timeline("fin_remote_slow", shard, stat,
                                        cur_dbg_ms);
              fprintf(stderr,
                      "[MSET_FIN_REMOTE_SLOW] ts_ms=%lu shard=%u stat=%p age_ms=%lu "
                      "coord=%u pidx=%u total=%u ack=%u fin_ack_after=%u "
                      "n_processed=%u\n",
                      (unsigned long)cur_dbg_ms, shard->id, (void *)stat,
                      (unsigned long)age_ms,
                      stat->coordinator_id, stat->pipeline_idx, stat->total,
                      atomic_load_explicit(&stat->ack, memory_order_acquire),
                      prev + n_processed, n_processed);
            }
          }
          /* Remote shard: send the ONE FIN_ACK to coordinator. */
          struct spsc_message fack = {
              .op = MSG_PACK_OP(MSG_SYS_REPLY, MSG_CMD_MSET_FIN_ACK),
              .shard_owner = (uint8_t)shard->id,
              .conn_ptr = stat->conn_ptr,
              .conn_generation = stat->conn_generation,
              .pipeline_idx = stat->pipeline_idx,
              .kv_obj_ptr = stat,
          };
          atomic_store_explicit(&stat->dbg_send_fin_ack_ms, now_ms(),
                                memory_order_relaxed);
          mset_debug_lifecycle(shard, stat, "send_fin_ack", now_ms(),
                               stat->coordinator_id);
          send_to_shard(engine, shard->id, stat->coordinator_id, &fack);

          /* Wake up coordinator shard */
          if (engine->wake_fds && engine->wake_fds[stat->coordinator_id] >= 0) {
            uint64_t one = 1;
            (void)write(engine->wake_fds[stat->coordinator_id], &one, sizeof(one));
          }
        }
      }
      /* Non-last shards: nothing to do. */
    } else {
      mset_debug_lifecycle(shard, stat, "run_fin_noop", cur_ms, 0);
    }

  }

  if (queue != inline_queue)
    free(queue);
}

/* ── Early Commit ────────────────────────────────────────────────────── */

/*
 * Attempt to commit the current lock holder's MSET immediately, without
 * waiting for FIN, if all PREPAREs across all shards are already done
 * (ack == total).
 *
 * When ack == total, the coordinator has already decided to commit and FIN
 * is guaranteed to arrive — we just beat it here. Any command that finds
 * a key locked in this state would otherwise sit in the wait queue for an
 * entire FIN round-trip. Early commit eliminates that wait.
 *
 * Processes all stat->shard_heads[shard->id] cmds, updates fin_ack, and
 * handles the "last globally" case (set +OK or send FIN_ACK to coordinator).
 *
 * Late FIN arriving afterward is a no-op: shard_heads is already NULL so
 * mset_run_fin processes 0 cmds and skips the fin_ack increment.
 *
 * Returns true if early commit was performed (lock state has changed).
 * Returns false if conditions are not met (ack < total, or no MSET holder).
 */
static bool mset_try_early_commit(struct shard_engine *engine,
                                   struct shard *shard,
                                   struct lock_entry *le,
                                   uint64_t cur_ms) {
  if (!le->holder || !le->holder->is_mset)
    return false;

  struct mset_stat *stat = le->holder->stat;
  uint32_t ack = atomic_load_explicit(&stat->ack, memory_order_acquire);
  if (ack < stat->total)
    return false;

  /* ack == total: FIN is imminent — commit all local keys for this stat now */
  if (mixed_profile_enabled())
    mixed_early_commit_count[shard->id]++;

  uint32_t n_processed = 0;
  struct cmd_info *cmd;
  while ((cmd = stat->shard_heads[shard->id]) != NULL) {
    stat->shard_heads[shard->id] = cmd->next_in_mset;
    n_processed += cmd->total_acks;
    struct mset_stat *next_ready = mset_fin_one_cmd(engine, shard, cmd, cur_ms);
    if (next_ready)
      mset_broadcast_fin(engine, shard, next_ready);
  }

  if (n_processed == 0)
    return true;

  uint32_t prev_fin = atomic_fetch_add_explicit(&stat->fin_ack, n_processed,
                                                 memory_order_acq_rel);
  if (mset_debug_trace_enabled()) {
    fprintf(stderr,
            "[MSET_EARLY_FIN_STAT] shard=%u stat=%p n_processed=%u total=%u "
            "ack=%u fin_before=%u fin_after=%u coord=%u local=%d\n",
            shard->id, (void *)stat, n_processed, stat->total,
            atomic_load_explicit(&stat->ack, memory_order_acquire), prev_fin,
            prev_fin + n_processed, stat->coordinator_id,
            stat->coordinator_id == (uint8_t)shard->id);
  }
  if (prev_fin + n_processed != stat->total)
    return true;  /* not last globally — coordinator or other shards will close out */

  /* Last globally: deliver +OK and free stat */
  if (stat->coordinator_id == (uint8_t)shard->id) {
    if (mset_debug_enabled()) {
      uint64_t cur_dbg_ms = now_ms();
      uint64_t age_ms = mset_stat_age_ms(stat, cur_dbg_ms);
      if (age_ms >= mset_debug_slow_ms()) {
        mset_debug_print_timeline("early_fin_local_slow", shard, stat,
                                  cur_dbg_ms);
        fprintf(stderr,
                "[MSET_EARLY_FIN_LOCAL_SLOW] ts_ms=%lu shard=%u stat=%p age_ms=%lu "
                "pidx=%u total=%u ack=%u fin_ack_after=%u n_processed=%u "
                "inflight=%u\n",
                (unsigned long)cur_dbg_ms, shard->id, (void *)stat,
                (unsigned long)age_ms,
                stat->pipeline_idx, stat->total,
                atomic_load_explicit(&stat->ack, memory_order_acquire),
                prev_fin + n_processed, n_processed, shard->mset_inflight);
      }
    }
    mset_set_ok_reply(shard, stat, "early_fin_local");
    uint64_t e2e = stat->start_cycles;
    PROF_RECORD_HIST(shard->prof.mset_total_e2e, shard->prof.mset_e2e_hist, e2e);
    stat_free_real(engine, shard, stat);
  } else {
    if (mset_debug_enabled()) {
      uint64_t cur_dbg_ms = now_ms();
      uint64_t age_ms = mset_stat_age_ms(stat, cur_dbg_ms);
      if (age_ms >= mset_debug_slow_ms()) {
        mset_debug_print_timeline("early_fin_remote_slow", shard, stat,
                                  cur_dbg_ms);
        fprintf(stderr,
                "[MSET_EARLY_FIN_REMOTE_SLOW] ts_ms=%lu shard=%u stat=%p age_ms=%lu "
                "coord=%u pidx=%u total=%u ack=%u fin_ack_after=%u "
                "n_processed=%u\n",
                (unsigned long)cur_dbg_ms, shard->id, (void *)stat,
                (unsigned long)age_ms,
                stat->coordinator_id, stat->pipeline_idx, stat->total,
                atomic_load_explicit(&stat->ack, memory_order_acquire),
                prev_fin + n_processed, n_processed);
      }
    }
    struct spsc_message fack = {
        .op              = MSG_PACK_OP(MSG_SYS_REPLY, MSG_CMD_MSET_FIN_ACK),
        .shard_owner     = (uint8_t)shard->id,
        .conn_ptr        = stat->conn_ptr,
        .conn_generation = stat->conn_generation,
        .pipeline_idx    = stat->pipeline_idx,
        .kv_obj_ptr      = stat,
    };
    atomic_store_explicit(&stat->dbg_send_fin_ack_ms, now_ms(),
                          memory_order_relaxed);
    mset_debug_lifecycle(shard, stat, "early_send_fin_ack", now_ms(),
                         stat->coordinator_id);
    send_to_shard(engine, shard->id, stat->coordinator_id, &fack);
  }
  return true;
}

/* ── PREPARE: pure logic ─────────────────────────────────────────────── */

/*
 * Core PREPARE logic, called by mset_on_prepare.
 * Allocates new_obj, acquires or queues for the key lock, links cmd_info.
 * Returns mset_stat* if coordinator is local and all keys are now ready.
 * Returns NULL otherwise (waiting for remote ACKs or in wait queue).
 */
static struct mset_stat *mset_prepare_key(struct shard_engine *engine,
                                          struct shard *shard,
                                          struct spsc_message *msg,
                                          uint64_t cur_ms) {
  uint64_t start_cycles = cycles_now();
  struct lock_manager *lm = &shard->lm;
  struct slab_allocator *pool = shard->pool;

  const char *key = (const char *)msg->key_ptr;
  size_t klen = msg->key_len;
  const char *val = (const char *)msg->val_ptr;
  size_t vlen = msg->val_len;
  struct mset_stat *stat = (struct mset_stat *)msg->kv_obj_ptr;

  struct txn_id txn = {
      .timestamp_ns = (uint64_t)msg->reply_int,
      .conn_ptr = msg->conn_ptr,
      .pipeline_idx = msg->pipeline_idx,
  };

  uint64_t meta = ht_meta_pack(ht_key_hash(key, klen), VAL_TYPE_STRING);
  uint64_t hash56 = meta >> 8;

  uint64_t t_get = cycles_now();
  struct kv_obj *obj_ptr =
      ht_bucket_get(shard->table, key, klen, VAL_TYPE_STRING, 0);
  PROF_RECORD(shard->prof.ht_get_internal, t_get);
  PROF_RECORD(shard->prof.ht_lookup, t_get);

  /* Allocate tentative new object */
  uint64_t t_alloc = cycles_now();
  struct kv_obj *new_obj = alloc_new_obj(key, klen, val, vlen);
  PROF_RECORD(shard->prof.obj_alloc, t_alloc);
  if (!new_obj) {
    shard->ops_completed++;
    PROF_RECORD(shard->prof.mset_prepare, start_cycles);
    return try_send_ack(engine, shard, stat);
  }

  /* Physical HT state at THIS transaction's arrival.
   * Valid for: tentative-insert gate (line below), rollback cleanup.
   * Do NOT use in the dup_cmd block — there the correct check is
   * dup_cmd->old_obj == NULL, which captures the semantic state recorded
   * by the first PREPARE for this txn, immune to HT pollution from other
   * txns' tentatives. */
  bool key_is_new = (obj_ptr == NULL);
  struct kv_obj *lock_key;

  if (key_is_new) {
    /* Tentative HT insert: makes key visible under the lock */
    uint64_t t_ins = cycles_now();
    struct kv_obj *replaced =
        ht_bucket_put(shard->table, new_obj, VAL_TYPE_STRING, 0, HT_PUT_NONE);
    PROF_RECORD(shard->prof.ht_insert, t_ins);
    if (replaced == (struct kv_obj *)(uintptr_t)-1) {
      /* HT full */
      shard_obj_destroy(shard, new_obj);
      shard->ops_completed++;
      PROF_RECORD(shard->prof.mset_prepare, start_cycles);
      return try_send_ack(engine, shard, stat);
    }
    if (replaced) {
      /*
       * RACE DETECTED: Someone else beat us to the tentative insert.
       * 1. Destroy our failed new_obj.
       * 2. Use the existing object (replaced) as the anchor.
       * 3. Switch to Case B (Lock check).
       */
      shard_obj_destroy(shard, new_obj);
      new_obj = replaced;
      key_is_new = false;
      lock_key = replaced;
    } else {
      if (new_obj->expire_ms)
        ttl_index_node_push(&shard->ttl_idx, new_obj, new_obj->expire_ms);
      lock_key = new_obj;
    }
  } else {
    lock_key = obj_ptr;
  }

  /* Look up existing lock entry */
  uint64_t t_lm = cycles_now();
  struct lock_entry *le = lock_manager_lookup(lm, lock_key, hash56);
  PROF_RECORD(shard->prof.lm_lookup, t_lm);

  if (le) {
    struct cmd_info *incumbent = le->holder;

    /* ── Duplicate at holder (same txn already holds the lock) ── */
    if (incumbent && incumbent->is_mset && txn_id_eq(&txn, &incumbent->txn)) {
      struct kv_obj *old_new_obj = incumbent->new_obj;

      /* For new keys: old_new_obj is the tentative in HT.
       * Swap our new_obj in so FIN finds the right object. */
      if (incumbent->old_obj == NULL) {
        ht_bucket_put(shard->table, new_obj, VAL_TYPE_STRING, 0, HT_PUT_NONE);
        le->obj_ptr = new_obj;
      }

      shard_obj_destroy(shard, old_new_obj);
      incumbent->new_obj = new_obj;
      incumbent->sub_idx = msg->sub_idx;
      incumbent->total_acks++;   /* this raw pair will be counted at FIN */
      shard->ops_completed++;
      PROF_RECORD(shard->prof.mset_prepare, start_cycles);
      return try_send_ack(engine, shard, stat);
    }

    /* ── Duplicate in wait queue (same txn waiting) ── */
    struct wait_group *dup_wg = NULL;
    uint64_t t_wq = cycles_now();
    struct cmd_info *dup_cmd = lock_wq_find_txn(le, &txn, &dup_wg);
    PROF_RECORD(shard->prof.lm_wq_find, t_wq);
    if (dup_cmd) {
      struct kv_obj *old_tentative = dup_cmd->new_obj;
      if (dup_cmd->old_obj == NULL) {
        struct kv_obj *tentative =
            ht_bucket_get(shard->table, key, klen, VAL_TYPE_STRING, 0);
        if (tentative) {
          ht_bucket_delete(shard->table, key, klen, VAL_TYPE_STRING);
          /*
           * tentative may be the same object as old_tentative: the waiting
           * dup_cmd inserted its new_obj as a tentative placeholder when it
           * was originally a new-key PREPARE.  Destroy it once here and skip
           * the unconditional destroy below to avoid double-free.
           */
          shard_obj_destroy(shard, tentative);
          if (tentative == old_tentative)
            old_tentative = NULL;
        }
        ht_bucket_put(shard->table, new_obj, VAL_TYPE_STRING, 0, HT_PUT_NONE);
        if (new_obj->expire_ms && new_obj->heap_idx == HEAP_IDX_NONE)
          ttl_index_node_push(&shard->ttl_idx, new_obj, new_obj->expire_ms);
      }
      if (old_tentative)
        shard_obj_destroy(shard, old_tentative);
      dup_cmd->new_obj = new_obj;
      dup_cmd->sub_idx = msg->sub_idx;
      dup_cmd->total_acks++;     /* this raw pair will be counted at FIN */
      shard->ops_completed++;
      PROF_RECORD(shard->prof.mset_prepare, start_cycles);
      return try_send_ack(engine, shard, stat);
    }
  }

  /* ── Allocate cmd_info ── */
  uint64_t t_slab = cycles_now();
  struct cmd_info *cmd = cmd_info_alloc(pool);
  PROF_RECORD(shard->prof.slab_alloc, t_slab);
  if (!cmd) {
    bool tentative_freed = false;
    if (key_is_new) {
      struct kv_obj *tentative =
          ht_bucket_get(shard->table, key, klen, VAL_TYPE_STRING, 0);
      if (tentative) {
        ht_bucket_delete(shard->table, key, klen, VAL_TYPE_STRING);
        shard_obj_destroy(shard, tentative);
        tentative_freed = true;
      }
    }
    if (!tentative_freed)
      shard_obj_destroy(shard, new_obj);
    shard->ops_completed++;
    PROF_RECORD(shard->prof.mset_prepare, start_cycles);
    return try_send_ack(engine, shard, stat);
  }

  cmd->is_mset = true;
  cmd->txn = txn;
  cmd->stat = stat;
  cmd->new_obj = new_obj;
  cmd->old_obj = obj_ptr;      /* NULL for new keys */
  cmd->sub_idx = msg->sub_idx;
  cmd->total_acks = 1;         /* counts raw pairs; incremented by duplicates */
  cmd->origin_shard = msg->shard_owner;
  cmd->req_nb = NULL;
  cmd->hash56 = hash56;

  /* Link into mset_stat per-shard list for FIN phase */
  cmd->next_in_mset = stat->shard_heads[shard->id];
  stat->shard_heads[shard->id] = cmd;

  if (!le) {
    /* ═══ CASE A: No lock — acquire ═══ */
    uint64_t t_ins2 = cycles_now();
    le = lock_manager_insert(lm, lock_key, hash56, cmd);
    PROF_RECORD(shard->prof.lm_insert, t_ins2);
    if (!le) {
      /* Lock manager full — rollback */
      stat->shard_heads[shard->id] = cmd->next_in_mset;
      bool tentative_freed = false;
      if (key_is_new) {
        struct kv_obj *tentative =
            ht_bucket_get(shard->table, key, klen, VAL_TYPE_STRING, 0);
        if (tentative) {
          ht_bucket_delete(shard->table, key, klen, VAL_TYPE_STRING);
          shard_obj_destroy(shard, tentative);
          tentative_freed = true;
        }
      }
      if (!tentative_freed)
        shard_obj_destroy(shard, new_obj);
      slab_obj_free(pool, cmd);
      shard->ops_completed++;
      PROF_RECORD(shard->prof.mset_prepare, start_cycles);
      return try_send_ack(engine, shard, stat);
    }

    cmd->le = le;
    ht_bucket_set_lock_status(shard->table, key, klen, VAL_TYPE_STRING, true);
    shard->ops_completed++;
    PROF_RECORD(shard->prof.mset_prepare, start_cycles);
    return try_send_ack(engine, shard, stat);
  }

  /* ═══ CASE B: Key already locked — wound-wait ═══ */
  struct cmd_info *incumbent = le->holder;

  /* Early commit: if incumbent's MSET is fully prepared, commit it now and
   * acquire the lock directly instead of going through wound-wait or queue. */
  if (incumbent && incumbent->is_mset) {
    uint32_t inc_ack =
        atomic_load_explicit(&incumbent->stat->ack, memory_order_acquire);
    if (inc_ack == incumbent->stat->total) {
      mset_try_early_commit(engine, shard, le, cur_ms);
      if (!le->holder) {
        /*
         * Key is now unlocked. cmd->old_obj still points to the pre-commit
         * object which incumbent's FIN has already destroyed. Re-anchor to
         * the object currently in HT (incumbent's committed new_obj) so our
         * FIN destroys the right object.
         */
        lock_key = ht_bucket_get(shard->table, key, klen, VAL_TYPE_STRING, 0);
        if (lock_key) {
          cmd->old_obj = lock_key;
        } else {
          /* Key absent from HT (defensive) — insert tentative */
          ht_bucket_put(shard->table, new_obj, VAL_TYPE_STRING, 0, HT_PUT_NONE);
          if (new_obj->expire_ms)
            ttl_index_node_push(&shard->ttl_idx, new_obj, new_obj->expire_ms);
          lock_key = new_obj;
          cmd->old_obj = NULL;
        }

        uint64_t t_ins2 = cycles_now();
        le = lock_manager_insert(lm, lock_key, hash56, cmd);
        PROF_RECORD(shard->prof.lm_insert, t_ins2);
        if (!le) {
          stat->shard_heads[shard->id] = cmd->next_in_mset;
          slab_obj_free(pool, cmd);
          shard->ops_completed++;
          PROF_RECORD(shard->prof.mset_prepare, start_cycles);
          return try_send_ack(engine, shard, stat);
        }
        cmd->le = le;
        ht_bucket_set_lock_status(shard->table, key, klen, VAL_TYPE_STRING, true);
        shard->ops_completed++;
        PROF_RECORD(shard->prof.mset_prepare, start_cycles);
        return try_send_ack(engine, shard, stat);
      }
      /* Key still locked — wound-wait against the new holder */
      incumbent = le->holder;
    }
  }

  bool can_preempt = false;

  if (incumbent && incumbent->is_mset) {
    uint32_t inc_ack =
        atomic_load_explicit(&incumbent->stat->ack, memory_order_acquire);
    PROF_RECORD(shard->prof.lm_preempt_attempt, start_cycles);
    if (inc_ack < incumbent->stat->total &&
        cmd->txn.conn_ptr != incumbent->txn.conn_ptr &&
        txn_id_cmp(&cmd->txn, &incumbent->txn) < 0)
      can_preempt = true;
  }

  if (can_preempt) {
    /* Wound: decrement incumbent's ack safely, inherit its old_obj */
    uint32_t expected = atomic_load_explicit(&incumbent->stat->ack, memory_order_acquire);
    uint32_t ack_before = expected;
    if (mset_debug_trace_enabled()) {
      fprintf(stderr,
              "[MSET_PREEMPT] shard=%u hash56=%lx le=%p old_holder=%p "
              "old_stat=%p old_ack_before=%u old_total=%u old_total_acks=%u "
              "new_cmd=%p new_stat=%p new_ack_before=%u old_obj=%p "
              "cmd_old_obj=%p cmd_new_obj=%p le_obj=%p\n",
              shard->id, (unsigned long)hash56, (void *)le,
              (void *)incumbent, (void *)incumbent->stat, ack_before,
              incumbent->stat->total, incumbent->total_acks, (void *)cmd,
              (void *)cmd->stat,
              atomic_load_explicit(&cmd->stat->ack, memory_order_acquire),
              (void *)incumbent->old_obj, (void *)cmd->old_obj,
              (void *)cmd->new_obj, (void *)le->obj_ptr);
    }
    while (expected > 0 && expected < incumbent->stat->total) {
      if (atomic_compare_exchange_weak_explicit(&incumbent->stat->ack, &expected,
                                                expected - 1,
                                                memory_order_acq_rel,
                                                memory_order_acquire)) {
        break;
      }
    }
    
    if (expected == 0 || expected >= incumbent->stat->total) {
      /* Race: incumbent is either not ACKed yet, or reached FIN while we were
       * deciding to wound. We must not underflow or preempt it here. */
      can_preempt = false;
    }
    if (mset_debug_trace_enabled()) {
      fprintf(stderr,
              "[MSET_PREEMPT_ACK] shard=%u old_stat=%p ack_before=%u "
              "ack_after=%u total=%u can_preempt=%d\n",
              shard->id, (void *)incumbent->stat, ack_before,
              atomic_load_explicit(&incumbent->stat->ack,
                                   memory_order_acquire),
              incumbent->stat->total, can_preempt);
    }
  }

  if (can_preempt) {
    PROF_RECORD(shard->prof.lm_preempt_success, cycles_now());

    cmd->old_obj = incumbent->old_obj;
    /* 
     * IMPORTANT: Update incumbent's old_obj to point to our new_obj.
     * When the incumbent eventually runs its FIN, it will replace our 
     * value and must free it.
     */
    incumbent->old_obj = cmd->new_obj;

    if (cmd->old_obj == NULL) {
      /*
       * Incumbent was creating a new key.
       * SAFETY: Do NOT swap HT and do NOT destroy anything.
       * We just inherit the NULL old_obj and wait for FIN to swap.
       */
    }

    uint64_t t_wq = cycles_now();
    lock_wq_add_mset(lm, le, incumbent);
    PROF_RECORD(shard->prof.lm_wq_add, t_wq);
    le->holder = cmd;
    cmd->le = le;
    if (mset_debug_trace_enabled()) {
      fprintf(stderr,
              "[MSET_PREEMPT_DONE] shard=%u le=%p old_holder=%p new_holder=%p "
              "old_stat=%p old_ack=%u new_stat=%p new_ack=%u "
              "old_old_obj=%p cmd_old_obj=%p le_obj=%p\n",
              shard->id, (void *)le, (void *)incumbent, (void *)cmd,
              (void *)incumbent->stat,
              atomic_load_explicit(&incumbent->stat->ack,
                                   memory_order_acquire),
              (void *)cmd->stat,
              atomic_load_explicit(&cmd->stat->ack, memory_order_acquire),
              (void *)incumbent->old_obj, (void *)cmd->old_obj,
              (void *)le->obj_ptr);
    }
    shard->ops_completed++;
    PROF_RECORD(shard->prof.mset_prepare, start_cycles);
    return try_send_ack(engine, shard, stat);
  }

  /* Wait: cmd goes into the wait queue */
  if (key_is_new)
    ht_bucket_delete(shard->table, key, klen, VAL_TYPE_STRING);
  cmd->le = le;
  uint64_t t_wq2 = cycles_now();
  lock_wq_add_mset(lm, le, cmd);
  PROF_RECORD(shard->prof.lm_wq_add, t_wq2);
  shard->ops_completed++;
  PROF_RECORD(shard->prof.mset_prepare, start_cycles);
  return NULL;
}

/* ── Public entry points ─────────────────────────────────────────────── */

/* ── Coordinator: dispatch MSET ──────────────────────────────────────── */

int mset_coordinator_dispatch(struct reactor *r, struct net_conn *c,
                              struct resp_parser *p, uint32_t pipeline_seq) {
  uint64_t start_cycles = cycles_now();
  uint32_t npairs = (uint32_t)(p->argc_got - 1) / 2;
  uint32_t my_id = r->shard->id;
  uint32_t nshards = r->engine->num_shards;
  struct shard_engine *engine = r->engine;
  struct slab_allocator *pool = r->shard->pool;
  struct net_buf *nb = c->rbuf_nb;

  /* Back-pressure: spin until in-flight MSET count drops below the limit.
   * While waiting, drain our inbox so ACKs/FIN_ACKs keep flowing and
   * the inflight counter can actually decrease. */
  uint64_t t_bp = cycles_now();
  uint64_t bp_spins = 0;
  uint64_t bp_drain_calls = 0;
  while (r->shard->mset_inflight >= r->shard->mset_max_concurrent) {
    shard_msg_drain(engine, r->shard, r->shard->proxy_cb, r);
    bp_drain_calls++;
    mset_debug_progress(r->shard, now_ms(), "backpressure");
    mset_debug_backpressure(r, c, now_ms());
    bp_spins++;
    __builtin_ia32_pause();
  }
  if (mixed_profile_enabled()) {
    uint64_t bp_cycles = cycles_now() - t_bp;
    uint64_t slow = mixed_profile_slow_cycles();
    if (bp_cycles >= slow || bp_spins > 0) {
      fprintf(stderr,
              "[MSET_BACKPRESSURE_PROFILE] shard=%u pidx=%u waitcy=%lu "
              "spins=%lu drain_calls=%lu inflight_after=%u max=%u\n",
              r->shard->id, pipeline_seq, (unsigned long)bp_cycles,
              (unsigned long)bp_spins, (unsigned long)bp_drain_calls,
              r->shard->mset_inflight, r->shard->mset_max_concurrent);
    }
  }
  PROF_RECORD(r->shard->prof.coord_backpressure_wait, t_bp);
  r->shard->mset_inflight++;

  /* Protect the request buffer against Use-After-Free.
   * Local key PREPAREs might trigger stat_free and unref this buffer while 
   * we are still looping through its arguments. */
  if (nb)
    net_buf_ref(nb);

  uint64_t t0 = cycles_now();
  struct mset_stat *stat =
      (struct mset_stat *)slab_obj_alloc(pool, sizeof(struct mset_stat));
  PROF_RECORD(r->shard->prof.coord_stat_alloc, t0);
  if (!stat) {
    if (nb)
      net_buf_unref(pool, nb);
    return -1;
  }

  uint64_t txn_ts = txn_now_ns();
  atomic_init(&stat->ack, 0);
  atomic_init(&stat->fin_ack, 0);
  stat->total = npairs;
  stat->txn.timestamp_ns = txn_ts;
  stat->txn.conn_ptr = c;
  stat->txn.pipeline_idx = pipeline_seq;
  stat->conn_ptr = c;
  stat->conn_generation = c->generation;
  stat->pipeline_idx = pipeline_seq;
  stat->coordinator_id = (uint8_t)my_id;
  stat->batch_cleanup_head = NULL;
  stat->start_cycles = cycles_now();
  stat->start_ms = now_ms();
  stat->last_debug_ms = 0;
  atomic_init(&stat->dbg_ack_last_ms, 0);
  atomic_init(&stat->dbg_on_ack_ms, 0);
  atomic_init(&stat->dbg_broadcast_fin_ms, 0);
  atomic_init(&stat->dbg_run_fin_ms, 0);
  atomic_init(&stat->dbg_send_fin_remote_ms, 0);
  atomic_init(&stat->dbg_on_fin_ms, 0);
  atomic_init(&stat->dbg_send_fin_ack_ms, 0);
  atomic_init(&stat->dbg_on_fin_ack_ms, 0);
  atomic_init(&stat->dbg_bfs_enqueue_ms, 0);
  atomic_init(&stat->dbg_bfs_pop_ms, 0);
  atomic_init(&stat->dbg_bfs_done_ms, 0);
  atomic_init(&stat->dbg_bfs_processed, 0);
  atomic_init(&stat->dbg_bfs_cascade, 0);
  memset(stat->shard_heads, 0, sizeof(stat->shard_heads));

  /* Hold a ref on the input buffer until all PREPARE memcpy's are done.
   * Released in stat_free (via mset_run_fin or mset_on_fin_ack). */
  stat->coord_rbuf_nb = c->rbuf_nb;
  if (c->rbuf_nb)
    net_buf_ref(c->rbuf_nb);

  /* Store stat in pipeline slot for ACK handler retrieval */
  struct net_pipeline_slot *ps =
      &c->proxy.slots[pipeline_seq & (c->proxy.cap - 1)];
  ps->kv_obj_ptr = stat;
  ps->parent_cmd_id = MSG_CMD_MSET_PART;
  if (mset_debug_enabled()) {
    ps->debug_mset_stat = stat;
    ps->debug_mset_start_ms = stat->start_ms;
  }

  uint32_t wake_mask = 0;

  /* Stack arrays: O(1) access by shard_id during dispatch, no heap alloc. */
  uint32_t            counts[MAX_SHARDS];
  struct mset_batch  *batch_ptrs[MAX_SHARDS];
  memset(counts,     0, nshards * sizeof(counts[0]));
  memset(batch_ptrs, 0, nshards * sizeof(batch_ptrs[0]));

  /* ── Pass 1: count remote keys per shard ── */
  for (uint32_t i = 0; i < npairs; i++) {
    uint64_t t1 = cycles_now();
    uint32_t target = shard_for_key(p->argv[1 + i * 2], p->arglen[1 + i * 2], nshards);
    PROF_RECORD(r->shard->prof.coord_hash, t1);
    if (target != my_id)
      counts[target]++;
  }

  /* ── Alloc: one mset_batch per remote shard, chain into stat ── */
  for (uint32_t sid = 0; sid < nshards; sid++) {
    if (!counts[sid]) continue;
    size_t alloc_sz = sizeof(struct mset_batch) +
                      counts[sid] * sizeof(struct mset_batch_entry);
    struct mset_batch *b = slab_obj_alloc(pool, alloc_sz);
    if (!b) {
      /* OOM: stat_free_real walks batch_cleanup_head and frees what's there */
      ps->kv_obj_ptr = NULL;
      stat_free_real(engine, r->shard, stat);
      if (nb) net_buf_unref(pool, nb);
      return -1;
    }
    b->count         = counts[sid];
    b->cleanup_next  = stat->batch_cleanup_head;
    stat->batch_cleanup_head = b;
    batch_ptrs[sid]  = b;
    counts[sid]      = 0; /* reuse as fill cursor in pass 2 */
  }

  /* ── Pass 2: local keys inline, remote keys fill batch entries ── */
  for (uint32_t i = 0; i < npairs; i++) {
    const char *key = p->argv[1 + i * 2];
    size_t klen     = p->arglen[1 + i * 2];
    const char *val = p->argv[2 + i * 2];
    size_t vlen     = p->arglen[2 + i * 2];
    uint32_t target = shard_for_key(key, klen, nshards);

    if (target == my_id) {
      struct spsc_message msg = {
          .op              = MSG_PACK_OP(MSG_SYS_REQ, MSG_CMD_MSET_KEY),
          .shard_owner     = (uint8_t)my_id,
          .conn_ptr        = c,
          .conn_generation = c->generation,
          .pipeline_idx    = pipeline_seq,
          .sub_idx         = i,
          .key_ptr         = (void *)key,
          .key_len         = (uint32_t)klen,
          .val_ptr         = (void *)val,
          .val_len         = (uint32_t)vlen,
          .req_nb          = c->rbuf_nb,
          .reply_int       = (int64_t)txn_ts,
          .kv_obj_ptr      = stat,
      };
      uint64_t t_local = cycles_now();
      mset_on_prepare(engine, r->shard, &msg, now_ms());
      PROF_RECORD(r->shard->prof.coord_local_exec, t_local);
    } else {
      uint32_t idx = counts[target]++;              /* O(1) — stack array  */
      batch_ptrs[target]->entries[idx] = (struct mset_batch_entry){
          .key = key, .klen = klen,
          .val = val, .vlen = vlen,
          .sub_idx = i,
      };
    }
  }

  /* ── Pass 3: send ONE batch message per remote shard ── */
  for (uint32_t sid = 0; sid < nshards; sid++) {
    if (!batch_ptrs[sid]) continue;
    struct spsc_message bmsg = {
        .op              = MSG_PACK_OP(MSG_SYS_REQ, MSG_CMD_MSET_KEY_BATCH),
        .shard_owner     = (uint8_t)my_id,
        .conn_ptr        = c,
        .conn_generation = c->generation,
        .pipeline_idx    = pipeline_seq,
        .key_ptr         = batch_ptrs[sid], /* points to mset_batch header  */
        .reply_int       = (int64_t)txn_ts,
        .kv_obj_ptr      = stat,
    };
    struct spsc_queue *q = &engine->queues[my_id * nshards + sid];
    uint64_t t2 = cycles_now();
    while (!spsc_queue_push(q, &bmsg))
      __builtin_ia32_pause();
    PROF_RECORD(r->shard->prof.coord_queue_wait, t2);
    wake_mask |= (1U << sid);
  }

  if (wake_mask && engine->wake_fds) {
    uint64_t t3 = cycles_now();
    for (uint32_t sid = 0; sid < nshards; sid++) {
      if ((wake_mask & (1U << sid)) && engine->wake_fds[sid] >= 0) {
        uint64_t one = 1;
        (void)write(engine->wake_fds[sid], &one, sizeof(one));
      }
    }
    PROF_RECORD(r->shard->prof.wake_write, t3);
  }

  PROF_RECORD(r->shard->prof.mset_dispatch, start_cycles);
  mset_debug_progress(r->shard, now_ms(), "dispatch");

  if (nb)
    net_buf_unref(pool, nb);

  return 0;
}

/* ── mset_on_prepare: entry point for MSG_CMD_MSET_KEY ──────────────── */

void mset_on_prepare(struct shard_engine *engine, struct shard *shard,
                     struct spsc_message *msg, uint64_t cur_ms) {
  struct mset_stat *ready = mset_prepare_key(engine, shard, msg, cur_ms);
  if (ready) {
    mset_broadcast_fin(engine, shard, ready);
  }
}

/* ── mset_on_prepare_batch: entry point for MSG_CMD_MSET_KEY_BATCH ──── */

void mset_on_prepare_batch(struct shard_engine *engine, struct shard *shard,
                            struct spsc_message *msg, uint64_t cur_ms) {
  PROF_RECORD(shard->prof.mset_prepare_queue_latency, msg->sent_cycles);
  struct mset_batch *batch = (struct mset_batch *)msg->key_ptr;
  bool mix_prof = mixed_profile_enabled();
  uint64_t batch_start = mix_prof ? cycles_now() : 0;
  uint64_t max_entry_cycles = 0;
  uint32_t slow_idx = 0;
  uint32_t ready_count = 0;
  uint64_t early_before =
      mix_prof && shard->id < MAX_SHARDS ? mixed_early_commit_count[shard->id] : 0;

  for (uint32_t i = 0; i < batch->count; i++) {
    struct spsc_message key_msg = {
        .op              = MSG_PACK_OP(MSG_SYS_REQ, MSG_CMD_MSET_KEY),
        .shard_owner     = msg->shard_owner,
        .conn_ptr        = msg->conn_ptr,
        .conn_generation = msg->conn_generation,
        .pipeline_idx    = msg->pipeline_idx,
        .sub_idx         = batch->entries[i].sub_idx,
        .key_ptr         = (void *)batch->entries[i].key,
        .key_len         = (uint32_t)batch->entries[i].klen,
        .val_ptr         = (void *)batch->entries[i].val,
        .val_len         = (uint32_t)batch->entries[i].vlen,
        .reply_int       = msg->reply_int,  /* txn_ts */
        .kv_obj_ptr      = msg->kv_obj_ptr, /* stat   */
    };
    uint64_t entry_start = mix_prof ? cycles_now() : 0;
    struct mset_stat *ready = mset_prepare_key(engine, shard, &key_msg, cur_ms);
    if (ready) {
      ready_count++;
      mset_broadcast_fin(engine, shard, ready);
    }
    if (mix_prof) {
      uint64_t entry_cycles = cycles_now() - entry_start;
      if (entry_cycles > max_entry_cycles) {
        max_entry_cycles = entry_cycles;
        slow_idx = i;
      }
    }
  }
  if (mix_prof) {
    uint64_t total_cycles = cycles_now() - batch_start;
    uint64_t slow = mixed_profile_slow_cycles();
    uint64_t early_after =
        shard->id < MAX_SHARDS ? mixed_early_commit_count[shard->id] : early_before;
    if (total_cycles >= slow || max_entry_cycles >= slow) {
      fprintf(stderr,
              "[MSET_BATCH_PROFILE] shard=%u sender=%u count=%u totalcy=%lu "
              "max_entrycy=%lu slow_idx=%u ready=%u early=%lu\n",
              shard->id, msg->shard_owner, batch->count,
              (unsigned long)total_cycles, (unsigned long)max_entry_cycles,
              slow_idx, ready_count,
              (unsigned long)(early_after - early_before));
    }
  }
  /* batch allocation is owned by coordinator; freed in stat_free_real */
}

/* ── mset_on_fin: entry point for MSG_CMD_MSET_FIN ───────────────────── */

void mset_on_fin(struct shard_engine *engine, struct shard *shard,
                 struct spsc_message *msg, uint64_t cur_ms) {
  struct mset_stat *stat = (struct mset_stat *)msg->kv_obj_ptr;
  if (!stat)
    return;
  atomic_store_explicit(&stat->dbg_on_fin_ms, cur_ms, memory_order_relaxed);
  mset_debug_lifecycle(shard, stat, "on_fin", cur_ms, msg->shard_owner);
  mset_run_fin(engine, shard, stat, cur_ms);
}

/* ── mset_on_ack: entry point for MSG_CMD_MSET_ACK ───────────────────── */

/*
 * Called on the coordinator shard when all exec shards have completed PREPARE.
 * mset_broadcast_fin handles both the remote FIN dispatch and local FIN run.
 * The pipeline slot NULL-check guards against a rare race where mset_on_prepare
 * already broadcast FIN (coordinator-is-last path) before this ACK was drained.
 */
void mset_on_ack(struct shard_engine *engine, struct shard *shard,
                 struct spsc_message *ack_msg) {
  PROF_RECORD(shard->prof.mset_ack_queue_latency, ack_msg->sent_cycles);
  uint64_t start_cycles = cycles_now();
  struct net_conn *c = (struct net_conn *)ack_msg->conn_ptr;

  if (!c || c->generation != ack_msg->conn_generation)
    return;

  uint32_t pidx = ack_msg->pipeline_idx;
  struct net_pipeline_slot *ps = &c->proxy.slots[pidx & (c->proxy.cap - 1)];

  struct mset_stat *stat = (struct mset_stat *)ps->kv_obj_ptr;
  if (!stat) {
    if (mset_debug_enabled()) {
      fprintf(stderr,
              "[MSET_ON_ACK_EMPTY] ts_ms=%lu shard=%u conn=%p pidx=%u "
              "slot_state=%u slot_cmd=%u from=%u\n",
              (unsigned long)now_ms(), shard->id, (void *)c, pidx,
              (uint32_t)ps->state, ps->parent_cmd_id, ack_msg->shard_owner);
    }
    return;
  }

  ps->kv_obj_ptr = NULL;
  atomic_store_explicit(&stat->dbg_on_ack_ms, now_ms(), memory_order_relaxed);
  mset_debug_lifecycle(shard, stat, "on_ack", now_ms(), ack_msg->shard_owner);
  mset_broadcast_fin(engine, shard, stat);

  PROF_RECORD(shard->prof.mset_ack, start_cycles);
}

/* ── mset_on_fin_ack: entry point for MSG_CMD_MSET_FIN_ACK ──────────── */

/*
 * Called on the coordinator shard when the last remote shard signals that
 * FIN is globally complete (fin_ack == total across all shards).
 * Exactly ONE FIN_ACK arrives per MSET — no partial counts, no refcount math.
 */
void mset_on_fin_ack(struct shard_engine *engine, struct shard *shard,
                     struct spsc_message *msg) {
  (void)engine;
  uint64_t start_cycles = cycles_now();
  struct mset_stat *stat = (struct mset_stat *)msg->kv_obj_ptr;
  if (!stat)
    return;

  atomic_store_explicit(&stat->dbg_on_fin_ack_ms, now_ms(),
                        memory_order_relaxed);
  mset_debug_lifecycle(shard, stat, "on_fin_ack", now_ms(), msg->shard_owner);

  if (mset_debug_enabled()) {
    uint64_t cur_dbg_ms = now_ms();
    uint64_t age_ms = mset_stat_age_ms(stat, cur_dbg_ms);
    if (age_ms >= mset_debug_slow_ms()) {
      mset_debug_print_timeline("fin_ack_slow", shard, stat, cur_dbg_ms);
      fprintf(stderr,
              "[MSET_FIN_ACK_SLOW] ts_ms=%lu shard=%u stat=%p age_ms=%lu from=%u "
              "pidx=%u total=%u ack=%u fin_ack=%u inflight=%u\n",
              (unsigned long)cur_dbg_ms, shard->id, (void *)stat,
              (unsigned long)age_ms,
              msg->shard_owner, stat->pipeline_idx, stat->total,
              atomic_load_explicit(&stat->ack, memory_order_acquire),
              atomic_load_explicit(&stat->fin_ack, memory_order_acquire),
              shard->mset_inflight);
    }
  }

  mset_set_ok_reply(shard, stat, "fin_ack");
  uint64_t e2e = stat->start_cycles;
  PROF_RECORD_HIST(shard->prof.mset_total_e2e, shard->prof.mset_e2e_hist, e2e);
  stat_free_real(engine, shard, stat);

  PROF_RECORD(shard->prof.mset_fin_ack, start_cycles);
}

/* ── Regular command lock check ──────────────────────────────────────── */

bool mset_check_lock_defer(struct shard *shard, struct spsc_message *msg,
                           const void *key, size_t klen) {
  bool prof = mixed_profile_enabled();
  static uint64_t lock_hits[MAX_SHARDS];
  static uint64_t lock_no_le[MAX_SHARDS];
  static uint64_t lock_no_holder[MAX_SHARDS];
  static uint64_t lock_early_unlock[MAX_SHARDS];
  static uint64_t lock_early_still_locked[MAX_SHARDS];
  static uint64_t lock_deferred[MAX_SHARDS];
  static uint64_t set_deferred[MAX_SHARDS];
  static uint64_t set_inbox_q_sum[MAX_SHARDS];
  static uint64_t set_inbox_q_max[MAX_SHARDS];
  static uint64_t last_summary_ms[MAX_SHARDS];

  if (!ht_bucket_is_locked(shard->table, key, klen, VAL_TYPE_STRING))
    return false;
  if (prof)
    lock_hits[shard->id]++;

  uint64_t meta = ht_meta_pack(ht_key_hash(key, klen), VAL_TYPE_STRING);
  uint64_t hash56 = meta >> 8;
  struct kv_obj *obj_ptr =
      ht_bucket_get(shard->table, key, klen, VAL_TYPE_STRING, 0);

  struct lock_manager *lm = &shard->lm;
  struct lock_entry *le = lock_manager_lookup(lm, obj_ptr, hash56);
  if (!le) {
    if (prof)
      lock_no_le[shard->id]++;
    return false;
  }

  if (!le->holder) {
    if (prof)
      lock_no_holder[shard->id]++;
    return false;
  }

  /* Early commit: if MSET holder is fully prepared, commit it now so this
   * command doesn't need to queue at all. */
  uint64_t t_early = prof ? cycles_now() : 0;
  bool early = mset_try_early_commit(shard->engine, shard, le, now_ms());
  if (early) {
    uint64_t early_cycles = prof ? cycles_now() - t_early : 0;
    if (!ht_bucket_is_locked(shard->table, key, klen, VAL_TYPE_STRING)) {
      if (prof) {
        lock_early_unlock[shard->id]++;
        if (early_cycles >= mixed_profile_slow_cycles())
          fprintf(stderr,
                  "[MSET_MIXED_EARLY] shard=%u cmd=%u unlocked=1 cycles=%lu\n",
                  shard->id, MSG_GET_CMD(msg->op),
                  (unsigned long)early_cycles);
      }
      return false;  /* key unlocked — caller executes command normally */
    }
    if (prof) {
      lock_early_still_locked[shard->id]++;
      if (early_cycles >= mixed_profile_slow_cycles())
        fprintf(stderr,
                "[MSET_MIXED_EARLY] shard=%u cmd=%u unlocked=0 cycles=%lu\n",
                shard->id, MSG_GET_CMD(msg->op), (unsigned long)early_cycles);
    }
    /* Key still locked (new holder from wait queue drain) — re-lookup */
    obj_ptr = ht_bucket_get(shard->table, key, klen, VAL_TYPE_STRING, 0);
    le = lock_manager_lookup(lm, obj_ptr, hash56);
    if (!le || !le->holder)
      return false;
  }

  struct cmd_info *cmd = cmd_info_alloc(shard->pool);
  if (!cmd)
    return false;

  cmd->is_mset = false;
  cmd->msg = *msg;
  cmd->defer_cycles = cycles_now();
  cmd->origin_shard = msg->shard_owner;
  cmd->req_nb = msg->req_nb;
  if (cmd->req_nb)
    net_buf_ref(cmd->req_nb);

  lock_wq_add_regular(lm, le, cmd);
  if (prof) {
    uint64_t cur_ms = now_ms();
    uint16_t cmd_op = MSG_GET_CMD(msg->op);
    uint64_t inbox_q = msg->sent_cycles ? cmd->defer_cycles - msg->sent_cycles : 0;
    lock_deferred[shard->id]++;
    if (mixed_is_set_cmd(cmd_op)) {
      set_deferred[shard->id]++;
      set_inbox_q_sum[shard->id] += inbox_q;
      if (inbox_q > set_inbox_q_max[shard->id])
        set_inbox_q_max[shard->id] = inbox_q;
      if (inbox_q >= mixed_profile_slow_cycles()) {
        fprintf(stderr,
                "[MSET_MIXED_SET_DEFER] ts_ms=%lu shard=%u cmd=%s origin=%u "
                "pidx=%u inbox_qcy=%lu lock_hits=%lu deferred=%lu\n",
                (unsigned long)cur_ms, shard->id, mixed_cmd_name(cmd_op),
                cmd->origin_shard, msg->pipeline_idx, (unsigned long)inbox_q,
                (unsigned long)lock_hits[shard->id],
                (unsigned long)lock_deferred[shard->id]);
      }
    }
    if (cur_ms - last_summary_ms[shard->id] >= 1000) {
      last_summary_ms[shard->id] = cur_ms;
      uint64_t sdef = set_deferred[shard->id];
      uint64_t sqavg = sdef ? set_inbox_q_sum[shard->id] / sdef : 0;
      fprintf(stderr,
              "[MSET_MIXED_LOCK_SUMMARY] ts_ms=%lu shard=%u hits=%lu "
              "no_le=%lu no_holder=%lu early_unlock=%lu "
              "early_still_locked=%lu deferred=%lu set_deferred=%lu "
              "set_inbox_q_avg=%lu set_inbox_q_max=%lu\n",
              (unsigned long)cur_ms, shard->id,
              (unsigned long)lock_hits[shard->id],
              (unsigned long)lock_no_le[shard->id],
              (unsigned long)lock_no_holder[shard->id],
              (unsigned long)lock_early_unlock[shard->id],
              (unsigned long)lock_early_still_locked[shard->id],
              (unsigned long)lock_deferred[shard->id],
              (unsigned long)set_deferred[shard->id],
              (unsigned long)sqavg,
              (unsigned long)set_inbox_q_max[shard->id]);
    }
  }
  return true;
}
