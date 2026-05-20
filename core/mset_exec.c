#include "mset_exec.h"
#include "lock_manager.h"
#include "txn.h"

#include "../core/message.h"
#include "../core/object.h"
#include "../core/routing.h"
#include "../core/shard.h"
#include "../core/snapshot.h"
#include "../hashtable/htable.h"
#include "../mem/mem_api.h"
#include "../mem/mem_pool.h"
#include "../net/conn.h"
#include "../net/reactor.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── Forward declarations ────────────────────────────────────────────── */

extern enum proxy_exec_result
proxy_exec(struct shard *s, struct spsc_message *msg, uint64_t cur);

static int mset_coordinator_dispatch_argv(struct reactor *r, struct net_conn *c,
                                          uint32_t pipeline_seq, uint32_t argc,
                                          char **argv, size_t *arglen,
                                          struct net_buf *req_nb);

/* ── Low-level helpers ───────────────────────────────────────────────── */


static inline uint64_t now_ms(void) {
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return (uint64_t)ts.tv_sec * 1000ULL + (uint64_t)ts.tv_nsec / 1000000ULL;
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

  if (shard->mark_conn_dirty_cb)
    shard->mark_conn_dirty_cb(shard->mark_conn_dirty_ctx, c, reason);
  return true;
}

static void mset_complete_regular_reply(struct shard_engine *engine,
                                        struct shard *shard,
                                        struct cmd_info *ci) {
  uint16_t cmd_op = MSG_GET_CMD(ci->msg.op);

  if (ci->origin_shard == shard->id) {
    struct net_conn *c = (struct net_conn *)ci->msg.u.reply.conn_ptr;
    if (!c) {
      if (ci->msg.u.reply.kv_obj_ptr)
        shard_obj_unref(shard, (struct kv_obj *)ci->msg.u.reply.kv_obj_ptr);
      if (ci->msg.u.reply.reply_type == REPLY_TYPE_BUF &&
          ci->msg.u.reply.reply_buf)
        slab_obj_free(shard->pool, ci->msg.u.reply.reply_buf);
      return;
    }

    struct net_pipeline_slot *slot =
        &c->proxy.slots[ci->msg.u.reply.pipeline_idx & (c->proxy.cap - 1)];
    if (slot->is_multi) {
      uint32_t sidx = ci->msg.u.reply.sub_idx;
      if (sidx < slot->multi_parts) {
        slot->multi_replies[sidx] = (uint8_t *)ci->msg.u.reply.reply_buf;
        slot->multi_reply_lens[sidx] = ci->msg.u.reply.reply_len;
        if (slot->multi_kv_objs)
          slot->multi_kv_objs[sidx] = ci->msg.u.reply.kv_obj_ptr;
        if (slot->multi_owners)
          slot->multi_owners[sidx] = (uint8_t)shard->id;
        slot->multi_replied++;
        if (slot->multi_replied >= slot->multi_parts)
          slot->state = PSLOT_DONE;
      }
    } else {
      slot->reply_type = ci->msg.u.reply.reply_type;
      slot->reply_int = ci->msg.u.reply.reply_int;
      slot->reply_data = (uint8_t *)ci->msg.u.reply.reply_buf;
      slot->reply_len = ci->msg.u.reply.reply_len;
      slot->kv_obj_ptr = ci->msg.u.reply.kv_obj_ptr;
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
      .u.reply = ci->msg.u.reply,
  };
  shard_send_msg_wake(engine, shard->id, ci->origin_shard, &reply);
  ci->req_nb = NULL; /* reply now owns the request-buffer ref */
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

  if (cmd_op == MSG_CMD_MGET_PART || cmd_op == MSG_CMD_DEL_PART) {
    key = msg->u.key_part_req.key_ptr;
    klen = msg->u.key_part_req.key_len;
  } else if (msg->u.regular_req.cmd.argc > 1) {
    key = msg->u.regular_req.cmd.argv[1];
    klen = msg->u.regular_req.cmd.arglen[1];
  }

  if (!key || !klen)
    return;

  struct kv_obj *cur =
      ht_bucket_get(shard->table, key, klen, VAL_TYPE_STRING, 0);
  if (cur) {
    le->obj_ptr = cur;
    ht_bucket_set_lock_status(shard->table, key, klen, VAL_TYPE_STRING, true);
  }
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
  o->pool_idx = total <= SMALL_ALLOC_MAX ? pool_idx_get(total) : NR_SMALL_POOLS;
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

  if (stat->coordinator_id == shard->id) {
    return stat;
  }

  /* Remote coordinator — send SPSC ACK */
  struct spsc_message ack = {
      .op = MSG_PACK_OP(MSG_SYS_REPLY, MSG_CMD_MSET_ACK),
      .shard_owner = (uint8_t)shard->id,
      .u.mset_ack =
          {
              .conn_ptr = stat->conn_ptr,
              .pipeline_idx = stat->pipeline_idx,
          },
  };
  shard_send_msg_wake(engine, shard->id, stat->coordinator_id, &ack);
  return NULL;
}

/*
 * Low-level free for mset_stat. Only called by the owner shard.
 */
static inline void stat_free_real(struct shard_engine *engine,
                                  struct shard *shard, struct mset_stat *stat) {
  (void)engine;
  shard->mset_inflight--;
  struct mset_batch *b = stat->batch_cleanup_head;
  while (b) {
    struct mset_batch *next = b->cleanup_next;
    slab_obj_free(shard->pool, b);
    b = next;
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
  for (uint32_t sid = 0; sid < nshards; sid++) {
    if (sid == my_id || !stat->shard_heads[sid])
      continue;
    struct spsc_message fin = {
        .op = MSG_PACK_OP(MSG_SYS_REQ, MSG_CMD_MSET_FIN),
        .shard_owner = (uint8_t)my_id,
        .u.mset_stat.stat = stat,
    };
    shard_send_msg_wake(engine, my_id, sid, &fin);
  }
}

static void mset_run_fin(struct shard_engine *engine, struct shard *shard,
                         struct mset_stat *initial_stat, uint64_t cur_ms);

static bool mset_try_early_commit(struct shard_engine *engine,
                                  struct shard *shard, struct lock_entry *le,
                                  uint64_t cur_ms);

/*
 * Detach stat from its pipeline slot, then drive the full FIN phase:
 * broadcasts FIN to remote shards and runs local FIN processing.
 * Safe to call from both mset_on_prepare (coordinator-is-last path)
 * and mset_on_ack (normal ACK-received path).
 */
static void mset_broadcast_fin(struct shard_engine *engine, struct shard *shard,
                               struct mset_stat *stat) {
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
          ci->old_obj = ht_bucket_get(shard->table, obj_key_get(ci->new_obj),
                                      ci->new_obj->key_len, VAL_TYPE_STRING, 0);

          struct mset_stat *ready = try_send_ack(engine, shard, ci->stat);

          /* If this was the last command in group, free it */
          if (!wg->head) {
            le->queue_head = wg->queue_next;
            if (le->queue_head)
              le->queue_head->queue_prev = NULL;
            else
              le->queue_tail = NULL;
            wait_group_free(shard->pool, wg);
          }

          return ready;
        }

        /* Regular command: execute and reply */
        enum proxy_exec_result exec_rc = proxy_exec(shard, &ci->msg, cur_ms);
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
        enum proxy_exec_result exec_rc = proxy_exec(shard, &ci->msg, cur_ms);
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
      struct kv_obj *replaced =
          shard_bucket_put(shard, new_obj, VAL_TYPE_STRING, 0, HT_PUT_NONE);
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
    size_t total =
        sizeof(struct kv_obj) + new_obj->key_len + new_obj->val_len + 1;
    uint32_t obj_size =
        (total <= SMALL_ALLOC_MAX) ? POOL_CLASSES[pool_idx_get(total)] : 0;
    snap_obj_mark(&shard->snap, shard->mem, new_obj, obj_size);
  }

  /* 3. Release lock */
  if (le->holder == cmd) {
    le->holder = NULL;
  }

  /* 4. Drain wait queue — may promote a waiting MSET to new holder */
  struct mset_stat *next_ready =
      mset_drain_waitqueue(engine, shard, le, cur_ms);

  /* 5. Update HT lock bit */
  if (!le->holder) {
    bool has_waiters = (le->queue_head != NULL);
    if (!has_waiters) {
      for (uint32_t j = 0; j < le->num_shards && !has_waiters; j++)
        if (le->open_groups[j])
          has_waiters = true;
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
  /* Broadcast FIN to remote participant shards for the initial stat. */
  mset_send_fin_to_remotes(engine, shard, initial_stat);

  enum { MSET_FIN_QUEUE_INLINE = 256 };
  struct mset_stat *inline_queue[MSET_FIN_QUEUE_INLINE];
  struct mset_stat **queue = inline_queue;
  size_t queue_cap = MSET_FIN_QUEUE_INLINE;
  size_t queue_head = 0;
  size_t queue_tail = 0;

  queue[queue_tail++] = initial_stat;

  while (queue_head < queue_tail) {
    struct mset_stat *stat = queue[queue_head++];
    uint32_t n_processed = 0;

    struct cmd_info *cmd;
    while ((cmd = stat->shard_heads[shard->id]) != NULL) {
      stat->shard_heads[shard->id] = cmd->next_in_mset;

      uint32_t acks = cmd->total_acks;
      struct mset_stat *next_ready =
          mset_fin_one_cmd(engine, shard, cmd, cur_ms);
      n_processed += acks;

      if (next_ready) {
        /* try_send_ack only returns non-NULL for the coordinator, so
         * next_ready->coordinator_id == shard->id is always true here. */
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
      }
    }

    if (n_processed > 0) {
      uint32_t prev = atomic_fetch_add_explicit(&stat->fin_ack, n_processed,
                                                memory_order_acq_rel);
      if (prev + n_processed == stat->total) {
        /* This shard is the last to finish FIN globally. */
        if (stat->coordinator_id == (uint8_t)shard->id) {
          mset_set_ok_reply(shard, stat, "fin_local");
          stat_free_real(engine, shard, stat);
        } else {
          /* Remote shard: send the ONE FIN_ACK to coordinator. */
          struct spsc_message fack = {
              .op = MSG_PACK_OP(MSG_SYS_REPLY, MSG_CMD_MSET_FIN_ACK),
              .shard_owner = (uint8_t)shard->id,
              .u.mset_stat.stat = stat,
          };
          shard_send_msg_wake(engine, shard->id, stat->coordinator_id, &fack);
        }
      }
      /* Non-last shards: nothing to do. */
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
                                  struct shard *shard, struct lock_entry *le,
                                  uint64_t cur_ms) {
  if (!le->holder || !le->holder->is_mset)
    return false;

  struct mset_stat *stat = le->holder->stat;
  uint32_t ack = atomic_load_explicit(&stat->ack, memory_order_acquire);
  if (ack < stat->total)
    return false;

  /* ack == total: FIN is imminent — commit all local keys for this stat now */
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
  if (prev_fin + n_processed != stat->total)
    return true; /* not last globally — coordinator or other shards will close
                    out */

  /* Last globally: deliver +OK and free stat */
  if (stat->coordinator_id == (uint8_t)shard->id) {
    mset_set_ok_reply(shard, stat, "early_fin_local");
    stat_free_real(engine, shard, stat);
  } else {
    struct spsc_message fack = {
        .op = MSG_PACK_OP(MSG_SYS_REPLY, MSG_CMD_MSET_FIN_ACK),
        .shard_owner = (uint8_t)shard->id,
        .u.mset_stat.stat = stat,
    };
    shard_send_msg_wake(engine, shard->id, stat->coordinator_id, &fack);
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
static struct mset_stat *
mset_prepare_key(struct shard_engine *engine, struct shard *shard,
                 const char *key, size_t klen, const char *val, size_t vlen,
                 struct mset_stat *stat, uint32_t sub_idx, uint8_t origin_shard,
                 uint64_t cur_ms) {
  struct lock_manager *lm = &shard->lm;
  struct slab_allocator *pool = shard->pool;

  struct txn_id txn = stat->txn;

  uint64_t meta = ht_meta_pack(ht_key_hash(key, klen), VAL_TYPE_STRING);
  uint64_t hash56 = meta >> 8;

  struct kv_obj *obj_ptr =
      ht_bucket_get(shard->table, key, klen, VAL_TYPE_STRING, 0);

  /* Allocate tentative new object */
  struct kv_obj *new_obj = alloc_new_obj(key, klen, val, vlen);
  if (!new_obj) {
    shard->ops_completed++;
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
    struct kv_obj *replaced =
        shard_bucket_put(shard, new_obj, VAL_TYPE_STRING, 0, HT_PUT_NONE);
    if (replaced == (struct kv_obj *)(uintptr_t)-1) {
      /* HT full */
      shard_obj_destroy(shard, new_obj);
      shard->ops_completed++;
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
  struct lock_entry *le = lock_manager_lookup(lm, lock_key, hash56);

  if (le) {
    struct cmd_info *incumbent = le->holder;

    /* ── Duplicate at holder (same txn already holds the lock) ── */
    if (incumbent && incumbent->is_mset && txn_id_eq(&txn, &incumbent->txn)) {
      struct kv_obj *old_new_obj = incumbent->new_obj;

      /* For new keys: old_new_obj is the tentative in HT.
       * Swap our new_obj in so FIN finds the right object. */
      if (incumbent->old_obj == NULL) {
        shard_bucket_put(shard, new_obj, VAL_TYPE_STRING, 0, HT_PUT_NONE);
        le->obj_ptr = new_obj;
      }

      shard_obj_destroy(shard, old_new_obj);
      incumbent->new_obj = new_obj;
      incumbent->sub_idx = sub_idx;
      incumbent->total_acks++; /* this raw pair will be counted at FIN */
      shard->ops_completed++;
      return try_send_ack(engine, shard, stat);
    }

    /* ── Duplicate in wait queue (same txn waiting) ── */
    struct wait_group *dup_wg = NULL;
    struct cmd_info *dup_cmd = lock_wq_find_txn(le, &txn, &dup_wg);
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
        shard_bucket_put(shard, new_obj, VAL_TYPE_STRING, 0, HT_PUT_NONE);
        if (new_obj->expire_ms && new_obj->heap_idx == HEAP_IDX_NONE)
          ttl_index_node_push(&shard->ttl_idx, new_obj, new_obj->expire_ms);
      }
      if (old_tentative)
        shard_obj_destroy(shard, old_tentative);
      dup_cmd->new_obj = new_obj;
      dup_cmd->sub_idx = sub_idx;
      dup_cmd->total_acks++; /* this raw pair will be counted at FIN */
      shard->ops_completed++;
      return try_send_ack(engine, shard, stat);
    }
  }

  /* ── Allocate cmd_info ── */
  struct cmd_info *cmd = cmd_info_alloc(pool);
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
    return try_send_ack(engine, shard, stat);
  }

  cmd->is_mset = true;
  cmd->txn = txn;
  cmd->stat = stat;
  cmd->new_obj = new_obj;
  cmd->old_obj = obj_ptr; /* NULL for new keys */
  cmd->sub_idx = sub_idx;
  cmd->total_acks = 1; /* counts raw pairs; incremented by duplicates */
  cmd->origin_shard = origin_shard;
  cmd->req_nb = NULL;
  cmd->hash56 = hash56;

  /* Link into mset_stat per-shard list for FIN phase */
  cmd->next_in_mset = stat->shard_heads[shard->id];
  stat->shard_heads[shard->id] = cmd;

  if (!le) {
    /* ═══ CASE A: No lock — acquire ═══ */
    le = lock_manager_insert(lm, lock_key, hash56, cmd);
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
      return try_send_ack(engine, shard, stat);
    }

    cmd->le = le;
    ht_bucket_set_lock_status(shard->table, key, klen, VAL_TYPE_STRING, true);
    shard->ops_completed++;
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
          shard_bucket_put(shard, new_obj, VAL_TYPE_STRING, 0, HT_PUT_NONE);
          if (new_obj->expire_ms)
            ttl_index_node_push(&shard->ttl_idx, new_obj, new_obj->expire_ms);
          lock_key = new_obj;
          cmd->old_obj = NULL;
        }

        le = lock_manager_insert(lm, lock_key, hash56, cmd);
        if (!le) {
          stat->shard_heads[shard->id] = cmd->next_in_mset;
          slab_obj_free(pool, cmd);
          shard->ops_completed++;
          return try_send_ack(engine, shard, stat);
        }
        cmd->le = le;
        ht_bucket_set_lock_status(shard->table, key, klen, VAL_TYPE_STRING,
                                  true);
        shard->ops_completed++;
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
    if (inc_ack < incumbent->stat->total &&
        cmd->txn.conn_ptr != incumbent->txn.conn_ptr &&
        txn_id_cmp(&cmd->txn, &incumbent->txn) < 0)
      can_preempt = true;
  }

  if (can_preempt) {
    /* Wound: decrement incumbent's ack safely, inherit its old_obj */
    uint32_t expected =
        atomic_load_explicit(&incumbent->stat->ack, memory_order_acquire);
    while (expected > 0 && expected < incumbent->stat->total) {
      if (atomic_compare_exchange_weak_explicit(
              &incumbent->stat->ack, &expected, expected - 1,
              memory_order_acq_rel, memory_order_acquire)) {
        break;
      }
    }

    if (expected == 0 || expected >= incumbent->stat->total) {
      /* Race: incumbent is either not ACKed yet, or reached FIN while we were
       * deciding to wound. We must not underflow or preempt it here. */
      can_preempt = false;
    }
  }

  if (can_preempt) {
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

    lock_wq_add_mset(lm, le, incumbent);
    le->holder = cmd;
    cmd->le = le;
    shard->ops_completed++;
    return try_send_ack(engine, shard, stat);
  }

  /* Wait: cmd goes into the wait queue */
  if (key_is_new)
    ht_bucket_delete(shard->table, key, klen, VAL_TYPE_STRING);
  cmd->le = le;
  lock_wq_add_mset(lm, le, cmd);
  shard->ops_completed++;
  return NULL;
}

/* ── Public entry points ─────────────────────────────────────────────── */

static void mset_pending_state_free(struct shard *shard, struct net_conn *c) {
  if (!c)
    return;
  if (c->pending_argv) {
    slab_obj_free(shard->pool, c->pending_argv);
    c->pending_argv = NULL;
  }
  if (c->pending_arglen) {
    slab_obj_free(shard->pool, c->pending_arglen);
    c->pending_arglen = NULL;
  }
  
  c->pending_argc = 0;
  c->pending_pipeline_idx = 0;
  c->pending_reason = CONN_PENDING_NONE;
}

void mset_pending_conn_cancel(struct shard *shard, struct net_conn *c) {
  if (!shard || !c)
    return;

  if (c->pending_queued) {
    struct net_conn *prev = NULL;
    struct net_conn *cur = shard->mset_pending_conn_head;
    while (cur) {
      if (cur == c) {
        if (prev)
          prev->pending_next = cur->pending_next;
        else
          shard->mset_pending_conn_head = cur->pending_next;
        if (shard->mset_pending_conn_tail == cur)
          shard->mset_pending_conn_tail = prev;
        if (shard->mset_pending_conn_len > 0)
          shard->mset_pending_conn_len--;
        break;
      }
      prev = cur;
      cur = cur->pending_next;
    }
    c->pending_next = NULL;
    c->pending_queued = false;
  }

  if (c->pending_reason == CONN_PENDING_MSET_BP && c->proxy.slots &&
      c->proxy.cap) {
    struct net_pipeline_slot *ps =
        &c->proxy.slots[c->pending_pipeline_idx & (c->proxy.cap - 1)];
    if (ps->state == PSLOT_PENDING && ps->parent_cmd_id == MSG_CMD_MSET_PART &&
        ps->kv_obj_ptr == NULL) {
      ps->state = PSLOT_DONE;
    }
  }
  mset_pending_state_free(shard, c);
}

static int mset_pending_conn_enqueue(struct reactor *r, struct net_conn *c,
                                     struct resp_parser *p,
                                     uint32_t pipeline_seq) {
  struct shard *shard = r->shard;
  if (c->pending_reason != CONN_PENDING_NONE)
    return -1;

  char **argv =
      (char **)slab_obj_alloc(shard->pool, p->argc_got * sizeof(char *));
  size_t *arglen =
      (size_t *)slab_obj_alloc(shard->pool, p->argc_got * sizeof(size_t));
  if (!argv || !arglen) {
    if (argv)
      slab_obj_free(shard->pool, argv);
    if (arglen)
      slab_obj_free(shard->pool, arglen);
    return -1;
  }
  memcpy(argv, p->argv, p->argc_got * sizeof(char *));
  memcpy(arglen, p->arglen, p->argc_got * sizeof(size_t));

  struct net_pipeline_slot *ps =
      &c->proxy.slots[pipeline_seq & (c->proxy.cap - 1)];
  ps->state = PSLOT_PENDING;
  ps->parent_cmd_id = MSG_CMD_MSET_PART;
  ps->kv_obj_ptr = NULL;

  c->pending_reason = CONN_PENDING_MSET_BP;
  c->pending_pipeline_idx = pipeline_seq;
  c->pending_req_nb = c->rbuf_nb;
  c->pending_argc = p->argc_got;
  c->pending_argv = argv;
  c->pending_arglen = arglen;

  if (!c->pending_queued) {
    c->pending_next = NULL;
    if (shard->mset_pending_conn_tail)
      shard->mset_pending_conn_tail->pending_next = c;
    else
      shard->mset_pending_conn_head = c;
    shard->mset_pending_conn_tail = c;
    shard->mset_pending_conn_len++;
    shard->mset_pending_conn_enqueued++;
    if (shard->mset_pending_conn_len > shard->mset_pending_conn_max_len)
      shard->mset_pending_conn_max_len = shard->mset_pending_conn_len;
    c->pending_queued = true;
  }
  return 0;
}

void mset_pending_conn_drain(struct reactor *r, uint32_t budget) {
  if (!r || !r->shard || budget == 0)
    return;
  struct shard *shard = r->shard;

  while (budget-- > 0 && shard->mset_inflight < shard->mset_max_concurrent &&
         shard->mset_pending_conn_head) {
    struct net_conn *c = shard->mset_pending_conn_head;
    shard->mset_pending_conn_head = c->pending_next;
    if (!shard->mset_pending_conn_head)
      shard->mset_pending_conn_tail = NULL;
    if (shard->mset_pending_conn_len > 0)
      shard->mset_pending_conn_len--;
    c->pending_next = NULL;
    c->pending_queued = false;

    if (c->state == CONN_DEAD || c->pending_reason != CONN_PENDING_MSET_BP) {
      mset_pending_state_free(shard, c);
      continue;
    }

    uint32_t pipeline_idx = c->pending_pipeline_idx;
    uint32_t argc = c->pending_argc;
    char **argv = c->pending_argv;
    size_t *arglen = c->pending_arglen;
    struct net_buf *req_nb = c->pending_req_nb;
    c->pending_reason = CONN_PENDING_NONE;
    c->pending_pipeline_idx = 0;
    c->pending_argc = 0;
    c->pending_argv = NULL;
    c->pending_arglen = NULL;
    c->pending_req_nb = NULL;

    int rc = mset_coordinator_dispatch_argv(r, c, pipeline_idx, argc, argv,
                                            arglen, req_nb);
    shard->mset_pending_conn_resumed++;

    if (argv)
      slab_obj_free(shard->pool, argv);
    if (arglen)
      slab_obj_free(shard->pool, arglen);
   
    if (rc < 0 && c->proxy.slots && c->proxy.cap) {
      struct net_pipeline_slot *ps =
          &c->proxy.slots[pipeline_idx & (c->proxy.cap - 1)];
      ps->reply_type = REPLY_TYPE_ERR;
      ps->reply_data = (uint8_t *)"OOM";
      ps->reply_len = 3;
      ps->state = PSLOT_LOCAL;
    }

    if (shard->mark_conn_dirty_cb)
      shard->mark_conn_dirty_cb(shard->mark_conn_dirty_ctx, c,
                                rc < 0 ? "mset_pending_oom"
                                       : "mset_pending_resume");
  }
}

/* ── Coordinator: dispatch MSET ──────────────────────────────────────── */

int mset_coordinator_dispatch(struct reactor *r, struct net_conn *c,
                              struct resp_parser *p, uint32_t pipeline_seq) {
  if (r->shard->mset_inflight >= r->shard->mset_max_concurrent) {
    if (mset_pending_conn_enqueue(r, c, p, pipeline_seq) < 0)
      return -1;
    return 1;
  }
  return mset_coordinator_dispatch_argv(r, c, pipeline_seq, p->argc_got,
                                        p->argv, p->arglen, c->rbuf_nb);
}

static int mset_coordinator_dispatch_argv(struct reactor *r, struct net_conn *c,
                                          uint32_t pipeline_seq, uint32_t argc,
                                          char **argv, size_t *arglen,
                                          struct net_buf *req_nb) {
  uint32_t npairs = (uint32_t)(argc - 1) / 2;
  uint32_t my_id = r->shard->id;
  uint32_t nshards = r->engine->num_shards;
  struct shard_engine *engine = r->engine;
  struct slab_allocator *pool = r->shard->pool;
  struct net_buf *nb = req_nb;
  r->shard->mset_inflight++;

  /* Protect the request buffer against Use-After-Free.
   * Local key PREPAREs might trigger stat_free and unref this buffer while
   * we are still looping through its arguments. */

  struct mset_stat *stat =
      (struct mset_stat *)slab_obj_alloc(pool, sizeof(struct mset_stat));
  if (!stat) {
    r->shard->mset_inflight--;
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
  memset(stat->shard_heads, 0, sizeof(stat->shard_heads));

  /* Hold a ref on the input buffer until all PREPARE memcpy's are done.
   * Released in stat_free (via mset_run_fin or mset_on_fin_ack). */
  stat->coord_rbuf_nb = req_nb;

  /* Store stat in pipeline slot for ACK handler retrieval */
  struct net_pipeline_slot *ps =
      &c->proxy.slots[pipeline_seq & (c->proxy.cap - 1)];
  ps->kv_obj_ptr = stat;
  ps->parent_cmd_id = MSG_CMD_MSET_PART;

  uint64_t wake_mask = 0;

  /* Stack arrays: O(1) access by shard_id during dispatch, no heap alloc. */
  uint32_t counts[MAX_SHARDS];
  struct mset_batch *batch_ptrs[MAX_SHARDS];
  memset(counts, 0, nshards * sizeof(counts[0]));
  memset(batch_ptrs, 0, nshards * sizeof(batch_ptrs[0]));

  /* ── Pass 1: count remote keys per shard ── */
  for (uint32_t i = 0; i < npairs; i++) {
    uint32_t target =
        shard_for_key(argv[1 + i * 2], arglen[1 + i * 2], nshards);
    if (target != my_id)
      counts[target]++;
  }

  /* ── Alloc: one mset_batch per remote shard, chain into stat ── */
  for (uint32_t sid = 0; sid < nshards; sid++) {
    if (!counts[sid])
      continue;
    size_t alloc_sz = sizeof(struct mset_batch) +
                      counts[sid] * sizeof(struct mset_batch_entry);
    struct mset_batch *b = slab_obj_alloc(pool, alloc_sz);
    if (!b) {
      /* OOM: stat_free_real walks batch_cleanup_head and frees what's there */
      ps->kv_obj_ptr = NULL;
      stat_free_real(engine, r->shard, stat);
      
      return -1;
    }
    b->count = counts[sid];
    b->cleanup_next = stat->batch_cleanup_head;
    stat->batch_cleanup_head = b;
    batch_ptrs[sid] = b;
    counts[sid] = 0; /* reuse as fill cursor in pass 2 */
  }

  /* ── Pass 2: local keys inline, remote keys fill batch entries ── */
  for (uint32_t i = 0; i < npairs; i++) {
    const char *key = argv[1 + i * 2];
    size_t klen = arglen[1 + i * 2];
    const char *val = argv[2 + i * 2];
    size_t vlen = arglen[2 + i * 2];
    uint32_t target = shard_for_key(key, klen, nshards);

    if (target == my_id) {
      struct mset_stat *ready =
          mset_prepare_key(engine, r->shard, key, klen, val, vlen, stat, i,
                           (uint8_t)my_id, now_ms());
      if (ready)
        mset_broadcast_fin(engine, r->shard, ready);
    } else {
      uint32_t idx = counts[target]++; /* O(1) — stack array  */
      batch_ptrs[target]->entries[idx] = (struct mset_batch_entry){
          .key = key,
          .klen = klen,
          .val = val,
          .vlen = vlen,
          .sub_idx = i,
      };
    }
  }

  /* ── Pass 3: send ONE batch message per remote shard ── */
  for (uint32_t sid = 0; sid < nshards; sid++) {
    if (!batch_ptrs[sid])
      continue;
    struct spsc_message bmsg = {
        .op = MSG_PACK_OP(MSG_SYS_REQ, MSG_CMD_MSET_KEY_BATCH),
        .shard_owner = (uint8_t)my_id,
        .u.mset_prepare_batch =
            {
                .batch = batch_ptrs[sid],
                .stat = stat,
            },
    };
    shard_send_msg_deferred(engine, my_id, sid, &bmsg, &wake_mask);
  }

  if (wake_mask) {
    shard_flush_wakeup(engine, &wake_mask);
  }
  return 0;
}

/* ── mset_on_prepare: entry point for MSG_CMD_MSET_KEY ──────────────── */

void mset_on_prepare(struct shard_engine *engine, struct shard *shard,
                     struct spsc_message *msg, uint64_t cur_ms) {
  (void)engine;
  (void)shard;
  (void)msg;
  (void)cur_ms;
}

/* ── mset_on_prepare_batch: entry point for MSG_CMD_MSET_KEY_BATCH ──── */

void mset_on_prepare_batch(struct shard_engine *engine, struct shard *shard,
                           struct spsc_message *msg, uint64_t cur_ms) {
  struct mset_batch *batch = msg->u.mset_prepare_batch.batch;
  struct mset_stat *stat = msg->u.mset_prepare_batch.stat;

  for (uint32_t i = 0; i < batch->count; i++) {
    struct mset_stat *ready = mset_prepare_key(
        engine, shard, batch->entries[i].key, batch->entries[i].klen,
        batch->entries[i].val, batch->entries[i].vlen, stat,
        batch->entries[i].sub_idx, msg->shard_owner, cur_ms);
    if (ready)
      mset_broadcast_fin(engine, shard, ready);
  }
  /* batch allocation is owned by coordinator; freed in stat_free_real */
}

/* ── mset_on_fin: entry point for MSG_CMD_MSET_FIN ───────────────────── */

void mset_on_fin(struct shard_engine *engine, struct shard *shard,
                 struct spsc_message *msg, uint64_t cur_ms) {
  struct mset_stat *stat = msg->u.mset_stat.stat;
  if (!stat)
    return;
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
  (void)shard;
  struct net_conn *c = (struct net_conn *)ack_msg->u.mset_ack.conn_ptr;
  if (!c)
    return;

  uint32_t pidx = ack_msg->u.mset_ack.pipeline_idx;
  struct net_pipeline_slot *ps = &c->proxy.slots[pidx & (c->proxy.cap - 1)];

  struct mset_stat *stat = (struct mset_stat *)ps->kv_obj_ptr;
  if (!stat)
    return;

  ps->kv_obj_ptr = NULL;
  mset_broadcast_fin(engine, shard, stat);
}

/* ── mset_on_fin_ack: entry point for MSG_CMD_MSET_FIN_ACK ──────────── */

/*
 * Called on the coordinator shard when the last remote shard signals that
 * FIN is globally complete (fin_ack == total across all shards).
 * Exactly ONE FIN_ACK arrives per MSET — no partial counts, no refcount math.
 */
void mset_on_fin_ack(struct shard_engine *engine, struct shard *shard,
                     struct spsc_message *msg) {
  struct mset_stat *stat = msg->u.mset_stat.stat;
  if (!stat)
    return;

  mset_set_ok_reply(shard, stat, "fin_ack");
  stat_free_real(engine, shard, stat);
}

/* ── Regular command lock check ──────────────────────────────────────── */

bool mset_check_lock_defer(struct shard *shard, struct spsc_message *msg,
                           const void *key, size_t klen) {
  if (!ht_bucket_is_locked(shard->table, key, klen, VAL_TYPE_STRING))
    return false;

  uint64_t meta = ht_meta_pack(ht_key_hash(key, klen), VAL_TYPE_STRING);
  uint64_t hash56 = meta >> 8;
  struct kv_obj *obj_ptr =
      ht_bucket_get(shard->table, key, klen, VAL_TYPE_STRING, 0);

  struct lock_manager *lm = &shard->lm;
  struct lock_entry *le = lock_manager_lookup(lm, obj_ptr, hash56);
  if (!le)
    return false;

  if (!le->holder)
    return false;

  /* Early commit: if MSET holder is fully prepared, commit it now so this
   * command doesn't need to queue at all. */
  bool early = mset_try_early_commit(shard->engine, shard, le, now_ms());
  if (early) {
    if (!ht_bucket_is_locked(shard->table, key, klen, VAL_TYPE_STRING))
      return false; /* key unlocked — caller executes command normally */

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
  cmd->origin_shard = msg->shard_owner;
  uint16_t req_cmd_op = MSG_GET_CMD(msg->op);
  if (req_cmd_op == MSG_CMD_MGET_PART || req_cmd_op == MSG_CMD_DEL_PART)
    cmd->req_nb = msg->u.key_part_req.req_nb;
  else
    cmd->req_nb = msg->u.regular_req.req_nb;

  lock_wq_add_regular(lm, le, cmd);
  return true;
}
