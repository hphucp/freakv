#include "shard.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "../mem/mem_api.h"
#include "../mem/mem_buddy.h"
#include "../mem/mem_heap.h"
#include "../mem/mem_internal.h"
#include "../mem/mem_pool.h"
#include "../net/conn.h"
#include "eviction.h"
#include "htable.h"
#include "mset_exec.h"
#include <ctype.h>

static bool mixed_profile_enabled(void) {
#if !FREAKV_MIXED_PROFILE
  return false;
#else
  static int cached = -1;
  if (cached < 0) {
    const char *v = getenv("MSET_MIXED_PROFILE");
    cached = (v && v[0] && strcmp(v, "0") != 0) ? 1 : 0;
  }
  return cached != 0;
#endif
}

static uint64_t mixed_profile_slow_cycles(void) {
  static uint64_t cached = 0;
  if (!cached) {
    const char *v = getenv("MSET_MIXED_SLOW_US");
    uint64_t us = v && v[0] ? strtoull(v, NULL, 10) : 1000;
    if (!us)
      us = 1000;
    cached = us * 3000ULL; /* approximate on common 3GHz dev CPUs */
  }
  return cached;
}

static const char *mixed_cmd_name(uint16_t cmd) {
  switch (cmd) {
  case MSG_CMD_SET:
    return "SET";
  case MSG_CMD_GET:
    return "GET";
  case MSG_CMD_MGET_PART:
    return "MGET_PART";
  case MSG_CMD_DEL_PART:
    return "DEL_PART";
  case MSG_CMD_MSET_KEY:
    return "MSET_KEY";
  case MSG_CMD_MSET_KEY_BATCH:
    return "MSET_BATCH";
  case MSG_CMD_MSET_FIN:
    return "MSET_FIN";
  default:
    return "OTHER";
  }
}

/* ── LRU helpers (non-static — used by mset_exec.c) ──────────────────── */

void lru_node_remove(struct shard *s, struct kv_obj *o) {
  uint32_t p = o->pool_idx;

  if (o->lru_prev)
    o->lru_prev->lru_next = o->lru_next;
  else if (s->lru_head[p] == o)
    s->lru_head[p] = o->lru_next;

  if (o->lru_next)
    o->lru_next->lru_prev = o->lru_prev;
  else if (s->lru_tail[p] == o)
    s->lru_tail[p] = o->lru_prev;

  o->lru_prev = NULL;
  o->lru_next = NULL;
}

void lru_node_prepend(struct shard *s, struct kv_obj *o) {
  uint32_t p = o->pool_idx;

  lru_node_remove(s, o);

  o->lru_prev = NULL;
  o->lru_next = s->lru_head[p];

  if (s->lru_head[p])
    s->lru_head[p]->lru_prev = o;
  else
    s->lru_tail[p] = o;

  s->lru_head[p] = o;
}

static inline void lru_node_touch(struct shard *s, struct kv_obj *o) {
  lru_node_prepend(s, o);
}

/* ── LRU external declarations (in shard.h or forward) ───────────────── */
/* void lru_node_remove(struct shard *s, struct kv_obj *o);               */
/* void lru_node_prepend(struct shard *s, struct kv_obj *o);              */

/* ── Constants ────────────────────────────────────────────────────────── */

#define INITIAL_BUCKETS 8192
/* ── CPU helpers ──────────────────────────────────────────────────────── */

uint32_t num_cpu_count(void) {
  long n = sysconf(_SC_NPROCESSORS_ONLN);
  return (n > 0) ? (uint32_t)n : 1;
}

/* ── Object lifecycle ─────────────────────────────────────────────────── */

static void shard_obj_free(struct shard *s, struct kv_obj *o) {
  if (o->val_len == 0) {
    void *ptr = obj_val_ptr_get(o);
    if (ptr)
      mem_free(ptr);
  }

  uint64_t t0 = PROF_NOW();
  mem_free(o);
  if (s->snap.interval_ms == 0) {
    PROF_RECORD(s->prof.free_std, t0);
  } else {
    PROF_RECORD(s->prof.free_snap, t0);
  }
}

void shard_obj_ref(struct kv_obj *o) {
  atomic_fetch_add_explicit(&o->refcount, 1, memory_order_relaxed);
}

void shard_obj_unref(struct shard *s, struct kv_obj *o) {
  if (atomic_fetch_sub_explicit(&o->refcount, 1, memory_order_release) == 1) {
    atomic_thread_fence(memory_order_acquire);
    if (o->marked_deletion) {
      shard_obj_free(s, o);
    }
  }
}

void shard_obj_destroy(struct shard *s, struct kv_obj *o) {
  if (!o)
    return;

  o->marked_deletion = 1;

  size_t total = sizeof(struct kv_obj) + o->key_len + o->val_len + 1;
  uint32_t obj_size =
      total <= SMALL_ALLOC_MAX ? POOL_CLASSES[pool_idx_get(total)] : 0;

  if (g_snapshot_active) {
    uint64_t t_m = PROF_NOW();
    snap_obj_mark(&s->snap, s->mem, o, obj_size);
    PROF_RECORD(s->prof.mark_snap, t_m);

    /* Determine if snap thread will visit this slot in current epoch.
     * snap_engine_tick (which sets is_snapshotting=true and swaps bitmaps)
     * runs on THIS thread, so this check is consistent with the bitmaps. */
    struct thread_heap *heap = s->mem;
    bool snap_will_visit = false;

    if (atomic_load_explicit(&s->snap.is_snapshotting, memory_order_acquire)) {
      uint32_t lpage = ptr_lpage_idx(o);
      if (lpage < heap->nr_pages_committed && heap->snap_dirty_bits &&
          dirty_bit_test(heap->snap_dirty_bits, lpage)) {
        uint32_t slot = (obj_size > 0) ? ptr_to_page_slot(o, heap->data_base,
                                                          heap->pages, obj_size)
                                       : 0;
        if (slot != INVALID && heap->snap_slot_bits &&
            modified_bit_test(heap->snap_slot_bits, lpage, slot)) {
          snap_will_visit = true;
        }
      }
    }

    if (snap_will_visit) {
      /* Slot is in frozen bitmap — snap thread will visit.
       * Race with snap thread via CAS on snap_flag. */
      uint8_t expected = SNAP_FLAG_LIVE_FRESH;
      if (atomic_compare_exchange_strong_explicit(
              &o->snap_flag, &expected, SNAP_FLAG_DEAD_PENDING,
              memory_order_acq_rel, memory_order_acquire)) {
        /* Won: snap hasn't visited yet. flag=DEAD_PENDING.
         * Snap will see flag=2, write LIVE (PIT), set flag=1.
         * Active dirty bit already set → next epoch writes tombstone. */
      } else {
        /* Lost: expected==LIVE_VISITED(3), snap already serialized LIVE.
         * Set DEAD_SAFE so next epoch writes tombstone + unrefs. */
        atomic_store_explicit(&o->snap_flag, SNAP_FLAG_DEAD_SAFE,
                              memory_order_release);
      }
      /* Main decrements its ref only. Snap will decrement its ref later. */
    } else {
      /* Snap will NOT visit this slot in current epoch.
       * Set DEAD_SAFE — next epoch's snap will write tombstone + unref.
       * Active dirty bit already set by snap_obj_mark above. */
      atomic_store_explicit(&o->snap_flag, SNAP_FLAG_DEAD_SAFE,
                            memory_order_release);
      /* Main decrements its ref only. Snap handles its ref next epoch. */
    }
  }

  lru_node_remove(s, o);
  if (o->heap_idx != HEAP_IDX_NONE)
    ttl_index_node_remove(&s->ttl_idx, o);

  shard_obj_unref(s, o);
}

static inline void obj_destroy(struct shard *s, struct kv_obj *o) {
  shard_obj_destroy(s, o);
}

/* ── Hot-path ops ─────────────────────────────────────────────────────── */

struct kv_obj *shard_bucket_put(struct shard *s, struct kv_obj *obj,
                                enum val_type type, uint64_t expire_ms,
                                uint32_t put_flags) {
  struct kv_obj *old =
      ht_bucket_put(s->table, obj, type, expire_ms, put_flags);
  if (old == (struct kv_obj *)(uintptr_t)-1 && shard_mem_evict(s, 0)) {
    old = ht_bucket_put(s->table, obj, type, expire_ms, put_flags);
  }
  return old;
}

bool shard_key_set(struct shard *s, const void *key, size_t klen,
                   const char *val, size_t vlen, uint64_t expire_ms,
                   uint32_t put_flags) {
  uint64_t t_set = PROF_NOW();
  /* Pre-check NX/XX condition before allocating — avoids alloc+rollback. */
  if (put_flags & HT_PUT_NX) {
    uint64_t t_h = PROF_NOW();
    if (ht_bucket_get(s->table, key, klen, VAL_TYPE_STRING, 0) != NULL) {
      PROF_RECORD(s->prof.ht_lookup, t_h);
      return false;
    }
    PROF_RECORD(s->prof.ht_lookup, t_h);
  } else if (put_flags & HT_PUT_XX) {
    uint64_t t_h = PROF_NOW();
    if (ht_bucket_get(s->table, key, klen, VAL_TYPE_STRING, 0) == NULL) {
      PROF_RECORD(s->prof.ht_lookup, t_h);
      return false;
    }
    PROF_RECORD(s->prof.ht_lookup, t_h);
  }

  size_t total = sizeof(struct kv_obj) + klen + vlen + 1;
  uint64_t t0 = PROF_NOW();
  struct kv_obj *o = mem_malloc(total);
  PROF_RECORD(s->prof.obj_alloc, t0);
  if (!o)
    return false;

  o->heap_idx = HEAP_IDX_NONE;
  o->pool_idx = total <= SMALL_ALLOC_MAX ? pool_idx_get(total) : NR_SMALL_POOLS;
  o->lru_prev = NULL;
  o->lru_next = NULL;
  o->key_len = (uint16_t)klen;
  o->val_len = (uint32_t)vlen;
  o->expire_ms = expire_ms;
  atomic_init(&o->refcount, g_snapshot_active ? 2 : 1);
  o->marked_deletion = 0;
  atomic_init(&o->snap_flag, SNAP_FLAG_LIVE_FRESH);

  memcpy(o->data, key, klen);
  memcpy(o->data + klen, val, vlen);
  o->data[klen + vlen] = '\0';

  uint32_t obj_size =
      total <= SMALL_ALLOC_MAX ? POOL_CLASSES[pool_idx_get(total)] : 0;

  if (g_snapshot_active) {
    uint64_t t_m = PROF_NOW();
    snap_obj_mark(&s->snap, s->mem, o, obj_size);
    PROF_RECORD(s->prof.mark_snap, t_m);
  }
  uint64_t t_p = PROF_NOW();
  struct kv_obj *old =
      shard_bucket_put(s, o, VAL_TYPE_STRING, expire_ms, HT_PUT_NONE);
  PROF_RECORD(s->prof.ht_insert, t_p);

  bool ok = old != (struct kv_obj *)(uintptr_t)-1;
  if (ok)
    lru_node_prepend(s, o);

  if (old && old != (struct kv_obj *)(uintptr_t)-1)
    obj_destroy(s, old);

  if (!ok) {
    obj_destroy(s, o);
  }

  if (ok && expire_ms > 0)
    ttl_index_node_push(&s->ttl_idx, o, expire_ms);

  PROF_RECORD(s->prof.set, t_set);
  return ok;
}

/* ── (Old atomic MSET helpers removed — replaced by async 2PC in mset_exec.c)
 */

struct kv_obj *shard_key_get(struct shard *s, const void *key, size_t klen,
                             uint64_t net_time_ms_get) {
  uint64_t t_get = PROF_NOW();
  struct kv_obj *expired = NULL;
  uint64_t t_h = PROF_NOW();
  struct kv_obj *v = ht_bucket_get_lazy(s->table, key, klen, VAL_TYPE_STRING,
                                        net_time_ms_get, &expired);
  PROF_RECORD(s->prof.ht_lookup, t_h);
  if (v)
    lru_node_touch(s, v);
  if (expired)
    obj_destroy(s, expired);
  PROF_RECORD(s->prof.get, t_get);
  return v;
}

bool shard_key_delete(struct shard *s, const void *key, size_t klen) {
  struct kv_obj *o = ht_bucket_take(s->table, key, klen, VAL_TYPE_STRING);
  if (!o)
    return false;
  obj_destroy(s, o);
  return true;
}

void shard_table_flush(struct shard *s) {
  ht_table_flush(s->table);
  ttl_index_destroy(&s->ttl_idx);
  ttl_index_init(&s->ttl_idx);
  for (int p = 0; p < LRU_POOLS; p++) {
    s->lru_head[p] = NULL;
    s->lru_tail[p] = NULL;
  }
}

bool shard_key_expire(struct shard *s, const void *key, size_t klen,
                      uint64_t expire_ms) {
  if (!key)
    return false;
  uint64_t current_expire = ht_expire_get(s->table, key, klen, VAL_TYPE_STRING);
  if (current_expire == (uint64_t)-1)
    return false;

  /* PERSIST on key without TTL returns 0 */
  if (expire_ms == 0 && current_expire == 0)
    return false;

  struct kv_obj *o = ht_bucket_get(s->table, key, klen, VAL_TYPE_STRING, 0);
  if (!o)
    return false;

  shard_bucket_put(s, o, VAL_TYPE_STRING, expire_ms, HT_PUT_NONE);
  if (expire_ms > 0) {
    if (o->heap_idx == HEAP_IDX_NONE)
      ttl_index_node_push(&s->ttl_idx, o, expire_ms);
    else
      ttl_index_node_update(&s->ttl_idx, o, expire_ms);
  } else {
    if (o->heap_idx != HEAP_IDX_NONE)
      ttl_index_node_remove(&s->ttl_idx, o);
  }
  return true;
}

long long shard_key_ttl(struct shard *s, const void *key, size_t klen,
                        uint64_t cur_ms, bool ms) {
  uint64_t exp = ht_expire_get(s->table, key, klen, VAL_TYPE_STRING);
  if (exp == (uint64_t)-1)
    return -2;
  if (exp == 0)
    return -1;
  if (cur_ms >= exp)
    return 0;
  uint64_t diff = exp - cur_ms;
  return ms ? (long long)diff : (long long)((diff + 999) / 1000);
}

uint64_t shard_key_expire_get(struct shard *s, const void *key, size_t klen) {
  return ht_expire_get(s->table, key, klen, VAL_TYPE_STRING);
}

struct kv_obj *shard_obj_put(struct shard *s, struct kv_obj *obj,
                             enum val_type type, uint64_t expire_ms) {
  obj->expire_ms = (expire_ms == (uint64_t)-1) ? 0 : expire_ms;
  lru_node_touch(s, obj);
  return shard_bucket_put(s, obj, type, expire_ms, HT_PUT_NONE);
}

/* ── TTL active expiry ────────────────────────────────────────────────── */

int shard_ttl_drain(struct shard *s, uint64_t net_time_ms_get, int max_work) {
  struct ttl_index *idx = &s->ttl_idx;
  int expired = 0;

  for (int i = 0; i < max_work; i++) {
    if (ttl_index_node_peek(idx) > net_time_ms_get)
      break;
    uint64_t expire_ms;
    struct kv_obj *o = ttl_index_node_pop(idx, &expire_ms);
    if (!o)
      break;

    bool locked = false;
    struct kv_obj *taken = ht_bucket_take_if_unlocked(
        s->table, obj_key_get(o), o->key_len, VAL_TYPE_STRING, o, &locked);
    if (locked) {
      ttl_index_node_push(idx, o, expire_ms);
      continue;
    }
    if (taken) {
      obj_destroy(s, taken);
      expired++;
    }
  }
  return expired;
}

static inline uint64_t shard_now_ms(void) {
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return (uint64_t)ts.tv_sec * 1000ULL + (uint64_t)ts.tv_nsec / 1000000ULL;
}

enum proxy_exec_result proxy_exec(struct shard *s, struct spsc_message *msg,
                                  uint64_t cur) {
  uint16_t sub_op = MSG_GET_CMD(msg->op);
  struct resp_cmd *cmd = &msg->cmd;

  /* Default */
  msg->reply_type = REPLY_TYPE_ERR;
  msg->reply_int = 0;
  msg->reply_buf = NULL;
  msg->reply_len = 0;
  msg->kv_obj_ptr = NULL;

  const char *key = NULL;
  size_t klen = 0;
  if (cmd->argc > 1) {
    key = cmd->argv[1];
    klen = cmd->arglen[1];
  } else if (sub_op == MSG_CMD_MGET_PART || sub_op == MSG_CMD_MSET_PART ||
             sub_op == MSG_CMD_DEL_PART) {
    key = (const char *)msg->key_ptr;
    klen = msg->key_len;
  }

  if (key) {
    if (mset_check_lock_defer(s, msg, key, klen))
      return PROXY_EXEC_DEFERRED;
  }

  if (sub_op == MSG_CMD_MGET_PART || sub_op == MSG_CMD_MSET_PART ||
      sub_op == MSG_CMD_DBSIZE || sub_op == MSG_CMD_DEL_PART ||
      sub_op == MSG_CMD_MSET_PREPARE)
    goto dispatch;

  if (cmd->argc < 1) {
    msg->reply_buf = "invalid proxy cmd";
    msg->reply_len = 17;
    return PROXY_EXEC_DONE;
  }

dispatch:
  switch (sub_op) {
  case MSG_CMD_GET: {
    struct kv_obj *v = shard_key_get(s, key, klen, cur);
    if (v) {
      shard_obj_ref(v);
      msg->kv_obj_ptr = v;
      msg->reply_type = REPLY_TYPE_KV_OBJ; /* Will use kv_obj_ptr directly */
    } else {
      msg->reply_type = REPLY_TYPE_NIL;
    }
    break;
  }
  case MSG_CMD_SET:
  case MSG_CMD_SET_NX:
  case MSG_CMD_SET_XX: {
    const char *val = (const char *)cmd->argv[2];
    size_t vlen = cmd->arglen[2];

    bool nx = (sub_op == MSG_CMD_SET_NX);
    bool xx = (sub_op == MSG_CMD_SET_XX);

    uint32_t put_flags = HT_PUT_NONE;
    if (nx)
      put_flags = HT_PUT_NX;
    else if (xx)
      put_flags = HT_PUT_XX;

    uint64_t exp = 0;
    bool pkeepttl = false;
    for (uint32_t i = 3; i < cmd->argc; i++) {
      char opt[16] = {0};
      size_t ol = cmd->arglen[i] < 15 ? cmd->arglen[i] : 15;
      for (size_t j = 0; j < ol; j++)
        opt[j] = (char)toupper((unsigned char)cmd->argv[i][j]);
      if ((strcmp(opt, "EX") == 0 || strcmp(opt, "PX") == 0) &&
          i + 1 < cmd->argc) {
        char vtmp[32] = {0};
        size_t vl = cmd->arglen[i + 1] < 31 ? cmd->arglen[i + 1] : 31;
        memcpy(vtmp, cmd->argv[++i], vl);
        long long n = strtoll(vtmp, NULL, 10);
        if (n > 0)
          exp = cur +
                (strcmp(opt, "EX") == 0 ? (uint64_t)n * 1000ULL : (uint64_t)n);
      } else if (strcmp(opt, "KEEPTTL") == 0) {
        pkeepttl = true;
      }
    }
    if (pkeepttl) {
      uint64_t old_exp = shard_key_expire_get(s, key, klen);
      if (old_exp != (uint64_t)-1 && old_exp > 0)
        exp = old_exp;
    }
    bool ok = shard_key_set(s, key, klen, val, vlen, exp, put_flags);
    if (ok)
      msg->reply_type = REPLY_TYPE_OK;
    else if (nx || xx)
      msg->reply_type = REPLY_TYPE_NIL;
    else {
      msg->reply_type = REPLY_TYPE_ERR;
      msg->reply_buf = "OOM";
      msg->reply_len = 3;
    }
    break;
  }
  case MSG_CMD_DEL: {
    long long count = 0;
    for (uint32_t i = 1; i < cmd->argc; i++)
      count += shard_key_delete(s, cmd->argv[i], cmd->arglen[i]) ? 1 : 0;
    msg->reply_type = REPLY_TYPE_INT;
    msg->reply_int = count;
    break;
  }
  case MSG_CMD_EXISTS: {
    struct kv_obj *v = shard_key_get(s, key, klen, cur);
    msg->reply_type = REPLY_TYPE_INT;
    msg->reply_int = v ? 1 : 0;
    break;
  }
  case MSG_CMD_TTL:
  case MSG_CMD_PTTL: {
    msg->reply_type = REPLY_TYPE_INT;
    msg->reply_int = shard_key_ttl(s, key, klen, cur, sub_op == MSG_CMD_PTTL);
    break;
  }
  case MSG_CMD_EXPIRE:
  case MSG_CMD_PEXPIRE: {
    if (cmd->argc < 3) {
      msg->reply_buf = "wrong number of arguments";
      msg->reply_len = 25;
      break;
    }
    char tmp[32] = {0};
    size_t tl = cmd->arglen[2] < 31 ? cmd->arglen[2] : 31;
    memcpy(tmp, cmd->argv[2], tl);
    long long n = strtoll(tmp, NULL, 10);
    uint64_t exp = n > 0
                       ? (sub_op == MSG_CMD_EXPIRE ? cur + (uint64_t)n * 1000ULL
                                                   : cur + (uint64_t)n)
                       : 1;
    msg->reply_type = REPLY_TYPE_INT;
    msg->reply_int = shard_key_expire(s, key, klen, exp) ? 1 : 0;
    break;
  }
  case MSG_CMD_PERSIST: {
    msg->reply_type = REPLY_TYPE_INT;
    msg->reply_int = shard_key_expire(s, key, klen, 0) ? 1 : 0;
    break;
  }
  case MSG_CMD_EXPIREAT:
  case MSG_CMD_PEXPIREAT: {
    if (cmd->argc < 3) {
      msg->reply_buf = "wrong number of arguments";
      msg->reply_len = 25;
      break;
    }
    char tmp[32] = {0};
    size_t tl = cmd->arglen[2] < 31 ? cmd->arglen[2] : 31;
    memcpy(tmp, cmd->argv[2], tl);
    long long n = strtoll(tmp, NULL, 10);
    uint64_t exp =
        (sub_op == MSG_CMD_EXPIREAT) ? (uint64_t)n * 1000ULL : (uint64_t)n;
    msg->reply_type = REPLY_TYPE_INT;
    msg->reply_int = shard_key_expire(s, key, klen, exp) ? 1 : 0;
    break;
  }
  case MSG_CMD_INCR: {
    char tmp_delta[32] = {0};
    size_t dl = cmd->arglen[2] < 31 ? cmd->arglen[2] : 31;
    memcpy(tmp_delta, cmd->argv[2], dl);
    long long delta = strtoll(tmp_delta, NULL, 10);
    struct kv_obj *v = shard_key_get(s, key, klen, cur);
    long long cur_val = 0;
    if (v) {
      char tmp[32] = {0};
      uint32_t vl = obj_val_len_get(v);
      if (vl >= sizeof(tmp)) {
        msg->reply_buf = "value is not an integer";
        msg->reply_len = 23;
        break;
      }
      memcpy(tmp, obj_val_get(v), vl);
      cur_val = strtoll(tmp, NULL, 10);
    }
    cur_val += delta;
    char buf[32];
    int bl = snprintf(buf, sizeof(buf), "%lld", cur_val);
    shard_key_set(s, key, klen, buf, (size_t)bl, 0, HT_PUT_NONE);
    msg->reply_type = REPLY_TYPE_INT;
    msg->reply_int = cur_val;
    break;
  }
  case MSG_CMD_MGET_PART: {
    const char *pkey = (const char *)msg->key_ptr;
    size_t pklen = msg->key_len;
    struct kv_obj *v = shard_key_get(s, pkey, pklen, cur);
    if (v) {
      shard_obj_ref(v);
      msg->kv_obj_ptr = v;
      msg->reply_type = REPLY_TYPE_KV_OBJ;
    } else {
      msg->reply_type = REPLY_TYPE_NIL;
    }
    break;
  }
  case MSG_CMD_MSET_PART: {
    const char *pkey = (const char *)msg->key_ptr;
    size_t pklen = msg->key_len;
    const char *pval = (const char *)msg->val_ptr;
    size_t pvlen = msg->val_len;
    shard_key_set(s, pkey, pklen, pval, pvlen, 0, HT_PUT_NONE);
    msg->reply_type = REPLY_TYPE_OK;
    break;
  }
  case MSG_CMD_DEL_PART: {
    const char *pkey = (const char *)msg->key_ptr;
    size_t pklen = msg->key_len;
    bool deleted = shard_key_delete(s, pkey, pklen);
    char buf[8];
    int bl = snprintf(buf, sizeof(buf), ":%d\r\n", deleted ? 1 : 0);
    char *rb = (char *)slab_obj_alloc(s->pool, (size_t)bl);
    if (rb)
      memcpy(rb, buf, (size_t)bl);
    msg->reply_buf = rb;
    msg->reply_len = rb ? (uint32_t)bl : 0;
    msg->reply_type = REPLY_TYPE_BUF;
    break;
  }
  case MSG_CMD_TYPE: {
    struct kv_obj *v = shard_key_get(s, key, klen, cur);
    const char *resp = v ? "+string\r\n" : "+none\r\n";
    size_t rlen = v ? 9 : 7;
    char *rb = (char *)slab_obj_alloc(s->pool, rlen);
    if (rb)
      memcpy(rb, resp, rlen);
    msg->reply_buf = rb;
    msg->reply_len = rb ? (uint32_t)rlen : 0;
    msg->reply_type = REPLY_TYPE_BUF;
    break;
  }
  case MSG_CMD_DBSIZE: {
    char buf[32];
    int bl = snprintf(buf, sizeof(buf), ":%lld\r\n", (long long)s->table->used);
    char *hdr = (char *)slab_obj_alloc(s->pool, (size_t)bl + 1);
    memcpy(hdr, buf, (size_t)bl);
    msg->reply_buf = hdr;
    msg->reply_len = (uint32_t)bl;
    msg->reply_type = REPLY_TYPE_BUF;
    break;
  }
  case MSG_CMD_STRLEN: {
    struct kv_obj *v = shard_key_get(s, key, klen, cur);
    msg->reply_type = REPLY_TYPE_INT;
    msg->reply_int = v ? (long long)obj_val_len_get(v) : 0;
    break;
  }
  case MSG_CMD_MSET_PREPARE: {
    /* DEPRECATED — old synchronous MSET. Should not arrive anymore.
     * New protocol uses MSG_CMD_MSET_KEY handled in shard_msg_drain. */
    msg->reply_type = REPLY_TYPE_ERR;
    msg->reply_buf = "deprecated MSET_PREPARE";
    msg->reply_len = 23;
    break;
  }
  default:
    msg->reply_buf = "unknown proxy cmd";
    msg->reply_len = 17;
    break;
  }

  return PROXY_EXEC_DONE;
}

/* ── SPSC inbox drain ─────────────────────────────────────────────────── */

/*
 * Helper: send a reply message back to the sender shard and wake it.
 */
static void shard_send_reply(struct shard_engine *engine, struct shard *shard,
                             const struct spsc_message *msg,
                             struct spsc_message *reply) {
  struct spsc_queue *rq =
      &engine->queues[shard->id * engine->num_shards + msg->shard_owner];
  while (!spsc_queue_push(rq, reply))
    sched_yield();
  /* Wake the sender */
  if (engine->wake_fds && engine->wake_fds[msg->shard_owner] >= 0) {
    uint64_t one = 1;
    if (write(engine->wake_fds[msg->shard_owner], &one, sizeof(one)) <
        0) { /* ignore */
    }
  }
}

/* ── (Old shard_mset_participant_wait removed — replaced by async 2PC) ── */

void shard_msg_drain(struct shard_engine *engine, struct shard *shard,
                     shard_proxy_reply_fn proxy_cb, void *proxy_ctx) {
  uint64_t cur = 0;
  bool drain_prof = mixed_profile_enabled();
  uint64_t drain_start = drain_prof ? PROF_NOW() : 0;
  uint32_t total_msgs = 0;
  uint32_t reply_msgs = 0;
  uint32_t release_msgs = 0;
  uint32_t req_msgs = 0;
  uint32_t set_msgs = 0;
  uint32_t mset_key_msgs = 0;
  uint32_t mset_batch_msgs = 0;
  uint32_t mset_fin_msgs = 0;
  uint32_t other_req_msgs = 0;
  uint32_t max_sender_run = 0;

  for (uint32_t sender = 0; sender < shard->num_shards; sender++) {
    if (sender == shard->id)
      continue;
    struct spsc_queue *q = shard->inboxes[sender];
    if (!q)
      continue;

    struct spsc_message msg;
    uint32_t wake_mask = 0;
    uint32_t sender_run = 0;
    while (spsc_queue_pop(q, &msg)) {
      if (drain_prof) {
        total_msgs++;
        sender_run++;
      }
      uint8_t sys_op = MSG_GET_SYS(msg.op);
      if (sys_op == MSG_SYS_REPLY) {
        if (drain_prof)
          reply_msgs++;
        if (proxy_cb)
          proxy_cb(proxy_ctx, &msg);
        continue;
      }

      if (sys_op == MSG_SYS_RELEASE) {
        if (drain_prof)
          release_msgs++;
        if (msg.kv_obj_ptr)
          shard_obj_unref(shard, (struct kv_obj *)msg.kv_obj_ptr);
        if (msg.reply_buf)
          slab_obj_free(shard->pool, msg.reply_buf);
        continue;
      }

      if (sys_op == MSG_SYS_REQ) {
        if (drain_prof)
          req_msgs++;
        if (!cur)
          cur = shard_now_ms();
        uint16_t cmd_op = MSG_GET_CMD(msg.op);
        bool mix_prof = mixed_profile_enabled();
        uint64_t q_cycles = 0;
        uint64_t t_msg = 0;
        if (mix_prof) {
          uint64_t now_c = PROF_NOW();
#if FREAKV_MIXED_PROFILE
          q_cycles = msg.sent_cycles ? now_c - msg.sent_cycles : 0;
#else
          q_cycles = 0;
#endif
          t_msg = now_c;
        }

        /* ── Async MSET handlers ──────────────────────────────────── */
        if (cmd_op == MSG_CMD_MSET_KEY) {
          if (drain_prof)
            mset_key_msgs++;
          mset_on_prepare(engine, shard, &msg, cur);
          if (mix_prof) {
            uint64_t dur = PROF_NOW() - t_msg;
            uint64_t slow = mixed_profile_slow_cycles();
            if (dur >= slow || q_cycles >= slow)
              fprintf(stderr,
                      "[MSET_MIXED_MSG] shard=%u cmd=%s sender=%u qcy=%lu "
                      "durcy=%lu deferred=0 ops=1\n",
                      shard->id, mixed_cmd_name(cmd_op), sender,
                      (unsigned long)q_cycles, (unsigned long)dur);
          }
          shard->cross_shard_received++;
          continue;
        }

        if (cmd_op == MSG_CMD_MSET_KEY_BATCH) {
          if (drain_prof)
            mset_batch_msgs++;
          uint32_t batch_count =
              msg.key_ptr ? ((struct mset_batch *)msg.key_ptr)->count : 0;
          mset_on_prepare_batch(engine, shard, &msg, cur);
          if (mix_prof) {
            uint64_t dur = PROF_NOW() - t_msg;
            uint64_t slow = mixed_profile_slow_cycles();
            if (dur >= slow || q_cycles >= slow)
              fprintf(stderr,
                      "[MSET_MIXED_MSG] shard=%u cmd=%s sender=%u qcy=%lu "
                      "durcy=%lu deferred=0 ops=%u\n",
                      shard->id, mixed_cmd_name(cmd_op), sender,
                      (unsigned long)q_cycles, (unsigned long)dur, batch_count);
          }
          shard->cross_shard_received++;
          continue;
        }

        if (cmd_op == MSG_CMD_MSET_FIN) {
          if (drain_prof)
            mset_fin_msgs++;
          mset_on_fin(engine, shard, &msg, cur);
          if (mix_prof) {
            uint64_t dur = PROF_NOW() - t_msg;
            uint64_t slow = mixed_profile_slow_cycles();
            if (dur >= slow || q_cycles >= slow)
              fprintf(stderr,
                      "[MSET_MIXED_MSG] shard=%u cmd=%s sender=%u qcy=%lu "
                      "durcy=%lu deferred=0 ops=0\n",
                      shard->id, mixed_cmd_name(cmd_op), sender,
                      (unsigned long)q_cycles, (unsigned long)dur);
          }
          shard->cross_shard_received++;
          continue;
        }

        /* ── Normal request path ──────────────────────────────────── */
        if (drain_prof) {
          if (cmd_op == MSG_CMD_SET || cmd_op == MSG_CMD_SET_NX ||
              cmd_op == MSG_CMD_SET_XX)
            set_msgs++;
          else
            other_req_msgs++;
        }
        enum proxy_exec_result exec_rc = proxy_exec(shard, &msg, cur);
        if (mix_prof) {
          uint64_t dur = PROF_NOW() - t_msg;
          uint64_t slow = mixed_profile_slow_cycles();
          if (dur >= slow || q_cycles >= slow || exec_rc == PROXY_EXEC_DEFERRED)
            fprintf(stderr,
                    "[MSET_MIXED_MSG] shard=%u cmd=%s sender=%u qcy=%lu "
                    "durcy=%lu deferred=%d ops=1\n",
                    shard->id, mixed_cmd_name(cmd_op), sender,
                    (unsigned long)q_cycles, (unsigned long)dur,
                    exec_rc == PROXY_EXEC_DEFERRED);
        }

        shard->cross_shard_received++;
        if (exec_rc == PROXY_EXEC_DEFERRED) {
          if (msg.req_nb)
            net_buf_unref(shard->pool, msg.req_nb);
          continue;
        }

        shard->ops_completed++;

        struct spsc_queue *rq =
            &engine->queues[shard->id * engine->num_shards + msg.shard_owner];
        struct spsc_message reply = {
            .op = MSG_PACK_OP(MSG_SYS_REPLY, cmd_op),
            .shard_owner = (uint8_t)shard->id,
            .conn_ptr = msg.conn_ptr,
            .conn_generation = msg.conn_generation,
            .pipeline_idx = msg.pipeline_idx,
            .sub_idx = msg.sub_idx,
            .kv_obj_ptr = msg.kv_obj_ptr,
            .old_obj_ptr = msg.old_obj_ptr,
            .reply_buf = msg.reply_buf,
            .reply_len = msg.reply_len,
            .reply_type = msg.reply_type,
            .reply_int = msg.reply_int,
            .req_nb = msg.req_nb,
        };
        while (true) {
          if (spsc_queue_push(rq, &reply)) {
            wake_mask |= (1U << msg.shard_owner);
            break;
          }
          sched_yield();
        }
      }
    }
    if (drain_prof && sender_run > max_sender_run)
      max_sender_run = sender_run;

    if (wake_mask && engine->wake_fds) {
      uint32_t p = 0;
      while (wake_mask) {
        if ((wake_mask & 1) && engine->wake_fds[p] >= 0) {
          uint64_t one = 1;
          (void)write(engine->wake_fds[p], &one, sizeof(one));
        }
        wake_mask >>= 1;
        p++;
      }
    }
  }
  if (drain_prof && total_msgs > 0) {
    uint64_t total_cycles = PROF_NOW() - drain_start;
    uint64_t slow = mixed_profile_slow_cycles();
    if (total_cycles >= slow || total_msgs >= 256 || max_sender_run >= 128) {
      fprintf(stderr,
              "[SHARD_DRAIN_PROFILE] shard=%u total_msgs=%u totalcy=%lu "
              "req=%u reply=%u release=%u set=%u mset_key=%u "
              "mset_batch=%u mset_fin=%u other_req=%u max_sender_run=%u\n",
              shard->id, total_msgs, (unsigned long)total_cycles, req_msgs,
              reply_msgs, release_msgs, set_msgs, mset_key_msgs,
              mset_batch_msgs, mset_fin_msgs, other_req_msgs, max_sender_run);
    }
  }
}

/* ── struct shard init / cleanup
 * ─────────────────────────────────────────────── */

static size_t shard_evict_callback(void *ctx, size_t bytes_needed) {
  struct shard *s = (struct shard *)ctx;
  if (shard_mem_evict(s, bytes_needed)) {
    return bytes_needed;
  }
  return 0;
}

static bool shard_local_init(struct shard *shard, uint32_t id, int cpu_core,
                             uint32_t num_shards,
                             struct slab_allocator *init_pool) {
  memset(shard, 0, sizeof(*shard));
  shard->id = id;
  shard->cpu_core = cpu_core;
  shard->num_shards = num_shards;

  /* Initialize thread-local heap and set TLS thread ID */
  if (!mem_thread_init(id))
    return false;
  mem_set_thread_id(id);
  shard->mem = &g_thread_heaps[id];

  mem_evict_register(id, shard_evict_callback, shard);

  shard->table = ht_table_create(shard->mem, id, INITIAL_BUCKETS);
  if (!shard->table) {
    mem_thread_destroy(id);
    return false;
  }

  shard->pool = slab_alloc_init();
  if (!shard->pool) {
    ht_table_destroy(shard->table);
    mem_thread_destroy(id);
    return false;
  }
  const size_t control_hot_sizes[] = {
      sizeof(struct net_conn),
      sizeof(struct net_buf) + 4096,
      PIPELINE_INIT_CAP * sizeof(struct net_pipeline_slot),
      sizeof(char *) * 8,
      sizeof(size_t) * 8,
      256,
  };
  slab_alloc_prewarm(shard->pool, control_hot_sizes,
                     sizeof(control_hot_sizes) / sizeof(control_hot_sizes[0]));

  if (!lock_manager_init(&shard->lm, num_shards, shard->pool)) {
    slab_alloc_destroy(shard->pool);
    ht_table_destroy(shard->table);
    mem_thread_destroy(id);
    return false;
  }

  shard->inboxes = (struct spsc_queue **)slab_obj_calloc(
      init_pool, num_shards, sizeof(struct spsc_queue *));
  if (!shard->inboxes) {
    slab_alloc_destroy(shard->pool);
    ht_table_destroy(shard->table);
    mem_thread_destroy(id);
    return false;
  }

  ttl_index_init(&shard->ttl_idx);
  for (int p = 0; p < LRU_POOLS; p++) {
    shard->lru_head[p] = NULL;
    shard->lru_tail[p] = NULL;
  }

  snap_engine_init(&shard->snap, id, snap_session_id_get(),
                   snap_config_dir_get(), snap_config_interval_get());

  if (!snap_thread_spawn(shard)) {
    slab_obj_free(init_pool, shard->inboxes);
    slab_alloc_destroy(shard->pool);
    ht_table_destroy(shard->table);
    mem_thread_destroy(id);
    return false;
  }

  return true;
}

static void shard_local_cleanup(struct shard *shard,
                                struct slab_allocator *init_pool) {
  snap_engine_destroy(&shard->snap);
  lock_manager_destroy(&shard->lm);
  ttl_index_destroy(&shard->ttl_idx);
  slab_obj_free(init_pool, shard->inboxes);
  if (shard->pool)
    slab_alloc_destroy(shard->pool);
  if (shard->table)
    ht_table_destroy(shard->table);
  mem_thread_destroy(shard->id);
}

/* ── Engine lifecycle ─────────────────────────────────────────────────── */

struct shard_engine *shard_engine_create(struct slab_allocator *init_pool,
                                         uint32_t num_shards) {
  if (num_shards == 0)
    num_shards = num_cpu_count();
  struct shard_engine *e =
      (struct shard_engine *)slab_obj_calloc(init_pool, 1, sizeof(*e));
  if (!e)
    return NULL;
  e->num_shards = num_shards;
  e->init_pool = init_pool;
  /* mset_inflight and mset_max_concurrent are now per-shard (set in loop below)
   */
  e->shards = (struct shard *)slab_obj_calloc(init_pool, num_shards,
                                              sizeof(struct shard));
  if (!e->shards) {
    slab_obj_free(init_pool, e);
    return NULL;
  }

  for (uint32_t i = 0; i < num_shards; i++) {
    if (!shard_local_init(&e->shards[i], i, (int)i, num_shards, init_pool)) {
      for (uint32_t j = 0; j < i; j++)
        shard_local_cleanup(&e->shards[j], init_pool);
      slab_obj_free(init_pool, e->shards);
      slab_obj_free(init_pool, e);
      return NULL;
    }
    e->shards[i].engine = e;
    e->shards[i].mset_inflight = 0;
    e->shards[i].mset_max_concurrent = 20;
  }
  e->queues = (struct spsc_queue *)slab_obj_calloc(
      init_pool, (size_t)num_shards * num_shards, sizeof(struct spsc_queue));
  if (!e->queues) {
    for (uint32_t i = 0; i < num_shards; i++)
      shard_local_cleanup(&e->shards[i], init_pool);
    slab_obj_free(init_pool, e->shards);
    slab_obj_free(init_pool, e);
    return NULL;
  }
  for (uint32_t s = 0; s < num_shards; s++) {
    for (uint32_t r = 0; r < num_shards; r++) {
      if (s == r)
        continue;
      struct spsc_queue *q = &e->queues[s * num_shards + r];
      if (!spsc_queue_init(q, e->shards[s].pool))
        return NULL;
      e->shards[r].inboxes[s] = q;
    }
  }
  e->wake_fds = (int *)slab_obj_calloc(init_pool, num_shards, sizeof(int));
  if (e->wake_fds)
    for (uint32_t i = 0; i < num_shards; i++)
      e->wake_fds[i] = -1;
  return e;
}

void shard_engine_stop(struct shard_engine *e) {
  if (!e)
    return;
  for (uint32_t i = 0; i < e->num_shards; i++)
    e->shards[i].running = false;
  e->running = false;
}

void shard_engine_destroy(struct shard_engine *e) {
  if (!e)
    return;
  if (e->running)
    shard_engine_stop(e);
  struct slab_allocator *init_pool = e->init_pool;
  if (e->queues) {
    for (uint32_t s = 0; s < e->num_shards; s++)
      for (uint32_t r = 0; r < e->num_shards; r++)
        if (s != r)
          spsc_queue_destroy(&e->queues[s * e->num_shards + r]);
    slab_obj_free(init_pool, e->queues);
  }
  for (uint32_t i = 0; i < e->num_shards; i++)
    shard_local_cleanup(&e->shards[i], init_pool);
  slab_obj_free(init_pool, e->shards);
  slab_obj_free(init_pool, e);
}

void shard_stats_get(const struct shard *s, struct shard_stats *out) {
  out->shard_id = s->id;
  out->cpu_core = s->cpu_core;
  out->table_size = s->table ? s->table->used : 0;
  out->ops_completed = s->ops_completed;
  out->cross_shard_sent = s->cross_shard_sent;
  out->cross_shard_received = s->cross_shard_received;
}

void shard_engine_stats_print(const struct shard_engine *e) {
  if (!e)
    return;
  printf("\n===========================================================\n");
  printf("  shard_engine stats  (%u shards)\n", e->num_shards);
  printf("===========================================================\n");
  printf("%-6s %-5s %-10s %-14s %-11s %-11s\n", "shard", "Core", "Entries",
         "Ops", "XShard->", "XShard<-");
  printf("-----------------------------------------------------------\n");
  size_t tot_e = 0;
  uint64_t tot_o = 0, tot_s = 0, tot_r = 0;
  for (uint32_t i = 0; i < e->num_shards; i++) {
    struct shard_stats st;
    shard_stats_get(&e->shards[i], &st);
    printf("%-6u %-5d %-10zu %-14lu %-11lu %-11lu\n", st.shard_id, st.cpu_core,
           st.table_size, st.ops_completed, st.cross_shard_sent,
           st.cross_shard_received);
    tot_e += st.table_size;
    tot_o += st.ops_completed;
    tot_s += st.cross_shard_sent;
    tot_r += st.cross_shard_received;
  }
  printf("-----------------------------------------------------------\n");
  printf("%-6s %-5s %-10zu %-14lu %-11lu %-11lu\n\n", "TOTAL", "-", tot_e,
         tot_o, tot_s, tot_r);

  printf("  MSET Connection-Level Pending\n");
  printf("-----------------------------------------------------------\n");
  printf("%-6s %-14s %-14s %-10s %-10s\n", "shard", "enqueued",
         "resumed", "cur_len", "max_len");
  uint64_t pending_enq = 0, pending_res = 0;
  uint32_t pending_cur = 0, pending_max = 0;
  for (uint32_t i = 0; i < e->num_shards; i++) {
    const struct shard *s = &e->shards[i];
    printf("%-6u %-14lu %-14lu %-10u %-10u\n", s->id,
           s->mset_pending_conn_enqueued, s->mset_pending_conn_resumed,
           s->mset_pending_conn_len, s->mset_pending_conn_max_len);
    pending_enq += s->mset_pending_conn_enqueued;
    pending_res += s->mset_pending_conn_resumed;
    pending_cur += s->mset_pending_conn_len;
    if (s->mset_pending_conn_max_len > pending_max)
      pending_max = s->mset_pending_conn_max_len;
  }
  printf("-----------------------------------------------------------\n");
  printf("%-6s %-14lu %-14lu %-10u %-10u\n\n", "TOTAL", pending_enq,
         pending_res, pending_cur, pending_max);

#if FREAKV_PROFILE
  /* ── Async MSET 2PC Profiling ─────────────────────────────────── */
  printf("  Async MSET 2PC Profiling (Avg CPU Cycles per Call)\n");
  printf("---------------------------------------------------------------------"
         "-----------------------------------------------------------\n");
  printf("%-6s %-28s %-28s %-28s %-28s %-28s %-28s %-28s\n", "shard",
         "mset_coordinator_dispatch", "mset_exec_prepare",
         "mset_coordinator_handle_ack", "mset_exec_fin",
         "mset_coordinator_handle_fin_ack", "mset_drain_waitqueue", "mset_e2e");
  for (uint32_t i = 0; i < e->num_shards; i++) {
    const struct shard *s = &e->shards[i];
    printf("%-6u %-28lu %-28lu %-28lu %-28lu %-28lu %-28lu %-28lu\n", s->id,
           s->prof.mset_dispatch.count ? s->prof.mset_dispatch.total_cycles /
                                             s->prof.mset_dispatch.count
                                       : 0,
           s->prof.mset_prepare.count
               ? s->prof.mset_prepare.total_cycles / s->prof.mset_prepare.count
               : 0,
           s->prof.mset_ack.count
               ? s->prof.mset_ack.total_cycles / s->prof.mset_ack.count
               : 0,
           s->prof.mset_fin.count
               ? s->prof.mset_fin.total_cycles / s->prof.mset_fin.count
               : 0,
           s->prof.mset_fin_ack.count
               ? s->prof.mset_fin_ack.total_cycles / s->prof.mset_fin_ack.count
               : 0,
           s->prof.mset_drain.count
               ? s->prof.mset_drain.total_cycles / s->prof.mset_drain.count
               : 0,
           s->prof.mset_total_e2e.count ? s->prof.mset_total_e2e.total_cycles /
                                              s->prof.mset_total_e2e.count
                                        : 0);
  }
  printf("---------------------------------------------------------------------"
         "-----------------------------------------------------------\n\n");

  /* ── mset_exec_prepare Breakdown ─────────────────────────────── */
  printf("  mset_exec_prepare Breakdown (Avg CPU Cycles per Call)\n");
  printf("---------------------------------------------------------------------"
         "-----------------------------------------------------------\n");
  printf("%-6s %-20s %-20s %-20s %-20s %-20s %-20s %-20s %-20s\n", "shard",
         "ht_bucket_get", "alloc_new_obj", "ht_bucket_put(tentative)",
         "lock_manager_lookup", "lock_manager_insert", "lock_wq_add_mset",
         "slab_obj_alloc(cmd)", "lock_wq_find_txn");
  for (uint32_t i = 0; i < e->num_shards; i++) {
    const struct shard *s = &e->shards[i];
    printf("%-6u %-20lu %-20lu %-20lu %-20lu %-20lu %-20lu %-20lu %-20lu\n",
           s->id,
           s->prof.ht_lookup.count
               ? s->prof.ht_lookup.total_cycles / s->prof.ht_lookup.count
               : 0,
           s->prof.obj_alloc.count
               ? s->prof.obj_alloc.total_cycles / s->prof.obj_alloc.count
               : 0,
           s->prof.ht_insert.count
               ? s->prof.ht_insert.total_cycles / s->prof.ht_insert.count
               : 0,
           s->prof.lm_lookup.count
               ? s->prof.lm_lookup.total_cycles / s->prof.lm_lookup.count
               : 0,
           s->prof.lm_insert.count
               ? s->prof.lm_insert.total_cycles / s->prof.lm_insert.count
               : 0,
           s->prof.lm_wq_add.count
               ? s->prof.lm_wq_add.total_cycles / s->prof.lm_wq_add.count
               : 0,
           s->prof.slab_alloc.count
               ? s->prof.slab_alloc.total_cycles / s->prof.slab_alloc.count
               : 0,
           s->prof.lm_wq_find.count
               ? s->prof.lm_wq_find.total_cycles / s->prof.lm_wq_find.count
               : 0);
  }
  printf("---------------------------------------------------------------------"
         "-----------------------------------------------------------\n\n");

  /* ── mset_coordinator_dispatch Breakdown ─────────────────────── */
  printf("  mset_coordinator_dispatch Breakdown (Avg CPU Cycles per Call)\n");
  printf("---------------------------------------------------------------------"
         "-----------------------------------------------------------\n");
  printf("%-6s %-20s %-20s %-20s %-20s %-20s %-20s\n", "shard",
         "slab_obj_alloc(stat)", "shard_for_key", "mset_exec_prepare(local)",
         "spsc_queue_push(wait)", "clock_gettime(txn_now_ns)",
         "backpressure_wait");
  for (uint32_t i = 0; i < e->num_shards; i++) {
    const struct shard *s = &e->shards[i];
    printf(
        "%-6u %-20lu %-20lu %-20lu %-20lu %-20lu %-20lu\n", s->id,
        s->prof.coord_stat_alloc.count ? s->prof.coord_stat_alloc.total_cycles /
                                             s->prof.coord_stat_alloc.count
                                       : 0,
        s->prof.coord_hash.count
            ? s->prof.coord_hash.total_cycles / s->prof.coord_hash.count
            : 0,
        s->prof.coord_local_exec.count ? s->prof.coord_local_exec.total_cycles /
                                             s->prof.coord_local_exec.count
                                       : 0,
        s->prof.coord_queue_wait.count ? s->prof.coord_queue_wait.total_cycles /
                                             s->prof.coord_queue_wait.count
                                       : 0,
        s->prof.time_now.count
            ? s->prof.time_now.total_cycles / s->prof.time_now.count
            : 0,
        s->prof.coord_backpressure_wait.count
            ? s->prof.coord_backpressure_wait.total_cycles /
                  s->prof.coord_backpressure_wait.count
            : 0);
  }
  printf("---------------------------------------------------------------------"
         "-----------------------------------------------------------\n\n");

  /* ── Message Queue Latency ────────────────────────────────────── */
  printf("  Message Queue Latency (Sent to Read Cycles)\n");
  printf("---------------------------------------------------------------------"
         "-----------------------------------------------------------\n");
  printf("%-6s %-30s %-30s\n", "shard", "mset_prepare_queue_latency",
         "mset_ack_queue_latency");
  for (uint32_t i = 0; i < e->num_shards; i++) {
    const struct shard *s = &e->shards[i];
    printf("%-6u %-30lu %-30lu\n", s->id,
           s->prof.mset_prepare_queue_latency.count
               ? s->prof.mset_prepare_queue_latency.total_cycles /
                     s->prof.mset_prepare_queue_latency.count
               : 0,
           s->prof.mset_ack_queue_latency.count
               ? s->prof.mset_ack_queue_latency.total_cycles /
                     s->prof.mset_ack_queue_latency.count
               : 0);
  }
  printf("---------------------------------------------------------------------"
         "-----------------------------------------------------------\n\n");

  /* ── Latency Profiling ────────────────────────────────────────── */
  printf("  Latency Profiling (Avg CPU Cycles per Op)\n");
  printf("---------------------------------------------------------------------"
         "-----------------------------------------------------------\n");
  printf("%-6s %-12s %-12s %-12s %-12s %-12s\n", "shard", "shard_key_get",
         "shard_key_set", "slab_obj_alloc", "wake_write", "queue_push");
  for (uint32_t i = 0; i < e->num_shards; i++) {
    const struct shard *s = &e->shards[i];
    printf("%-6u %-12lu %-12lu %-12lu %-12lu %-12lu\n", s->id,
           s->prof.get.count ? s->prof.get.total_cycles / s->prof.get.count : 0,
           s->prof.set.count ? s->prof.set.total_cycles / s->prof.set.count : 0,
           s->prof.slab_alloc.count
               ? s->prof.slab_alloc.total_cycles / s->prof.slab_alloc.count
               : 0,
           s->prof.wake_write.count
               ? s->prof.wake_write.total_cycles / s->prof.wake_write.count
               : 0,
           s->prof.queue_push.count
               ? s->prof.queue_push.total_cycles / s->prof.queue_push.count
               : 0);
  }
  printf("---------------------------------------------------------------------"
         "-----------------------------------------------------------\n\n");

  /* ── Lock Contention Stats ────────────────────────────────────── */
  printf("  Lock Contention Stats (Counts)\n");
  printf("---------------------------------------------------------------------"
         "-----------------------------------------------------------\n");
  printf("%-6s %-25s %-25s %-25s\n", "shard", "preempt_attempts",
         "preempt_successes", "wait_queue_adds");
  for (uint32_t i = 0; i < e->num_shards; i++) {
    const struct shard *s = &e->shards[i];
    printf("%-6u %-25lu %-25lu %-25lu\n", s->id,
           s->prof.lm_preempt_attempt.count, s->prof.lm_preempt_success.count,
           s->prof.lm_wq_add.count);
  }
  printf("---------------------------------------------------------------------"
         "-----------------------------------------------------------\n\n");
  /* ── MSET Call Counts ──────────────────────────────────────────────── */
  printf("  MSET Call Counts\n");
  printf("---------------------------------------------------------------------"
         "-----------------------------------------------------------\n");
  printf("%-6s %-12s %-12s %-12s %-12s %-12s %-12s %-12s %-12s %-12s\n",
         "shard", "dispatch", "prepare", "ack", "fin", "fin_ack", "drain",
         "e2e", "ht_insert", "lm_insert");
  for (uint32_t i = 0; i < e->num_shards; i++) {
    const struct shard *s = &e->shards[i];
    printf(
        "%-6u %-12lu %-12lu %-12lu %-12lu %-12lu %-12lu %-12lu %-12lu %-12lu\n",
        s->id, s->prof.mset_dispatch.count, s->prof.mset_prepare.count,
        s->prof.mset_ack.count, s->prof.mset_fin.count,
        s->prof.mset_fin_ack.count, s->prof.mset_drain.count,
        s->prof.mset_total_e2e.count, s->prof.ht_insert.count,
        s->prof.lm_insert.count);
  }
  printf("---------------------------------------------------------------------"
         "-----------------------------------------------------------\n\n");

  /* ── MSET PREPARE Round-Trip Percentiles ────────────────────────────── */
  printf("  MSET PREPARE Round-Trip Percentiles (cycles)  "
         "[dispatch → all ACKs received]\n");
  printf("---------------------------------------------------------------------"
         "-----------------------------------------------------------\n");
  printf("%-6s %-14s %-14s %-14s %-14s %-14s %-14s\n", "shard", "p50", "p75",
         "p95", "p99", "p999", "max");
  for (uint32_t i = 0; i < e->num_shards; i++) {
    const struct shard *s = &e->shards[i];
    uint64_t total = s->prof.mset_prepare_rtt.count;
    printf("%-6u %-14lu %-14lu %-14lu %-14lu %-14lu %-14lu\n", s->id,
           hist_percentile(&s->prof.mset_prepare_rtt_hist, total, 0.50),
           hist_percentile(&s->prof.mset_prepare_rtt_hist, total, 0.75),
           hist_percentile(&s->prof.mset_prepare_rtt_hist, total, 0.95),
           hist_percentile(&s->prof.mset_prepare_rtt_hist, total, 0.99),
           hist_percentile(&s->prof.mset_prepare_rtt_hist, total, 0.999),
           s->prof.mset_prepare_rtt.max_cycles);
  }
  printf("---------------------------------------------------------------------"
         "-----------------------------------------------------------\n\n");

  /* ── MSET E2E Latency Percentiles ───────────────────────────────────── */
  printf("  MSET E2E Latency Percentiles (cycles)\n");
  printf("---------------------------------------------------------------------"
         "-----------------------------------------------------------\n");
  printf("%-6s %-14s %-14s %-14s %-14s %-14s %-14s\n", "shard", "p50", "p75",
         "p95", "p99", "p999", "max");
  for (uint32_t i = 0; i < e->num_shards; i++) {
    const struct shard *s = &e->shards[i];
    uint64_t total = s->prof.mset_total_e2e.count;
    printf("%-6u %-14lu %-14lu %-14lu %-14lu %-14lu %-14lu\n", s->id,
           hist_percentile(&s->prof.mset_e2e_hist, total, 0.50),
           hist_percentile(&s->prof.mset_e2e_hist, total, 0.75),
           hist_percentile(&s->prof.mset_e2e_hist, total, 0.95),
           hist_percentile(&s->prof.mset_e2e_hist, total, 0.99),
           hist_percentile(&s->prof.mset_e2e_hist, total, 0.999),
           s->prof.mset_total_e2e.max_cycles);
  }
  printf("---------------------------------------------------------------------"
         "-----------------------------------------------------------\n\n");
#endif
}

bool shard_mem_evict(struct shard *s, size_t size_req) {
  bool force_entry_eviction = size_req == 0;
  uint32_t target_pidx = force_entry_eviction
                             ? NR_SMALL_POOLS
                             : (size_req <= SMALL_ALLOC_MAX
                                    ? pool_idx_get(size_req)
                                    : NR_SMALL_POOLS);
  uint32_t buddy_pages_needed = 0;
  if (force_entry_eviction) {
    buddy_pages_needed = 0;
  } else if (size_req <= SMALL_ALLOC_MAX) {
    struct small_pool *pool = &s->mem->pools[target_pidx];
    buddy_pages_needed =
        (pool->partial_spans || pool->empty_spans) ? 0 : pool->lpages_per_span;
  } else {
    buddy_pages_needed = (uint32_t)((size_req + LPAGE_SIZE - 1) / LPAGE_SIZE);
  }
  pool_span_force_reclaim_all(s->mem->pools, &s->mem->buddy, s->mem->pages,
                              &s->mem->span_meta_pool, s->mem->data_base);
  size_t mem_used = mem_usage_get(s->id);
  size_t mem_limit = s->mem->limit_bytes;
  bool soft_ok = (mem_limit == 0) || (mem_used + size_req <= mem_limit);
  bool hard_ok = (buddy_pages_needed == 0) ||
                 buddy_span_check(&s->mem->buddy, buddy_pages_needed);
  if (!force_entry_eviction && soft_ok && hard_ok)
    return true;
  int total_evicted = 0;
  while (true) {
    bool evicted_any = false;
    for (int b = 0; b < 100; b++) {
      struct kv_obj *victim = s->lru_tail[target_pidx];
      if (!victim)
        for (int p = LRU_POOLS - 1; p >= 0 && !victim; p--)
          victim = s->lru_tail[p];
      if (!victim)
        break;
      bool locked = false;
      struct kv_obj *taken = ht_bucket_take_if_unlocked(
          s->table, obj_key_get(victim), victim->key_len, VAL_TYPE_STRING,
          victim, &locked);
      if (taken) {
        shard_obj_destroy(s, taken);
        evicted_any = true;
        total_evicted++;
      } else if (locked) {
        lru_node_prepend(s, victim);
      } else {
        lru_node_remove(s, victim);
      }
    }
    if (!evicted_any)
      return false;
    if (force_entry_eviction)
      return true;
    if (total_evicted >= 200) {
      pool_span_force_reclaim_all(s->mem->pools, &s->mem->buddy, s->mem->pages,
                                  &s->mem->span_meta_pool, s->mem->data_base);
      total_evicted = 0;
    }
    mem_used = mem_usage_get(s->id);
    soft_ok = (mem_limit == 0) || (mem_used + size_req <= mem_limit);
    if (size_req <= SMALL_ALLOC_MAX &&
        (s->mem->pools[target_pidx].partial_spans ||
         s->mem->pools[target_pidx].empty_spans))
      buddy_pages_needed = 0;
    hard_ok = (buddy_pages_needed == 0) ||
              buddy_span_check(&s->mem->buddy, buddy_pages_needed);
    if (soft_ok && hard_ok)
      return true;
    if (s->table->used == 0)
      return false;
  }
}
