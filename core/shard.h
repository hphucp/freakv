#ifndef SHARD_H
#define SHARD_H

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "../hashtable/htable.h"
#include "../mem/mem_heap.h"
#include "../memory/slab.h"
#include "lock_manager.h"
#include "message.h"
#include "object.h"
#include "profile.h"
#include "snapshot.h"
#include "spsc.h"
#include "ttl_index.h"

#define LRU_POOLS (NR_SMALL_POOLS + 1)

typedef void (*shard_proxy_reply_fn)(void *ctx, const struct spsc_message *msg);
struct net_conn;
typedef void (*shard_conn_dirty_fn)(void *ctx, struct net_conn *c,
                                    const char *reason);

struct shard_engine;  /* forward declaration — defined below */

struct shard {
  uint32_t id;
  int cpu_core;
  pthread_t thread;
  bool running;

  struct shard_engine *engine;  /* back-pointer to parent engine */

  /* Per-shard MSET back-pressure: limits concurrent in-flight MSETs for
   * this coordinator, independent of other shards. Single-threaded access
   * (reactor thread only) — no atomic needed. */
  uint32_t mset_inflight;
  uint32_t mset_max_concurrent;
  struct net_conn *mset_pending_conn_head;
  struct net_conn *mset_pending_conn_tail;
  uint32_t mset_pending_conn_len;
  uint64_t mset_pending_conn_enqueued;
  uint64_t mset_pending_conn_resumed;
  uint32_t mset_pending_conn_max_len;

  /* Callback used by mset back-pressure drain (set by reactor at init). */
  shard_proxy_reply_fn proxy_cb;
  shard_conn_dirty_fn mark_conn_dirty_cb;
  void *mark_conn_dirty_ctx;

  struct hash_table *table;
  struct thread_heap *mem; /* Pointer to thread-local heap in g_thread_heaps */
  struct ttl_index ttl_idx;
  struct slab_allocator *pool;

  struct lock_manager lm;  /* key-level lock manager for async MSET */

  struct spsc_queue **inboxes;
  uint32_t num_shards;

  /* One LRU list per pool class — head = MRU, tail = LRU. */
  struct kv_obj *lru_head[LRU_POOLS];
  struct kv_obj *lru_tail[LRU_POOLS];

  uint64_t ops_completed;
  uint64_t cross_shard_sent;
  uint64_t cross_shard_received;
#if FREAKV_MSET_DEBUG
  uint64_t mset_debug_last_ms;
  uint64_t mset_debug_progress_last_ms;
  uint64_t mset_debug_last_dispatch;
  uint64_t mset_debug_last_e2e;
  bool mset_debug_all_complete_logged;
#endif

  struct snap_state snap;
#if FREAKV_PROFILE
  struct shard_prof prof;
#endif
};

struct shard_engine {
  struct shard *shards;
  uint32_t num_shards;
  bool running;
  struct spsc_queue *queues;
  int *wake_fds;

  /* Allocator for engine-level structures */
  struct slab_allocator *init_pool;

  /* (mset_inflight and mset_max_concurrent moved to struct shard) */
};

/* ── Engine lifecycle ─────────────────────────────────────────────────── */

struct shard_engine *shard_engine_create(struct slab_allocator *init_pool,
                                         uint32_t num_shards);
void shard_engine_stop(struct shard_engine *engine);
void shard_engine_destroy(struct shard_engine *engine);

/* ── Routing ──────────────────────────────────────────────────────────── */

uint32_t num_cpu_count(void);

/* ── Hot-path ops ─────────────────────────────────────────────────────── */

void shard_obj_destroy(struct shard *s, struct kv_obj *o);
void shard_obj_ref(struct kv_obj *o);
void shard_obj_unref(struct shard *s, struct kv_obj *o);

/* LRU helpers (used by mset_exec.c) */
void lru_node_remove(struct shard *s, struct kv_obj *o);
void lru_node_prepend(struct shard *s, struct kv_obj *o);

bool shard_key_set(struct shard *s, const void *key, size_t klen,
                   const char *val, size_t vlen, uint64_t expire_ms,
                   uint32_t put_flags);

struct kv_obj *shard_key_get(struct shard *s, const void *key, size_t klen,
                             uint64_t net_time_ms_get);
bool shard_key_delete(struct shard *s, const void *key, size_t klen);
void shard_table_flush(struct shard *s);
bool shard_key_expire(struct shard *s, const void *key, size_t klen,
                      uint64_t expire_ms);
long long shard_key_ttl(struct shard *s, const void *key, size_t klen,
                        uint64_t net_time_ms_get, bool ms);
uint64_t shard_key_expire_get(struct shard *s, const void *key, size_t klen);
struct kv_obj *shard_obj_put(struct shard *s, struct kv_obj *obj,
                             enum val_type type, uint64_t expire_ms);
struct kv_obj *shard_bucket_put(struct shard *s, struct kv_obj *obj,
                                enum val_type type, uint64_t expire_ms,
                                uint32_t put_flags);

/* ── TTL active expiry ────────────────────────────────────────────────── */

int shard_ttl_drain(struct shard *s, uint64_t net_time_ms_get, int max_work);

/* ── SPSC drain ───────────────────────────────────────────────────────── */

void shard_msg_drain(struct shard_engine *engine, struct shard *shard,
                     shard_proxy_reply_fn proxy_cb, void *proxy_ctx);
enum proxy_exec_result {
  PROXY_EXEC_DONE = 0,
  PROXY_EXEC_DEFERRED = 1,
};
enum proxy_exec_result proxy_exec(struct shard *s, struct spsc_message *msg,
                                  uint64_t cur);

/* ── Stats ────────────────────────────────────────────────────────────── */

struct shard_stats {
  uint32_t shard_id;
  int cpu_core;
  size_t table_size;
  uint64_t ops_completed;
  uint64_t cross_shard_sent;
  uint64_t cross_shard_received;
};

void shard_stats_get(const struct shard *shard, struct shard_stats *out);
void shard_engine_stats_print(const struct shard_engine *engine);

/* ── Background Snap ─────────────────────────────────────────────────── */

bool snap_thread_spawn(struct shard *shard);

#endif /* SHARD_H */
