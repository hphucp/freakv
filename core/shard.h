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
#include "message.h"
#include "object.h"
#include "profile.h"
#include "snapshot.h"
#include "spsc.h"
#include "ttl_index.h"

#define LRU_POOLS (NR_SMALL_POOLS + 1)

struct shard {
  uint32_t id;
  int cpu_core;
  pthread_t thread;
  bool running;

  struct hash_table *table;
  struct thread_heap *mem; /* Pointer to thread-local heap in g_thread_heaps */
  struct ttl_index ttl_idx;
  struct slab_allocator *pool;

  struct spsc_queue **inboxes;
  uint32_t num_shards;

  /* One LRU list per pool class — head = MRU, tail = LRU. */
  struct kv_obj *lru_head[LRU_POOLS];
  struct kv_obj *lru_tail[LRU_POOLS];

  uint64_t ops_completed;
  uint64_t cross_shard_sent;
  uint64_t cross_shard_received;

  struct snap_state snap;
  struct shard_prof prof;
};

struct shard_engine {
  struct shard *shards;
  uint32_t num_shards;
  bool running;
  struct spsc_queue *queues;
  int *wake_fds;

  /* Global lock for atomic MSET: UINT32_MAX = free, otherwise = owner shard id
   */
  _Atomic uint32_t mset_lock;

  /* Allocator for engine-level structures */
  struct slab_allocator *init_pool;
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

bool shard_key_set(struct shard *s, const void *key, size_t klen,
                   const char *val, size_t vlen, uint64_t expire_ms,
                   uint32_t put_flags);

/*
 * shard_key_set_tentative — Phase 1 of atomic MSET.
 * Allocates new kv_obj and replaces HT entry, but does NOT free old_obj.
 * Returns old_obj (caller must hold it for commit/rollback), or NULL if key was
 * new. Returns (kv_obj*)-1 on allocation failure.
 */
struct kv_obj *shard_key_set_tentative(struct shard *s, const void *key,
                                       size_t klen, const char *val,
                                       size_t vlen);

/*
 * shard_key_set_commit — Phase 2a: finalize tentative SET.
 * Destroys old_obj (the one returned by _tentative).
 */
void shard_key_set_commit(struct shard *s, struct kv_obj *old_obj);

/*
 * shard_key_set_rollback — Phase 2b: undo tentative SET.
 * Restores old_obj into HT, destroys the new_obj that was placed by _tentative.
 * If old_obj is NULL, deletes the key entirely.
 */
void shard_key_set_rollback(struct shard *s, const void *key, size_t klen,
                            struct kv_obj *old_obj);

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

/* ── TTL active expiry ────────────────────────────────────────────────── */

int shard_ttl_drain(struct shard *s, uint64_t net_time_ms_get, int max_work);

/* ── SPSC drain ───────────────────────────────────────────────────────── */

typedef void (*shard_proxy_reply_fn)(void *ctx, const struct spsc_message *msg);

void shard_msg_drain(struct shard_engine *engine, struct shard *shard,
                     shard_proxy_reply_fn proxy_cb, void *proxy_ctx);
void proxy_exec(struct shard *s, struct spsc_message *msg, uint64_t cur);

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
