#ifndef PROFILE_H
#define PROFILE_H

#include <stdint.h>
#include <stdio.h>
#include <x86intrin.h>

#ifndef FREAKV_PROFILE
#define FREAKV_PROFILE 0
#endif

#ifndef FREAKV_MSET_DEBUG
#define FREAKV_MSET_DEBUG 0
#endif

#ifndef FREAKV_MIXED_PROFILE
#define FREAKV_MIXED_PROFILE 0
#endif

/* Simple cycle measurement using RDTSC */
static inline uint64_t cycles_now() { return __rdtsc(); }

#if FREAKV_PROFILE || FREAKV_MIXED_PROFILE
#define PROF_NOW() cycles_now()
#else
#define PROF_NOW() 0ULL
#endif

struct prof_metric {
  uint64_t total_cycles;
  uint64_t count;
  uint64_t max_cycles;
};

#if FREAKV_PROFILE
#define PROF_RECORD(metric, start)                                             \
  do {                                                                         \
    uint64_t delta = cycles_now() - (start);                                   \
    (metric).total_cycles += delta;                                            \
    (metric).count++;                                                          \
    if (delta > (metric).max_cycles)                                           \
      (metric).max_cycles = delta;                                             \
  } while (0)
#else
#define PROF_RECORD(metric, start)                                             \
  do {                                                                         \
  } while (0)
#endif

/* ── Latency histogram (power-of-2 buckets) ─────────────────────────── */

#define PROF_HIST_BUCKETS 40  /* covers ~1 cycle to ~2^40 cycles (~6 min) */

struct prof_hist {
  uint64_t buckets[PROF_HIST_BUCKETS];
};

/*
 * Record to both a prof_metric and a prof_hist in one RDTSC read.
 * bucket k covers [2^k, 2^(k+1)) cycles.
 */
#if FREAKV_PROFILE
#define PROF_RECORD_HIST(metric, hist, start)                                  \
  do {                                                                         \
    uint64_t _delta = cycles_now() - (uint64_t)(start);                       \
    (metric).total_cycles += _delta;                                           \
    (metric).count++;                                                          \
    if (_delta > (metric).max_cycles) (metric).max_cycles = _delta;           \
    int _b = (_delta < 2) ? 0 : (63 - __builtin_clzll(_delta));               \
    if (_b >= PROF_HIST_BUCKETS) _b = PROF_HIST_BUCKETS - 1;                  \
    (hist).buckets[_b]++;                                                      \
  } while (0)
#else
#define PROF_RECORD_HIST(metric, hist, start)                                  \
  do {                                                                         \
  } while (0)
#endif

/*
 * Return the approximate cycle count at percentile p (0.0–1.0).
 * Returns the lower bound of the bucket: 2^k for bucket k.
 */
static inline uint64_t hist_percentile(const struct prof_hist *h,
                                       uint64_t total, double p) {
  if (total == 0) return 0;
  uint64_t target = (uint64_t)((double)total * p);
  if (target == 0) target = 1;
  uint64_t accum = 0;
  for (int b = 0; b < PROF_HIST_BUCKETS; b++) {
    accum += h->buckets[b];
    if (accum >= target)
      return (b == 0) ? 1ULL : (1ULL << b);
  }
  return 1ULL << (PROF_HIST_BUCKETS - 1);
}

struct shard_prof {
  struct prof_metric alloc;
  struct prof_metric free_std;   /* Snapshots OFF path */
  struct prof_metric free_snap;  /* Snapshots ON path (defer_free) */
  struct prof_metric mark_snap;  /* snap_obj_mark path */
  struct prof_metric merge_snap; /* pool_snap_merge path */
  struct prof_metric get;        /* shard_key_get */
  struct prof_metric set;        /* shard_key_set */
  struct prof_metric
      snap_duration;            /* Total Time taken by background snap thread */
  struct prof_metric snap_open; /* I/O Open phase */
  struct prof_metric
      snap_scan; /* Pure CPU logic processing and dirty bits traversal */
  struct prof_metric snap_write; /* Copying data to mmap and growing file */
  struct prof_metric snap_close; /* I/O Sync and Truncate */
  struct prof_metric snap_cache; /* Cache miss latency: time to load bucket
                                    obj/old_ptr from RAM */

  /* Async MSET 2PC metrics */
  struct prof_metric mset_dispatch;
  struct prof_metric mset_prepare;
  struct prof_metric mset_ack;
  struct prof_metric mset_fin;
  struct prof_metric mset_fin_ack;
  struct prof_metric mset_drain;

  /* Granular MSET metrics */
  struct prof_metric hash_shard;
  struct prof_metric slab_alloc;
  struct prof_metric queue_push;
  struct prof_metric wake_write;
  struct prof_metric ht_lookup;
  struct prof_metric ht_insert;
  struct prof_metric obj_alloc;
  struct prof_metric lm_lookup;
  struct prof_metric lm_insert;
  struct prof_metric lm_remove;
  struct prof_metric lm_wq_add;
  struct prof_metric lm_wq_find;
  struct prof_metric ht_rehash;
  struct prof_metric mset_total_e2e;
  struct prof_hist   mset_e2e_hist;

  /* Time from coordinator dispatch to receiving all PREPARE ACKs
   * (start_cycles → mset_broadcast_fin). Measures the PREPARE round-trip. */
  struct prof_metric mset_prepare_rtt;
  struct prof_hist   mset_prepare_rtt_hist;

  /* Coordinator breakdown */
  struct prof_metric coord_stat_alloc;
  struct prof_metric coord_hash;
  struct prof_metric coord_queue_wait;
  struct prof_metric coord_local_exec;
  struct prof_metric time_now;

  /* Internal breakdown */
  struct prof_metric ht_get_internal;
  struct prof_metric ht_put_internal;
  struct prof_metric lm_preempt_attempt;
  struct prof_metric lm_preempt_success;

  /* New detailed metrics */
  struct prof_metric coord_backpressure_wait;
  struct prof_metric mset_prepare_queue_latency;
  struct prof_metric mset_ack_queue_latency;
};

#endif /* PROFILE_H */
