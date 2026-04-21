#ifndef PROFILE_H
#define PROFILE_H

#include <stdint.h>
#include <stdio.h>
#include <x86intrin.h>

/* Simple cycle measurement using RDTSC */
static inline uint64_t cycles_now() { return __rdtsc(); }

struct prof_metric {
  uint64_t total_cycles;
  uint64_t count;
  uint64_t max_cycles;
};

#define PROF_RECORD(metric, start)                                             \
  do {                                                                         \
    uint64_t delta = cycles_now() - (start);                                   \
    (metric).total_cycles += delta;                                            \
    (metric).count++;                                                          \
    if (delta > (metric).max_cycles)                                           \
      (metric).max_cycles = delta;                                             \
  } while (0)

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
};

#endif /* PROFILE_H */