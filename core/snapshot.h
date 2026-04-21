#ifndef SNAPSHOT_H
#define SNAPSHOT_H

#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

extern bool g_snapshot_active;

/* Forward declarations */
struct shard;
struct thread_heap;

/* ── File format ─────────────────────────────────────────────────────── */

#define SNAP_MAGIC 0x56414C53U /* "VALS" */
#define SNAP_VERSION 2

/*
 * File naming:
 *   session-{session_id}-shard{shard_id}-epoch-{epoch_id}.snap
 *
 * session_id : unix timestamp ms at server start — unique per run
 * shard_id   : owning shard
 * epoch_id   : monotonic counter within session, starts at 1
 *
 * Each epoch file is a self-contained incremental snapshot covering
 * only the slots dirtied since the previous epoch.
 *
 * File layout:
 *   [snap_file_header]
 *   for each dirty page:
 *     [snap_page_record]
 *     for each dirty slot in that page:
 *       [snap_object_entry]
 *       if (!deleted): key_len bytes + val_len bytes of payload
 *   [snap_file_footer]
 *
 * Recovery (new session):
 *   1. Find all epoch files of the newest session_id.
 *   2. Iterate epochs from highest to lowest.
 *   3. For each (lpage_idx, slot_index) seen for the first time:
 *        if deleted  → skip (key was erased in that slot)
 *        if !deleted → shard_key_set(key, value, expire)
 *   4. After the first epoch of the new session is committed,
 *      delete all files of the old session.
 */

struct __attribute__((packed)) snap_file_header {
  uint32_t magic;
  uint32_t version;
  uint32_t shard_id;
  uint32_t
      num_page_records;  /* number of snap_page_record entries that follow */
  uint64_t session_id;   /* unix ms at server start                        */
  uint64_t epoch;        /* monotonic counter within session               */
  uint64_t timestamp_ms; /* wall-clock time when snapshot was written      */
};

/*
 * One record per dirty page, followed by num_objects snap_object_entry
 * records for the dirty slots within that page.
 */
struct __attribute__((packed)) snap_page_record {
  uint32_t lpage_idx;      /* logical page index in thread heap              */
  uint32_t obj_size;       /* pool class object size for this page           */
  uint16_t offset_in_span; /* page's offset within its buddy span            */
  uint16_t num_objects;    /* number of snap_object_entry records that follow*/
};

/*
 * One record per dirty slot.
 *   deleted == 0: key_len + val_len bytes of payload follow immediately.
 *   deleted == 1: tombstone — no payload bytes follow.
 *
 * The (lpage_idx, slot_index) pair from the enclosing snap_page_record
 * uniquely identifies the memory slot within the session's address space.
 * It is used as a dedup key when merging multiple epochs during recovery.
 */
struct __attribute__((packed)) snap_object_entry {
  uint16_t slot_index; /* local slot index within this page              */
  uint16_t key_len;    /* 0 when deleted == 1                            */
  uint32_t val_len;    /* 0 when deleted == 1                            */
  uint64_t expire_ms;  /* absolute expiry epoch ms; 0 = no TTL          */
  uint8_t deleted;     /* 1 = tombstone                                  */
  uint8_t _pad[7];
};

struct __attribute__((packed)) snap_file_footer {
  uint32_t checksum; /* CRC32 of all bytes before footer (0 = unused) */
};

/* ── Per-shard snapshot runtime state ───────────────────────────────── */

struct snap_state {
  uint64_t session_id;           /* copied from global at init             */
  uint64_t current_epoch;        /* 0 = no epoch written yet               */
  bool old_files_pending_delete; /* true until first new epoch committed   */
  uint64_t old_session_id;       /* session whose files must be deleted     */

  char filepath[512]; /* path of last-written epoch file        */

  /* Config */
  char snap_dir[256];
  uint32_t interval_ms; /* 0 = snapshots disabled                 */
  uint64_t last_snap_ms;

  /* Background thread state */
  pthread_t snap_thread;        /* background thread handle        */
  int snap_eventfd;             /* eventfd: main -> snap wake      */
  _Atomic bool is_snapshotting; /* backpressure flag               */
  bool snap_thread_exit;        /* signal thread to terminate      */
};

/* ── API ─────────────────────────────────────────────────────────────── */

/*
 * Initialize snapshot engine for one shard.
 * session_id must be identical for all shards in the same server run —
 * obtain it via snap_session_id_get() which generates it once at startup.
 */
void snap_engine_init(struct snap_state *ss, uint32_t shard_id,
                      uint64_t session_id, const char *snap_dir,
                      uint32_t interval_ms);

/*
 * Called every reactor tick. Fires an incremental snapshot when the
 * configured interval has elapsed. On first successful epoch write,
 * deletes the old session's snapshot files.
 */
void snap_engine_tick(struct snap_state *ss, struct shard *shard);

/*
 * Mark a kv_obj slot as dirty in both the page-level dirty_bits[]
 * bitmap and the per-page modified[] bitmap.
 *
 * Must be called:
 *   - after allocating a new kv_obj (SET / overwrite new object)
 *   - before freeing a kv_obj (DELETE / overwrite old object)
 *     with marked_deletion already set to 1 for the delete case.
 *
 * obj_size must be the pool class size (POOL_CLASSES[pool_idx]).
 * For buddy (large) allocations obj_size == 0 — call is a no-op.
 */
void snap_obj_mark(struct snap_state *ss, struct thread_heap *heap,
                   const void *ptr, uint32_t obj_size);

/*
 * Clear the modified bit for a slot after it has been serialized.
 * Called only by the snapshot serializer, not by the write path.
 */
void snap_obj_clear_modified(struct thread_heap *heap, uint32_t lpage_idx,
                             uint32_t slot);

/*
 * Restore from snapshot files found in snap_dir for shard->id.
 * Finds the newest session, reads all its epoch files newest-first,
 * deduplicates by (lpage_idx, slot_index), and calls shard_key_set
 * for each live entry.
 *
 * Returns the number of keys restored (>= 0).
 * Writes the old session_id into *old_session_id_out (0 if none found).
 * Does NOT delete old files — deletion is deferred to snap_engine_tick
 * after the first new epoch is committed.
 */
int snap_engine_restore(struct shard *shard, const char *snap_dir,
                        uint64_t *old_session_id_out);

void snap_engine_destroy(struct snap_state *ss);

/* ── Global config ───────────────────────────────────────────────────── */

void snap_config_set(const char *dir, uint32_t interval_ms);
const char *snap_config_dir_get(void);
uint32_t snap_config_interval_get(void);

/*
 * Returns the session_id for this server run.
 * Generated once (unix ms at first call). Thread-safe after server.c
 * calls it before spawning shard threads.
 */
uint64_t snap_session_id_get(void);

#endif /* SNAPSHOT_H */
