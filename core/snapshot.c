#ifndef _GNU_SOURCE
#define _GNU_SOURCE /* mremap, MREMAP_MAYMOVE */
#endif
#include "snapshot.h"
#include "../mem/mem_api.h"
#include "../mem/mem_buddy.h"
#include "../mem/mem_heap.h"
#include "../mem/mem_internal.h"
#include "../hashtable/htable.h"
#include "object.h"
#include "shard.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

static char g_snap_dir[256] = "";
static uint32_t g_snap_interval = 0;
static uint64_t g_session_id = 0;
bool g_snapshot_active = false;

void snap_config_set(const char *dir, uint32_t interval_ms) {
  if (dir && dir[0])
    snprintf(g_snap_dir, sizeof(g_snap_dir), "%s", dir);
  g_snap_interval = interval_ms;
  g_snapshot_active = (interval_ms > 0);
}

const char *snap_config_dir_get(void) { return g_snap_dir; }
uint32_t snap_config_interval_get(void) { return g_snap_interval; }

uint64_t snap_session_id_get(void) {
  if (g_session_id == 0) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    g_session_id =
        (uint64_t)ts.tv_sec * 1000ULL + (uint64_t)ts.tv_nsec / 1000000ULL;
  }
  return g_session_id;
}

/* ── Helpers ─────────────────────────────────────────────────────────── */

static uint64_t snap_time_ms(void) {
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return (uint64_t)ts.tv_sec * 1000ULL + (uint64_t)ts.tv_nsec / 1000000ULL;
}

/* ── File naming helpers (Compaction mode) ──────────────────────────────── */

static void snap_base_filename(char *buf, size_t bufsz, const char *snap_dir,
                               uint32_t shard_id) {
  if (snap_dir[0])
    snprintf(buf, bufsz, "%s/shard%u-base.fsnap", snap_dir, shard_id);
  else
    snprintf(buf, bufsz, "shard%u-base.fsnap", shard_id);
}

static void snap_delta_filename(char *buf, size_t bufsz, const char *snap_dir,
                                uint32_t shard_id, uint64_t epoch) {
  if (snap_dir[0])
    snprintf(buf, bufsz, "%s/shard%u-delta-%" PRIu64 ".fsnap", snap_dir,
             shard_id, epoch);
  else
    snprintf(buf, bufsz, "shard%u-delta-%" PRIu64 ".fsnap", shard_id, epoch);
}

static void snap_base_tmp_filename(char *buf, size_t bufsz,
                                   const char *snap_dir, uint32_t shard_id) {
  if (snap_dir[0])
    snprintf(buf, bufsz, "%s/shard%u-base-new.tmp", snap_dir, shard_id);
  else
    snprintf(buf, bufsz, "shard%u-base-new.tmp", shard_id);
}

/* ── Background Threading ────────────────────────────────────────────── */

static bool snap_serialize_epoch(struct snap_state *ss, struct shard *shard);
static void snap_compact_delta_into_base(struct snap_state *ss,
                                         struct shard *shard, uint64_t epoch);

static void *snap_background_worker(void *arg) {
  struct shard *shard = (struct shard *)arg;
  struct snap_state *ss = &shard->snap;

  char name[16];
  snprintf(name, sizeof(name), "snap_%u", shard->id);
  pthread_setname_np(pthread_self(), name);

  for (;;) {
    uint64_t val;
    if (read(ss->snap_eventfd, &val, sizeof(val)) <= 0) {
      if (errno == EINTR)
        continue;
      break;
    }

    if (ss->snap_thread_exit)
      break;

    uint64_t epoch_just_written = ss->current_epoch;

    /* Memory mode Phase 1: write delta file */
    bool ok = snap_serialize_epoch(ss, shard);

    /* ── Signal main thread immediately after Phase 1 ──
     * Main thread can now proceed with freelist merge.
     * Phase 2 (compaction) runs silently in the background. */
    atomic_store_explicit(&ss->is_snapshotting, false, memory_order_release);

    /* Phase 2: compact delta into base (no main-thread interaction) */
    if (ok)
      snap_compact_delta_into_base(ss, shard, epoch_just_written);
  }

  return NULL;
}

/* ── Init / Destroy ──────────────────────────────────────────────────── */

void snap_engine_init(struct snap_state *ss, uint32_t shard_id,
                      uint64_t session_id, const char *snap_dir,
                      uint32_t interval_ms) {
  memset(ss, 0, sizeof(*ss));
  ss->session_id = session_id;
  ss->interval_ms = interval_ms;
  ss->last_snap_ms = snap_time_ms();
  atomic_init(&ss->is_snapshotting, false);

  if (snap_dir && snap_dir[0]) {
    snprintf(ss->snap_dir, sizeof(ss->snap_dir), "%s", snap_dir);
    mkdir(ss->snap_dir, 0755);
  }

  if (interval_ms > 0) {
    ss->snap_eventfd = eventfd(0, EFD_CLOEXEC);
    if (ss->snap_eventfd < 0) {
      perror("[snapshot] eventfd create failed");
      return;
    }
  }
}

void snap_engine_destroy(struct snap_state *ss) {
  (void)ss; /* Nothing to clean up — writer is local to snap_serialize_epoch */
}

/* ── Dirty marking ───────────────────────────────────────────────────── */

void snap_obj_mark(struct snap_state *ss, struct thread_heap *heap,
                   const void *ptr, uint32_t obj_size) {
  (void)ss;
  if (!heap || !heap->dirty_bits)
    return;

  uint32_t lpage =
      (uint32_t)(((const char *)ptr - heap->data_base) >> LPAGE_SHIFT);
  if (lpage >= heap->nr_pages_committed)
    return;
  struct page_info *pi = &heap->pages[lpage];

  /* For large objects (obj_size == 0), we mark the head of the span
   * and track its modification at slot index 0. */
  if (obj_size == 0) {
    uint32_t head_lpage = lpage - pi->offset_in_span;
    if (!dirty_bit_test(heap->dirty_bits, head_lpage)) {
      dirty_bit_set(heap->dirty_bits, head_lpage);
      heap->nr_pages_dirty++;
    }
    modified_bit_set(heap->slot_bits, head_lpage, 0);
    return;
  }

  /* Mark page dirty in the page-level bitmap */
  if (!dirty_bit_test(heap->dirty_bits, lpage)) {
    dirty_bit_set(heap->dirty_bits, lpage);
    heap->nr_pages_dirty++;
  }

  /* Use ptr_to_page_slot for correct slot index on any page of a span */
  uint32_t slot = ptr_to_page_slot(ptr, heap->data_base, heap->pages, obj_size);
  if (slot != INVALID)
    modified_bit_set(heap->slot_bits, lpage, slot);
}

void snap_obj_clear_modified(struct thread_heap *heap, uint32_t lpage_idx,
                             uint32_t slot) {
  if (!heap)
    return;
  modified_bit_clear(heap->slot_bits, lpage_idx, slot);
}

/* ── mmap-based sequential file writer ──────────────────────────────── */

#define SNAP_CHUNK_SIZE (2ULL * 1024 * 1024) /* 2 MB per grow step */

typedef struct {
  int fd;
  uint8_t *base;   /* start of current mmap region   */
  size_t map_size; /* total reserved bytes in region */
  size_t used;     /* bytes written so far           */
} snap_writer_t;

static bool snap_writer_open(snap_writer_t *w, const char *path) {
  memset(w, 0, sizeof(*w));
  w->fd = open(path, O_RDWR | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
  if (w->fd < 0)
    return false;

  w->map_size = SNAP_CHUNK_SIZE;
  if (ftruncate(w->fd, (off_t)w->map_size) < 0) {
    close(w->fd);
    w->fd = -1;
    return false;
  }
  w->base = (uint8_t *)mmap(NULL, w->map_size, PROT_READ | PROT_WRITE,
                            MAP_SHARED, w->fd, 0);
  if (w->base == MAP_FAILED) {
    close(w->fd);
    w->fd = -1;
    return false;
  }
  return true;
}

static bool snap_writer_grow(snap_writer_t *w) {
  size_t new_size = w->map_size + SNAP_CHUNK_SIZE;
  if (ftruncate(w->fd, (off_t)new_size) < 0)
    return false;
  void *nb = mremap(w->base, w->map_size, new_size, MREMAP_MAYMOVE);
  if (nb == MAP_FAILED)
    return false;
  w->base = (uint8_t *)nb;
  w->map_size = new_size;
  return true;
}

static void *snap_writer_reserve(snap_writer_t *w, size_t len) {
  while (w->used + len > w->map_size)
    if (!snap_writer_grow(w))
      return NULL;
  void *p = w->base + w->used;
  w->used += len;
  return p;
}

static bool snap_writer_put(snap_writer_t *w, const void *data, size_t len) {
  void *p = snap_writer_reserve(w, len);
  if (!p)
    return false;
  memcpy(p, data, len);
  return true;
}

static bool snap_writer_close(snap_writer_t *w) {
  bool ok = true;
  if (w->base) {
    if (msync(w->base, w->used, MS_ASYNC) < 0)
      ok = false;
    munmap(w->base, w->map_size);
    w->base = NULL;
  }
  if (w->fd >= 0) {
    if (ftruncate(w->fd, (off_t)w->used) < 0)
      ok = false;
    close(w->fd);
    w->fd = -1;
  }
  return ok;
}

static void snap_writer_abort(snap_writer_t *w, const char *path) {
  if (w->base) {
    munmap(w->base, w->map_size);
    w->base = NULL;
  }
  if (w->fd >= 0) {
    close(w->fd);
    w->fd = -1;
  }
  if (path)
    unlink(path);
}

/* ── Memory Mode Serializer ─────────────────────────────────────────── */

static bool snap_serialize_epoch(struct snap_state *ss, struct shard *shard) {
  struct thread_heap *heap = shard->mem;
  if (!heap || !heap->snap_dirty_bits)
    return false;

  /* Phase 1 writes a delta file; Phase 2 will merge it into base */
  snap_delta_filename(ss->filepath, sizeof(ss->filepath), ss->snap_dir,
                      shard->id, ss->current_epoch);

  snap_writer_t w;
  if (!snap_writer_open(&w, ss->filepath)) {
    fprintf(stderr, "[shard %u] snapshot: cannot open %s: %s\n", shard->id,
            ss->filepath, strerror(errno));
    return false;
  }
  const size_t hdr_offset = 0;
  {
    struct snap_file_header hdr_init = {
        .magic = SNAP_MAGIC,
        .version = SNAP_VERSION,
        .shard_id = shard->id,
        .num_page_records = 0,
        .session_id = ss->session_id,
        .epoch = ss->current_epoch,
        .timestamp_ms = snap_time_ms(),
    };
    bool _ok = snap_writer_put(&w, &hdr_init, sizeof(hdr_init));
    if (!_ok)
      goto fail;
  }

  uint32_t page_records = 0;
  uint32_t total_slots = 0;
  uint32_t nr_pages = heap->nr_pages_committed;
  uint32_t nr_words = (nr_pages + 63) / 64;

  struct span_info *last_span = NULL;

  for (uint32_t wi = 0; wi < nr_words; wi++) {
    uint64_t word = heap->snap_dirty_bits[wi];
    if (!word)
      continue;

    heap->snap_dirty_bits[wi] = 0;

    while (word) {
      uint32_t bit = (uint32_t)__builtin_ctzll(word);
      uint32_t lpage = wi * 64 + bit;
      word &= word - 1;

      if (lpage >= nr_pages)
        break;
      struct page_info *pi = &heap->pages[lpage];

      struct span_info *this_span = pi->span;
      if (this_span != last_span) {
        if (last_span && last_span->pool && last_span->snap_freelist != NULL) {
          if (!pool_snap_enqueue(last_span->pool, last_span)) {
            uint32_t old = atomic_load_explicit(&last_span->pool->to_merge_head,
                                                memory_order_relaxed);
            do {
              last_span->next_to_merge = old;
            } while (!atomic_compare_exchange_weak_explicit(
                &last_span->pool->to_merge_head, &old, last_span->head_lpage,
                memory_order_release, memory_order_relaxed));
          }
        }
        last_span = this_span;
      }

      uint32_t obj_size, first_off, slots_in_page;
      if (pi->span && pi->span->pool) {
        obj_size = pi->span->pool->obj_size;
        first_off = page_first_slot_offset(lpage, heap->pages, obj_size);
        slots_in_page = (LPAGE_SIZE - first_off) / obj_size;
      } else {
        obj_size = 0;
        first_off = 0;
        slots_in_page = 1;
        if (pi->span_size == 0)
          continue;
      }

      if (slots_in_page == 0)
        continue;

      uint16_t n_dirty = 0;
      for (uint32_t s = 0; s < slots_in_page && s < SNAP_MODIFIED_SIZE * 8;
           s++) {
        if (modified_bit_test(heap->snap_slot_bits, lpage, s))
          n_dirty++;
      }
      if (n_dirty == 0) {
        memset(heap->snap_slot_bits + (size_t)lpage * SNAP_MODIFIED_SIZE, 0,
               SNAP_MODIFIED_SIZE);
        continue;
      }

      struct snap_page_record *pr =
          (struct snap_page_record *)snap_writer_reserve(&w, sizeof(*pr));
      if (!pr)
        goto fail;
      *pr = (struct snap_page_record){
          .lpage_idx = lpage,
          .obj_size = obj_size,
          .offset_in_span =
              (uint16_t)(pi->span ? (lpage - pi->span->head_lpage) : 0),
          .num_objects = n_dirty,
      };
      page_records++;

      char *page_base = heap->data_base + (size_t)lpage * LPAGE_SIZE;

      for (uint32_t s = 0; s < slots_in_page && s < SNAP_MODIFIED_SIZE * 8;
           s++) {
        if (!modified_bit_test(heap->snap_slot_bits, lpage, s))
          continue;

        struct kv_obj *o =
            (struct kv_obj *)(page_base + first_off + (size_t)s * obj_size);

        uint8_t current_flag =
            atomic_load_explicit(&o->snap_flag, memory_order_acquire);

        bool is_deleted = false;
        bool snap_should_unref = false;

        uint8_t expected = SNAP_FLAG_LIVE_FRESH;
        if (current_flag == SNAP_FLAG_LIVE_FRESH &&
            atomic_compare_exchange_strong_explicit(
                &o->snap_flag, &expected, SNAP_FLAG_LIVE_VISITED,
                memory_order_acq_rel, memory_order_acquire)) {
          /* Won CAS */
        } else {
          expected =
              (current_flag == SNAP_FLAG_LIVE_FRESH) ? expected : current_flag;
          if (expected == SNAP_FLAG_DEAD_PENDING) {
            atomic_store_explicit(&o->snap_flag, SNAP_FLAG_DEAD_SAFE,
                                  memory_order_release);
          } else if (expected == SNAP_FLAG_DEAD_SAFE) {
            is_deleted = true;
            snap_should_unref = true;
          }
        }

        struct snap_object_entry ent = {
            .slot_index = (uint16_t)s,
            .expire_ms = o->expire_ms,
            .deleted = is_deleted,
            ._pad = {0},
        };
        if (!is_deleted) {
          ent.key_len = o->key_len;
          ent.val_len = o->val_len;
        }

        bool _ok1 = snap_writer_put(&w, &ent, sizeof(ent));
        if (!_ok1)
          goto fail;

        if (!is_deleted) {
          size_t kv_len = (size_t)o->key_len + (size_t)o->val_len;
          bool _ok2 = snap_writer_put(&w, o->data, kv_len);
          if (!_ok2)
            goto fail;
        }

        if (snap_should_unref) {
          if (atomic_fetch_sub_explicit(&o->refcount, 1,
                                        memory_order_release) == 1) {
            atomic_thread_fence(memory_order_acquire);
            mem_snap_free(o);
          }
        }
        total_slots++;
      }
      memset(heap->snap_slot_bits + (size_t)lpage * SNAP_MODIFIED_SIZE, 0,
             SNAP_MODIFIED_SIZE);
    }
  }

  if (last_span && last_span->pool && last_span->snap_freelist != NULL) {
    if (!pool_snap_enqueue(last_span->pool, last_span)) {
      uint32_t old = atomic_load_explicit(&last_span->pool->to_merge_head,
                                          memory_order_relaxed);
      do {
        last_span->next_to_merge = old;
      } while (!atomic_compare_exchange_weak_explicit(
          &last_span->pool->to_merge_head, &old, last_span->head_lpage,
          memory_order_release, memory_order_relaxed));
    }
  }

  ((struct snap_file_header *)(w.base + hdr_offset))->num_page_records =
      page_records;

  struct snap_file_footer footer = {.checksum = 0};
  bool _okf = snap_writer_put(&w, &footer, sizeof(footer));
  if (!_okf)
    goto fail;

  if (!snap_writer_close(&w)) {
    fprintf(stderr, "[shard %u] snapshot closure failed: %s\n", shard->id,
            strerror(errno));
    return false;
  }
  printf("[shard %u] snapshot epoch=%" PRIu64 ": %u pages, %u slots -> %s\n",
         shard->id, ss->current_epoch, page_records, total_slots, ss->filepath);
  fflush(stdout);

  return true;

fail:
  snap_writer_abort(&w, ss->filepath);
  return false;
}

/* ── Phase 2: Background Compaction ─────────────────────────────────── */

typedef struct {
  uint8_t *bits;
  size_t nr_bits;
} compact_dedup_t;

static bool compact_dedup_init(compact_dedup_t *d, uint32_t nr_pages) {
  size_t nr_bits = (size_t)nr_pages * SNAP_MODIFIED_SIZE * 8;
  size_t nr_bytes = (nr_bits + 7) / 8;
  d->bits = (uint8_t *)calloc(nr_bytes, 1);
  if (!d->bits)
    return false;
  d->nr_bits = nr_bits;
  return true;
}

static void compact_dedup_free(compact_dedup_t *d) {
  free(d->bits);
  d->bits = NULL;
}

static bool compact_dedup_mark(compact_dedup_t *d, uint32_t lpage,
                               uint16_t slot) {
  size_t idx = (size_t)lpage * SNAP_MODIFIED_SIZE * 8 + slot;
  if (idx >= d->nr_bits)
    return true;
  size_t byte = idx / 8;
  uint8_t bit = (uint8_t)(1u << (idx % 8));
  if (d->bits[byte] & bit)
    return false;
  d->bits[byte] |= bit;
  return true;
}

static void snap_compact_delta_into_base(struct snap_state *ss,
                                         struct shard *shard, uint64_t epoch) {
  char delta_path[512], base_path[512], tmp_path[512];
  snap_delta_filename(delta_path, sizeof(delta_path), ss->snap_dir, shard->id,
                      epoch);
  snap_base_filename(base_path, sizeof(base_path), ss->snap_dir, shard->id);
  snap_base_tmp_filename(tmp_path, sizeof(tmp_path), ss->snap_dir, shard->id);

  FILE *fp_delta = fopen(delta_path, "rb");
  if (!fp_delta) {
    fprintf(stderr, "[shard %u] compact: cannot open delta %s\n", shard->id,
            delta_path);
    return;
  }

  struct snap_file_header hdr_delta;
  if (fread(&hdr_delta, sizeof(hdr_delta), 1, fp_delta) != 1 ||
      hdr_delta.magic != SNAP_MAGIC || hdr_delta.version != SNAP_VERSION ||
      hdr_delta.shard_id != shard->id) {
    fprintf(stderr, "[shard %u] compact: delta header invalid\n", shard->id);
    fclose(fp_delta);
    return;
  }

  struct thread_heap *heap = shard->mem;
  uint32_t nr_pages = heap ? heap->nr_pages_committed : 0;
  if (nr_pages < 64)
    nr_pages = 64;

  compact_dedup_t dedup;
  if (!compact_dedup_init(&dedup, nr_pages * 2)) {
    fprintf(stderr, "[shard %u] compact: dedup alloc failed\n", shard->id);
    fclose(fp_delta);
    return;
  }

  snap_writer_t w;
  if (!snap_writer_open(&w, tmp_path)) {
    fprintf(stderr, "[shard %u] compact: cannot open %s: %s\n", shard->id,
            tmp_path, strerror(errno));
    compact_dedup_free(&dedup);
    fclose(fp_delta);
    return;
  }

  struct snap_file_header hdr_base_new = {
      .magic = SNAP_MAGIC,
      .version = SNAP_VERSION,
      .shard_id = shard->id,
      .num_page_records = 0,
      .session_id = ss->session_id,
      .epoch = epoch,
      .timestamp_ms = snap_time_ms(),
  };
  if (!snap_writer_put(&w, &hdr_base_new, sizeof(hdr_base_new)))
    goto fail_compact;

  uint32_t out_pages = 0, out_slots = 0;

  for (uint32_t p = 0; p < hdr_delta.num_page_records; p++) {
    struct snap_page_record pr;
    if (fread(&pr, sizeof(pr), 1, fp_delta) != 1)
      goto fail_compact;

    uint32_t written_for_page = 0;
    size_t pr_off = w.used;
    struct snap_page_record pr_out = pr;
    pr_out.num_objects = 0;
    if (!snap_writer_put(&w, &pr_out, sizeof(pr_out)))
      goto fail_compact;

    for (uint16_t si = 0; si < pr.num_objects; si++) {
      struct snap_object_entry ent;
      if (fread(&ent, sizeof(ent), 1, fp_delta) != 1)
        goto fail_compact;
      size_t payload = (size_t)ent.key_len + (size_t)ent.val_len;

      bool first_seen =
          compact_dedup_mark(&dedup, pr.lpage_idx, ent.slot_index);

      if (!first_seen || ent.deleted) {
        if (payload > 0)
          fseek(fp_delta, (long)payload, SEEK_CUR);
        continue;
      }

      if (!snap_writer_put(&w, &ent, sizeof(ent)))
        goto fail_compact;
      if (payload > 0) {
        char *ptr_buf = (char *)malloc(payload);
        if (!ptr_buf)
          goto fail_compact;
        if (fread(ptr_buf, 1, payload, fp_delta) != payload) {
          free(ptr_buf);
          goto fail_compact;
        }
        bool ok = snap_writer_put(&w, ptr_buf, payload);
        free(ptr_buf);
        if (!ok)
          goto fail_compact;
      }
      written_for_page++;
      out_slots++;
    }

    if (written_for_page > 0) {
      ((struct snap_page_record *)(w.base + pr_off))->num_objects =
          (uint16_t)written_for_page;
      out_pages++;
    } else {
      w.used = pr_off;
    }
  }
  fclose(fp_delta);
  fp_delta = NULL;

  FILE *fp_base = fopen(base_path, "rb");
  if (fp_base) {
    struct snap_file_header hdr_old;
    if (fread(&hdr_old, sizeof(hdr_old), 1, fp_base) == 1 &&
        hdr_old.magic == SNAP_MAGIC && hdr_old.version == SNAP_VERSION &&
        hdr_old.shard_id == shard->id) {
      for (uint32_t p = 0; p < hdr_old.num_page_records; p++) {
        struct snap_page_record pr;
        if (fread(&pr, sizeof(pr), 1, fp_base) != 1)
          break;
        uint32_t written_for_page = 0;
        size_t pr_off = w.used;
        struct snap_page_record pr_out = pr;
        pr_out.num_objects = 0;
        if (!snap_writer_put(&w, &pr_out, sizeof(pr_out))) {
          fclose(fp_base);
          goto fail_compact;
        }
        for (uint16_t si = 0; si < pr.num_objects; si++) {
          struct snap_object_entry ent;
          if (fread(&ent, sizeof(ent), 1, fp_base) != 1)
            goto base_done;
          size_t payload = (size_t)ent.key_len + (size_t)ent.val_len;
          bool first_seen =
              compact_dedup_mark(&dedup, pr.lpage_idx, ent.slot_index);
          if (!first_seen || ent.deleted) {
            if (payload > 0)
              fseek(fp_base, (long)payload, SEEK_CUR);
            continue;
          }
          if (!snap_writer_put(&w, &ent, sizeof(ent))) {
            fclose(fp_base);
            goto fail_compact;
          }
          if (payload > 0) {
            char *ptr_buf = (char *)malloc(payload);
            if (!ptr_buf) {
              fclose(fp_base);
              goto fail_compact;
            }
            if (fread(ptr_buf, 1, payload, fp_base) != payload) {
              free(ptr_buf);
              fclose(fp_base);
              goto fail_compact;
            }
            bool ok2 = snap_writer_put(&w, ptr_buf, payload);
            free(ptr_buf);
            if (!ok2) {
              fclose(fp_base);
              goto fail_compact;
            }
          }
          written_for_page++;
          out_slots++;
        }
        if (written_for_page > 0) {
          ((struct snap_page_record *)(w.base + pr_off))->num_objects =
              (uint16_t)written_for_page;
          out_pages++;
        } else {
          w.used = pr_off;
        }
      }
    }
  base_done:
    fclose(fp_base);
  }

  ((struct snap_file_header *)(w.base))->num_page_records = out_pages;
  struct snap_file_footer footer = {.checksum = 0};
  if (!snap_writer_put(&w, &footer, sizeof(footer)))
    goto fail_compact;

  if (!snap_writer_close(&w)) {
    fprintf(stderr, "[shard %u] compact: close tmp failed: %s\n", shard->id,
            strerror(errno));
    snap_writer_abort(&w, tmp_path);
    compact_dedup_free(&dedup);
    return;
  }

  if (rename(tmp_path, base_path) != 0) {
    fprintf(stderr, "[shard %u] compact: rename failed: %s\n", shard->id,
            strerror(errno));
    unlink(tmp_path);
  } else {
    unlink(delta_path);
    printf("[shard %u] compact epoch=%" PRIu64
           ": merged %u pages, %u slots -> %s\n",
           shard->id, epoch, out_pages, out_slots, base_path);
    fflush(stdout);
  }
  compact_dedup_free(&dedup);
  return;

fail_compact:
  if (fp_delta)
    fclose(fp_delta);
  snap_writer_abort(&w, tmp_path);
  compact_dedup_free(&dedup);
  fprintf(stderr, "[shard %u] compact: failed, delta preserved at %s\n",
          shard->id, delta_path);
}

static void snap_delete_session_files(const char *snap_dir, uint32_t shard_id,
                                      uint64_t session_id) {
  DIR *dir = opendir(snap_dir);
  if (!dir)
    return;
  char prefix[128];
  snprintf(prefix, sizeof(prefix), "session-%" PRIu64 "-shard%u-epoch-",
           session_id, shard_id);
  size_t prefix_len = strlen(prefix);
  struct dirent *de;
  while ((de = readdir(dir)) != NULL) {
    if (strncmp(de->d_name, prefix, prefix_len) == 0) {
      char path[512];
      snprintf(path, sizeof(path), "%s/%s", snap_dir, de->d_name);
      unlink(path);
    }
  }
  closedir(dir);
}

/* ── Tick ────────────────────────────────────────────────────────────── */

void snap_engine_tick(struct snap_state *ss, struct shard *shard) {
  if (ss->interval_ms == 0)
    return;

  uint64_t now = snap_time_ms();
  if (now - ss->last_snap_ms < (uint64_t)ss->interval_ms)
    return;

  bool expected = false;
  if (!atomic_compare_exchange_strong_explicit(&ss->is_snapshotting, &expected,
                                               true, memory_order_acq_rel,
                                               memory_order_acquire)) {
    return;
  }

  struct thread_heap *heap = shard->mem;
  for (uint32_t i = 0; i < NR_SMALL_POOLS; i++) {
    pool_snap_merge(&heap->pools[i], heap->pages);
  }
  pool_snap_merge(&heap->span_meta_pool, heap->pages);

  struct kv_obj *objs = heap->snap_large_freelist;
  heap->snap_large_freelist = NULL;
  while (objs) {
    struct kv_obj *next = objs->lru_next;
    uint32_t lp = ptr_lpage_idx((void *)objs);
    buddy_span_free(&heap->buddy, lp, heap->pages[lp].span_size);
    objs = next;
  }

  ss->current_epoch++;
  ss->last_snap_ms = now;

  uint64_t *tmp_dirty = heap->dirty_bits;
  heap->dirty_bits = heap->snap_dirty_bits;
  heap->snap_dirty_bits = tmp_dirty;

  uint8_t *tmp_slot = heap->slot_bits;
  heap->slot_bits = heap->snap_slot_bits;
  heap->snap_slot_bits = tmp_slot;

  heap->nr_pages_dirty = 0;

  uint64_t val = 1;
  if (write(ss->snap_eventfd, &val, sizeof(val)) < 0) {
    perror("[snapshot] eventfd signal failed");
    atomic_store_explicit(&ss->is_snapshotting, false, memory_order_release);
  }

  if (!ss->old_files_pending_delete)
    return;
  snap_delete_session_files(ss->snap_dir, shard->id, ss->old_session_id);
  ss->old_files_pending_delete = false;
}

bool snap_thread_spawn(struct shard *shard) {
  struct snap_state *ss = &shard->snap;
  if (ss->interval_ms == 0)
    return true;
  if (pthread_create(&ss->snap_thread, NULL, snap_background_worker, shard) !=
      0) {
    perror("[snapshot] pthread_create failed");
    return false;
  }
  return true;
}

/* ── Restore ─────────────────────────────────────────────────────────── */

#define MAX_EPOCH_FILES 4096

typedef struct {
  uint64_t epoch;
  char path[512];
} epoch_file_t;

static int epoch_file_cmp_desc(const void *a, const void *b) {
  const epoch_file_t *ea = (const epoch_file_t *)a;
  const epoch_file_t *eb = (const epoch_file_t *)b;
  if (eb->epoch > ea->epoch)
    return 1;
  if (eb->epoch < ea->epoch)
    return -1;
  return 0;
}

#define DEDUP_INIT_CAP (1u << 16)
typedef struct {
  uint64_t *keys;
  uint32_t cap;
  uint32_t used;
} dedup_set_t;

static bool dedup_init(dedup_set_t *d) {
  d->cap = DEDUP_INIT_CAP;
  d->used = 0;
  d->keys = (uint64_t *)malloc(d->cap * sizeof(uint64_t));
  if (!d->keys)
    return false;
  memset(d->keys, 0xFF, d->cap * sizeof(uint64_t));
  return true;
}

static void dedup_free(dedup_set_t *d) {
  free(d->keys);
  d->keys = NULL;
}

#define DEDUP_EMPTY UINT64_MAX

static bool dedup_insert(dedup_set_t *d, uint32_t lpage, uint16_t slot) {
  if (d->used * 4 >= d->cap * 3) {
    uint32_t new_cap = d->cap * 2;
    uint64_t *new_keys = (uint64_t *)malloc(new_cap * sizeof(uint64_t));
    if (!new_keys)
      return false;
    for (uint32_t i = 0; i < new_cap; i++)
      new_keys[i] = DEDUP_EMPTY;
    for (uint32_t i = 0; i < d->cap; i++) {
      if (d->keys[i] == DEDUP_EMPTY)
        continue;
      uint32_t h =
          (uint32_t)(d->keys[i] * 0x9e3779b97f4a7c15ULL >> 32) & (new_cap - 1);
      while (new_keys[h] != DEDUP_EMPTY)
        h = (h + 1) & (new_cap - 1);
      new_keys[h] = d->keys[i];
    }
    free(d->keys);
    d->keys = new_keys;
    d->cap = new_cap;
  }
  uint64_t key = ((uint64_t)lpage << 16) | slot;
  uint32_t h = (uint32_t)(key * 0x9e3779b97f4a7c15ULL >> 32) & (d->cap - 1);
  while (d->keys[h] != DEDUP_EMPTY) {
    if (d->keys[h] == key)
      return false;
    h = (h + 1) & (d->cap - 1);
  }
  d->keys[h] = key;
  d->used++;
  return true;
}

int snap_engine_restore(struct shard *shard, const char *snap_dir,
                        uint64_t *old_session_id_out) {
  *old_session_id_out = 0;
  if (!snap_dir || !snap_dir[0])
    return 0;

  uint64_t now = snap_time_ms();
  int total_keys = 0;

  dedup_set_t seen;
  if (!dedup_init(&seen))
    return 0;

  char base_path[512];
  snap_base_filename(base_path, sizeof(base_path), snap_dir, shard->id);
  FILE *fp_base = fopen(base_path, "rb");
  if (fp_base) {
    struct snap_file_header hdr;
    if (fread(&hdr, sizeof(hdr), 1, fp_base) == 1 && hdr.magic == SNAP_MAGIC &&
        hdr.version == SNAP_VERSION && hdr.shard_id == shard->id) {
      for (uint32_t p = 0; p < hdr.num_page_records; p++) {
        struct snap_page_record pr;
        if (fread(&pr, sizeof(pr), 1, fp_base) != 1)
          break;
        for (uint16_t si = 0; si < pr.num_objects; si++) {
          struct snap_object_entry ent;
          if (fread(&ent, sizeof(ent), 1, fp_base) != 1)
            goto base_done;
          size_t payload = (size_t)ent.key_len + (size_t)ent.val_len;
          bool is_new = dedup_insert(&seen, pr.lpage_idx, ent.slot_index);
          if (!is_new || ent.deleted) {
            if (!ent.deleted && payload > 0)
              fseek(fp_base, (long)payload, SEEK_CUR);
            continue;
          }
          if (ent.expire_ms != 0 && ent.expire_ms < now) {
            if (payload > 0)
              fseek(fp_base, (long)payload, SEEK_CUR);
            continue;
          }
          char *ptr_buf = (char *)malloc(payload);
          if (!ptr_buf) {
            fseek(fp_base, (long)payload, SEEK_CUR);
            continue;
          }
          if (fread(ptr_buf, 1, payload, fp_base) != payload) {
            free(ptr_buf);
            goto base_done;
          }
          shard_key_set(shard, ptr_buf, ent.key_len, ptr_buf + ent.key_len,
                        ent.val_len, ent.expire_ms, HT_PUT_NONE);
          total_keys++;
          free(ptr_buf);
        }
      }
    }
  base_done:
    fclose(fp_base);
  }

  DIR *dir_mem = opendir(snap_dir);
  uint32_t n_deltas = 0;
  if (dir_mem) {
    epoch_file_t *deltas =
        (epoch_file_t *)calloc(MAX_EPOCH_FILES, sizeof(epoch_file_t));
    struct dirent *de2;
    while ((de2 = readdir(dir_mem)) != NULL && n_deltas < MAX_EPOCH_FILES) {
      uint32_t shid = 0;
      uint64_t eid = 0;
      if (sscanf(de2->d_name, "shard%u-delta-%" SCNu64 ".fsnap", &shid, &eid) ==
          2) {
        if (shid == shard->id) {
          deltas[n_deltas].epoch = eid;
          snprintf(deltas[n_deltas].path, sizeof(deltas[n_deltas].path), "%s/%s",
                   snap_dir, de2->d_name);
          n_deltas++;
        }
      }
    }
    closedir(dir_mem);

    if (n_deltas > 0) {
      qsort(deltas, n_deltas, sizeof(epoch_file_t), epoch_file_cmp_desc);
      for (uint32_t fi = 0; fi < n_deltas; fi++) {
        FILE *fp = fopen(deltas[fi].path, "rb");
        if (!fp)
          continue;
        struct snap_file_header hdr;
        if (fread(&hdr, sizeof(hdr), 1, fp) == 1 && hdr.magic == SNAP_MAGIC &&
            hdr.version == SNAP_VERSION && hdr.shard_id == shard->id) {
          for (uint32_t p = 0; p < hdr.num_page_records; p++) {
            struct snap_page_record pr;
            if (fread(&pr, sizeof(pr), 1, fp) != 1)
              break;
            for (uint16_t si = 0; si < pr.num_objects; si++) {
              struct snap_object_entry ent;
              if (fread(&ent, sizeof(ent), 1, fp) != 1)
                goto delta_done;
              size_t payload = (size_t)ent.key_len + (size_t)ent.val_len;
              bool is_new = dedup_insert(&seen, pr.lpage_idx, ent.slot_index);
              if (!is_new || ent.deleted) {
                if (!ent.deleted && payload > 0)
                  fseek(fp, (long)payload, SEEK_CUR);
                continue;
              }
              if (ent.expire_ms != 0 && ent.expire_ms < now) {
                if (payload > 0)
                  fseek(fp, (long)payload, SEEK_CUR);
                continue;
              }
              char *ptr_buf = (char *)malloc(payload);
              if (!ptr_buf) {
                fseek(fp, (long)payload, SEEK_CUR);
                continue;
              }
              if (fread(ptr_buf, 1, payload, fp) != payload) {
                free(ptr_buf);
                goto delta_done;
              }
              shard_key_set(shard, ptr_buf, ent.key_len, ptr_buf + ent.key_len,
                            ent.val_len, ent.expire_ms, HT_PUT_NONE);
              total_keys++;
              free(ptr_buf);
            }
          }
        }
      delta_done:
        fclose(fp);
      }
    }
    free(deltas);
  }

  dedup_free(&seen);
  printf("[shard %u] restored %d keys (base + %u deltas)\n", shard->id,
         total_keys, n_deltas);
  return total_keys;
}
