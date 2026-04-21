#define _GNU_SOURCE
#include "mem_arena.h"
#include "mem_heap.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

/* Global singleton */
struct mem_global_arena g_mem_arena = {0};

/* ── Helper: Parse /proc/meminfo ─────────────────────────────────────── */

static size_t get_mem_available_bytes(void) {
  FILE *f = fopen("/proc/meminfo", "r");
  if (!f) {
    perror("fopen /proc/meminfo");
    return 0;
  }

  size_t mem_available = 0;
  char line[256];
  while (fgets(line, sizeof(line), f)) {
    if (sscanf(line, "MemAvailable: %zu kB", &mem_available) == 1) {
      mem_available *= 1024; /* Convert KB to bytes */
      break;
    }
  }
  fclose(f);

  if (mem_available == 0) {
    fprintf(stderr, "[mem_arena] WARNING: Could not parse MemAvailable, "
                    "using conservative default\n");
    /* Conservative fallback: 4GB */
    mem_available = 4ULL * 1024 * 1024 * 1024;
  }

  return mem_available;
}

/* ── mem_lib_init ────────────────────────────────────────────────────── */

bool mem_lib_init(uint32_t n_threads, size_t per_thread_hint) {
  if (n_threads == 0 || n_threads > 256) {
    fprintf(stderr, "[mem_arena] Invalid n_threads=%u (must be 1..256)\n",
            n_threads);
    return false;
  }

  /* 1. Determine per_thread_data_size with MemAvailable capping */
  size_t mem_available = get_mem_available_bytes();
  size_t max_per_thread = (size_t)((double)mem_available * 0.80 / n_threads);
  max_per_thread = prev_power_of_2(max_per_thread);

  size_t per_thread_data_size;
  if (per_thread_hint > 0) {
    per_thread_data_size = prev_power_of_2(per_thread_hint);
    if (per_thread_data_size > max_per_thread) {
      fprintf(stderr,
              "[mem_arena] WARNING: per_thread_hint=%zu exceeds "
              "MemAvailable cap, capping to %zu\n",
              per_thread_hint, max_per_thread);
      per_thread_data_size = max_per_thread;
    }
  } else {
    per_thread_data_size = max_per_thread;
  }

  /* Minimum 256MB per thread */
  if (per_thread_data_size < (256ULL * 1024 * 1024)) {
    per_thread_data_size = 256ULL * 1024 * 1024;
  }

  /* 2. Calculate metadata and arena sizes */
  /* Metadata: ~3.12% of data size (1/32) for page_info, buddy_nodes, bitmaps.
   * With page_info increased to 56/64 bytes, 1/64 is no longer enough. */
  size_t per_thread_meta_size = per_thread_data_size / 32;
  if (per_thread_meta_size < (16ULL * 1024 * 1024)) {
    per_thread_meta_size = 16ULL * 1024 * 1024; /* Minimum 16MB */
  }

  /* Linear arena: 128MB per thread for HT/OVF */
  size_t per_thread_arena_size = 128ULL * 1024 * 1024;

  /* 3. Reserve VA regions using mmap PROT_NONE */
  size_t total_data = per_thread_data_size * n_threads;
  size_t total_meta = per_thread_meta_size * n_threads;
  size_t total_arena = per_thread_arena_size * n_threads;

  /* Data region */
  void *data_ptr = mmap(NULL, total_data, PROT_NONE,
                        MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
  if (data_ptr == MAP_FAILED) {
    perror("[mem_arena] mmap data region");
    return false;
  }

  /* Metadata region (separate VA range) */
  void *meta_ptr = mmap(NULL, total_meta, PROT_NONE,
                        MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
  if (meta_ptr == MAP_FAILED) {
    perror("[mem_arena] mmap meta region");
    munmap(data_ptr, total_data);
    return false;
  }

  /* Linear arena region (separate VA range) */
  void *arena_ptr = mmap(NULL, total_arena, PROT_NONE,
                         MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
  if (arena_ptr == MAP_FAILED) {
    perror("[mem_arena] mmap arena region");
    munmap(data_ptr, total_data);
    munmap(meta_ptr, total_meta);
    return false;
  }

  /* 4. Initialize global arena structure */
  g_mem_arena.data_base = (uintptr_t)data_ptr;
  g_mem_arena.meta_base = (uintptr_t)meta_ptr;
  g_mem_arena.arena_base = (uintptr_t)arena_ptr;
  g_mem_arena.per_thread_data_size = per_thread_data_size;
  g_mem_arena.per_thread_meta_size = per_thread_meta_size;
  g_mem_arena.per_thread_arena_size = per_thread_arena_size;
  g_mem_arena.n_threads = n_threads;
  g_mem_arena.thread_id_shift = log2_u64(per_thread_data_size);

  /* 5. Allocate global thread_heaps array */
  size_t heaps_size = n_threads * sizeof(struct thread_heap);
  g_thread_heaps =
      (struct thread_heap *)mmap(NULL, heaps_size, PROT_READ | PROT_WRITE,
                                 MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (g_thread_heaps == MAP_FAILED) {
    perror("[mem_arena] mmap thread_heaps");
    munmap(data_ptr, total_data);
    munmap(meta_ptr, total_meta);
    munmap(arena_ptr, total_arena);
    return false;
  }
  memset(g_thread_heaps, 0, heaps_size);

  printf("[mem_arena] Initialized: %u threads, %zu MB/thread data, "
         "%zu MB/thread meta, %zu MB/thread arena\n",
         n_threads, per_thread_data_size / (1024 * 1024),
         per_thread_meta_size / (1024 * 1024),
         per_thread_arena_size / (1024 * 1024));
  printf("[mem_arena] VA regions: data=%p meta=%p arena=%p\n", data_ptr,
         meta_ptr, arena_ptr);

  return true;
}

/* ── mem_thread_init ─────────────────────────────────────────────────── */

bool mem_thread_init(uint32_t thread_id) {
  if (thread_id >= g_mem_arena.n_threads) {
    fprintf(stderr, "[mem_arena] Invalid thread_id=%u (max %u)\n", thread_id,
            g_mem_arena.n_threads - 1);
    return false;
  }

  if (!g_thread_heaps) {
    fprintf(stderr, "[mem_arena] g_thread_heaps not allocated\n");
    return false;
  }

  return thread_heap_init(&g_thread_heaps[thread_id], thread_id);
}

/* ── mem_thread_destroy ──────────────────────────────────────────────── */

void mem_thread_destroy(uint32_t thread_id) {
  if (thread_id >= g_mem_arena.n_threads) {
    fprintf(stderr, "[mem_arena] Invalid thread_id=%u (max %u)\n", thread_id,
            g_mem_arena.n_threads - 1);
    return;
  }

  if (g_thread_heaps) {
    thread_heap_destroy(&g_thread_heaps[thread_id]);
  }

  /* Decommit data pages using madvise */
  uintptr_t data_start = thread_data_base(thread_id);
  if (madvise((void *)data_start, g_mem_arena.per_thread_data_size,
              MADV_DONTNEED) < 0) {
    perror("[mem_arena] madvise MADV_DONTNEED data");
  }

  /* Decommit metadata pages */
  uintptr_t meta_start = thread_meta_base(thread_id);
  if (madvise((void *)meta_start, g_mem_arena.per_thread_meta_size,
              MADV_DONTNEED) < 0) {
    perror("[mem_arena] madvise MADV_DONTNEED meta");
  }

  /* Decommit arena pages */
  uintptr_t arena_start = thread_arena_base(thread_id);
  if (madvise((void *)arena_start, g_mem_arena.per_thread_arena_size,
              MADV_DONTNEED) < 0) {
    perror("[mem_arena] madvise MADV_DONTNEED arena");
  }

  printf("[mem_arena] Thread %u destroyed (pages decommitted)\n", thread_id);
}

/* ── mem_lib_destroy ─────────────────────────────────────────────────── */

void mem_lib_destroy(void) {
  if (g_mem_arena.data_base == 0) {
    return; /* Not initialized */
  }

  size_t total_data = g_mem_arena.per_thread_data_size * g_mem_arena.n_threads;
  size_t total_meta = g_mem_arena.per_thread_meta_size * g_mem_arena.n_threads;
  size_t total_arena =
      g_mem_arena.per_thread_arena_size * g_mem_arena.n_threads;

  /* Free thread heaps array */
  if (g_thread_heaps) {
    size_t heaps_size = g_mem_arena.n_threads * sizeof(struct thread_heap);
    munmap(g_thread_heaps, heaps_size);
    g_thread_heaps = NULL;
  }

  munmap((void *)g_mem_arena.data_base, total_data);
  munmap((void *)g_mem_arena.meta_base, total_meta);
  munmap((void *)g_mem_arena.arena_base, total_arena);

  memset(&g_mem_arena, 0, sizeof(g_mem_arena));
  printf("[mem_arena] Global arena destroyed\n");
}
