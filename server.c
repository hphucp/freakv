#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "core/shard.h"
#include "mem/mem_arena.h"
#include "mem/mem_heap.h"
#include "mem/mem_api.h"
#include "net/reactor.h"
#include "core/snapshot.h"

#define DEFAULT_PORT   7379
#define DEFAULT_SHARDS 0     /* 0 = auto-detect CPU cores */

/* Parse size string: "500M", "2G", "1073741824" etc. Returns bytes. */
static uint64_t
server_size_parse(const char *s)
{
    char  *end;
    double val = strtod(s, &end);
    if (end == s) return 0;
    switch (*end) {
    case 'k': case 'K': val *= 1024;               break;
    case 'm': case 'M': val *= 1024 * 1024;         break;
    case 'g': case 'G': val *= 1024 * 1024 * 1024;  break;
    }
    return (uint64_t)val;
}

/* ── Global state for signal handler ─────────────────────────────────── */

static struct slab_allocator  *g_init_pool = NULL;  /* For initialization allocations */
static struct shard_engine    *g_engine   = NULL;
static struct reactor       **g_reactors = NULL;
static volatile sig_atomic_t g_running = 1;

static void
server_signal_handler(int sig)
{
    (void)sig;
    g_running = 0;
}

/* ── struct reactor thread ───────────────────────────────────────────────────── */

static void *
server_reactor_run_thread(void *arg)
{
    struct reactor *r = (struct reactor *)arg;

    /* BUG FIX: Worker thread must declare its TLS thread ID! */
    mem_set_thread_id(r->shard->id);

    /* Signals are blocked in this thread as it was spawned while
     * they were blocked in the main thread. This ensures the main
     * thread handles SIGINT/SIGTERM. */

    printf("[shard %u] thread started (port %u)\n",
           r->shard->id, r->port);

    net_reactor_run(r);   /* blocks until shard->running == false */

    printf("[shard %u] thread exiting\n", r->shard->id);
    return NULL;
}

/* ── main ─────────────────────────────────────────────────────────────── */

int
main(int argc, char **argv)
{
    uint16_t port       = DEFAULT_PORT;
    uint32_t num_shards = DEFAULT_SHARDS;
    uint64_t maxmemory  = 0;   /* 0 = unlimited */
    const char *snap_dir = "";
    uint32_t snap_interval = 0; /* milliseconds, 0 = disabled */

    for (int i = 1; i < argc; i++) {
        if      (!strcmp(argv[i], "--port")      && i+1 < argc) port       = (uint16_t)atoi(argv[++i]);
        else if (!strcmp(argv[i], "--shards")    && i+1 < argc) num_shards = (uint32_t)atoi(argv[++i]);
        else if (!strcmp(argv[i], "--maxmemory") && i+1 < argc) maxmemory  = server_size_parse(argv[++i]);
        else if (!strcmp(argv[i], "--snapshot-dir") && i+1 < argc) snap_dir = argv[++i];
        else if (!strcmp(argv[i], "--snapshot-interval") && i+1 < argc) snap_interval = (uint32_t)atoi(argv[++i]);
        else if (!strcmp(argv[i], "--help") || !strcmp(argv[i], "-h")) {
            printf("Usage: %s [--port <port>] [--shards <n>] [--maxmemory <size>]\n"
                   "       [--snapshot-dir <path>] [--snapshot-interval <milliseconds>]\n\n"
                   "  --port               TCP port to listen on  (default %d)\n"
                   "  --shards             number of shards/cores (default = nproc)\n"
                   "  --maxmemory          max total memory e.g. 500M, 2G, 0=unlimited\n"
                   "  --snapshot-dir       directory for snapshot files (enables persistence)\n"
                   "  --snapshot-interval  milliseconds between snapshots (default 0 = off)\n",
                   argv[0], DEFAULT_PORT);
            return 0;
        }
    }

    /* Set up signal handler */
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = server_signal_handler;
    sigaction(SIGINT,  &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    signal(SIGHUP, SIG_IGN);  /* Ignore SIGHUP (like Redis) */
    signal(SIGPIPE, SIG_IGN);

    /* Block SIGINT/SIGTERM so spawned threads don't catch them */
    sigset_t mask, oldmask;
    sigemptyset(&mask);
    sigaddset(&mask, SIGINT);
    sigaddset(&mask, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &mask, &oldmask);

    printf("Starting FreaKV server...\n");

    /* ── 0. Global init allocator ─────────────────────────────────────── */
    g_init_pool = slab_alloc_init();
    if (!g_init_pool) {
        fprintf(stderr, "[server] Failed to create global init allocator\n");
        return 1;
    }

    /* ── 1. Global virtual arena ──────────────────────────────────────── */
    uint32_t resolved_shards = num_shards ? num_shards : num_cpu_count();
    if (!mem_lib_init(resolved_shards, 0)) {
        fprintf(stderr, "[server] mem_lib_init failed\n");
        return 1;
    }
    printf("[server] memory arena initialised (%u shard(s))\n", resolved_shards);

    /* ── 2. Snapshot config (must be set before shard_engine_create) ──── */
    if (snap_dir[0] && snap_interval > 0) {
        snap_config_set(snap_dir, snap_interval);
        /* Generate session_id once here — all shards share the same value */
        uint64_t sid = snap_session_id_get();
        printf("[server] snapshots enabled: dir=%s interval=%ums session=%lu\n",
               snap_dir, snap_interval, (unsigned long)sid);
    }

    /* ── 3. struct shard engine ──────────────────────────────────────────────── */
    g_engine = shard_engine_create(g_init_pool, num_shards);
    if (!g_engine) {
        fprintf(stderr, "[server] shard_engine_create failed\n");
        return 1;
    }
    num_shards = g_engine->num_shards;
    printf("[server] %u shard(s) created\n", num_shards);

    if (maxmemory > 0) {
        uint64_t per_shard = maxmemory / num_shards;
        /* maxmemory is divided evenly across shards and enforced independently.
         * Each shard evicts from its own LRU when it reaches its per-shard quota.
         * Uneven key-size distribution may cause one shard to evict while others
         * still have headroom. A global memory policy across shards is not
         * currently implemented. */
        for (uint32_t i = 0; i < num_shards; i++) {
            mem_limit_set(i, per_shard);
        }
        printf("[server] maxmemory=%lu bytes total, %lu bytes/shard\n",
               (unsigned long)maxmemory, (unsigned long)per_shard);
    }

    /* ── 3. Restore from snapshot (if available) ─────────────────────── */
    if (snap_dir[0]) {
        int total_restored = 0;
        for (uint32_t i = 0; i < num_shards; i++) {
            mem_set_thread_id(i);
            uint64_t old_session_id = 0;
            total_restored += snap_engine_restore(&g_engine->shards[i],
                                                  snap_dir, &old_session_id);
            /* Defer deletion of old session files until the first epoch of
             * the new session has been successfully committed. */
            if (old_session_id != 0) {
                g_engine->shards[i].snap.old_files_pending_delete = true;
                g_engine->shards[i].snap.old_session_id           = old_session_id;
            }
        }
        if (total_restored > 0)
            printf("[server] restored %d keys from snapshots\n", total_restored);
    }

    /* ── 4. Reactors ──────────────────────────────────────────────────── */
    g_reactors = (struct reactor **)slab_obj_calloc(g_init_pool, num_shards, sizeof(struct reactor *));
    if (!g_reactors) {
        fprintf(stderr, "[server] OOM allocating reactor array\n");
        shard_engine_destroy(g_engine);
        return 1;
    }

    for (uint32_t i = 0; i < num_shards; i++) {
        g_reactors[i] = (struct reactor *)slab_obj_alloc(g_init_pool, sizeof(struct reactor));
        if (!g_reactors[i]) {
            fprintf(stderr, "[server] OOM allocating reactor %u\n", i);
            return 1;
        }
        if (net_reactor_init(g_reactors[i], g_engine, &g_engine->shards[i],
                         port, (uint16_t)(port + i + 1), NULL) < 0) {
            fprintf(stderr, "[server] net_reactor_init failed for shard %u\n", i);
            return 1;
        }
    }


    /* ── 3. Start threads ─────────────────────────────────────────────── */
    g_engine->running = true;
    for (uint32_t i = 0; i < num_shards; i++) {
        g_engine->shards[i].running = true;
        if (pthread_create(&g_engine->shards[i].thread, NULL,
                           server_reactor_run_thread, g_reactors[i]) != 0) {
            perror("pthread_create");
            return 1;
        }
    }

    /* Restore signal mask in main thread so it can be interrupted */
    pthread_sigmask(SIG_SETMASK, &oldmask, NULL);

    printf("\nValkyd listening on ports %u..%u with %u shard(s). "
           "Press Ctrl-C to stop.\n\n",
           (unsigned)port, (unsigned)(port + num_shards), num_shards);

    /* ── 4. Main thread: wait for shutdown ────────────────────────────── */
    while (g_running)
        usleep(10000);

    printf("\n[server] shutdown initiated...\n");
    if (g_engine)
        shard_engine_stop(g_engine);

    /*
     * pthread_join and shard_engine_destroy are intentionally skipped here.
     * Destroying the hash table requires iterating every key to call free(),
     * which takes minutes on a database with millions of entries.
     * The OS reclaims the entire memory arena (munmap) instantly when the
     * process exits, making explicit teardown unnecessary.
     */
    
    printf("[server] closing network reactors...\n");
    for (uint32_t i = 0; i < num_shards; i++) {
        net_reactor_destroy(g_reactors[i]);
        slab_obj_free(g_init_pool, g_reactors[i]);
    }
    slab_obj_free(g_init_pool, g_reactors);

    shard_engine_stats_print(g_engine);
    
    /* shard_engine_destroy(g_engine) is intentionally not called — see comment above. */

    printf("[server] stopped cleanly (Resources reclaimed by OS).\n");
    /* TODO: trigger a final snap_engine_tick on each shard before exit so that
     * writes since the last snapshot interval are not lost on graceful shutdown.
     * Requires: joining shard threads after stop signal, ensuring SPSC queues
     * are fully drained, then calling snap_engine_tick once per shard.
     * Currently skipped: the OS reclaims mmap arenas instantly on exit, avoiding
     * the cost of iterating millions of kv_obj entries in shard_engine_destroy. */
    exit(0);
}


