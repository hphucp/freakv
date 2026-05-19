#ifndef TXN_H
#define TXN_H

/*
 * txn.h — Async MSET 2PC data structures.
 *
 * Provides: txn_id (transaction identity), mset_stat (shared ack counter),
 * cmd_info (per-key pending operation), wait_group (batched waiters),
 * lock_entry (per-key lock state).
 *
 * All structures except mset_stat are shard-local (single-threaded access).
 * mset_stat.ack is the only cross-shard atomic field.
 */

#include "message.h"
#include "object.h"
#include "../memory/slab.h"
#include "../net/conn.h"

#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

/* ── Transaction Identity ────────────────────────────────────────────── */

/*
 * Globally unique transaction identifier for wound-wait ordering.
 * Comparison: lower timestamp_ns = higher priority (older wins).
 * Tiebreaks: shard_id, then pipeline_idx.
 */
struct txn_id {
    uint64_t timestamp_ns;   /* CLOCK_MONOTONIC nanoseconds              */
    void *conn_ptr;          /* tie-break: coordinator connection ptr    */
    uint32_t pipeline_idx;   /* pipeline slot of MSET in coordinator     */
};

static inline int txn_id_cmp(const struct txn_id *a, const struct txn_id *b) {
    if (a->timestamp_ns != b->timestamp_ns)
        return (a->timestamp_ns < b->timestamp_ns) ? -1 : 1;
    if (a->pipeline_idx != b->pipeline_idx)
        return (a->pipeline_idx < b->pipeline_idx) ? -1 : 1;
    if (a->conn_ptr != b->conn_ptr)
        return (a->conn_ptr < b->conn_ptr) ? -1 : 1;
    return 0;
}

static inline bool txn_id_eq(const struct txn_id *a, const struct txn_id *b) {
    return a->timestamp_ns == b->timestamp_ns &&
           a->conn_ptr    == b->conn_ptr &&
           a->pipeline_idx == b->pipeline_idx;
}

static inline uint64_t txn_now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

/* ── MSET Stat (Coordinator-owned shared counter) ────────────────────── */
#define MAX_SHARDS 64


/*
 * Allocated by coordinator, pointer sent in PREPARE messages.
 * Exec shards do atomic_fetch_add on ack, then compare with total.
 * The last shard to reach ack == total sends ACK to coordinator.
 *
 * fin_ack counts individual keys (same unit as ack/total).
 * When fin_ack == total, all keys are committed across all shards.
 *
 * Lifetime: freed by coordinator shard after fin_ack == total.
 *   - If coordinator is local: freed inside mset_run_fin.
 *   - If coordinator is remote: freed inside mset_on_fin_ack.
 */
struct mset_stat {
    _Atomic uint32_t ack;     /* keys acknowledged (PREPARE done)        */
    _Atomic uint32_t fin_ack; /* keys committed (FIN done) — per key     */
    _Atomic uint32_t refcount; /* safety counter for distributed cleanup  */
    uint32_t total;           /* total keys in this MSET                 */
    struct txn_id txn;        /* transaction identity                    */
    void *conn_ptr;           /* coordinator's net_conn for reply        */
    uint64_t conn_generation; /* stale connection detection              */
    uint32_t pipeline_idx;    /* pipeline slot for reply routing         */
    uint8_t coordinator_id;   /* shard that owns this struct             */
    struct net_buf *coord_rbuf_nb; /* input buffer ref held by coordinator */
    struct cmd_info *shard_heads[MAX_SHARDS]; /* per-shard cmd lists      */
    struct mset_batch *batch_cleanup_head;    /* linked list of batch allocs to free */
};

/* ── Command Info (Per-key pending operation) ────────────────────────── */

/*
 * Represents one pending key operation inside the lock manager.
 * Lives either as lock holder (lock_entry.holder) or in a wait_group.
 *
 * For MSET: new_obj allocated but NOT in main HT until FIN.
 * For regular commands: deferred spsc_message for later execution.
 *
 * Lifetime: freed after execution on drain, or on cancel (self-preempt).
 */
struct cmd_info {
    struct cmd_info *next;        /* intrusive list within wait_group      */
    struct cmd_info *next_in_mset; /* intrusive list within mset_stat       */

    bool is_mset;                 /* true = MSET PREPARE, false = regular  */

    /* ── MSET fields (is_mset == true) ─────────────────────────────── */
    struct txn_id txn;            /* transaction identity                  */
    struct mset_stat *stat;       /* coordinator's state tracker             */
    struct lock_entry *le;        /* cached lock entry for fast FIN          */
    struct kv_obj *new_obj;       /* tentative object to be installed        */
    struct kv_obj *old_obj;       /* current object in main HT at PREPARE  */
    uint32_t sub_idx;             /* index within multi-key command        */
    uint32_t total_acks;          /* raw pair count this cmd represents (≥1) */

    /* ── Regular command fields (is_mset == false) ─────────────────── */
    struct spsc_message msg;      /* original message for deferred exec    */

    /* ── Common ────────────────────────────────────────────────────── */
    uint64_t hash56;              /* precomputed key hash for fast lookup  */
    uint8_t origin_shard;         /* shard that sent this command           */
    struct net_buf *req_nb;       /* refcounted input buffer               */
};

/* ── Wait Group ──────────────────────────────────────────────────────── */

/*
 * Batch of commands from one shard waiting on a locked key.
 * Maintains pipeline order (FIFO).
 *
 * OPEN:   only regular commands, stored in lock_entry.open_groups[shard_id].
 * CLOSED: has MSET anchor at tail, inserted into sorted wait queue.
 */
struct wait_group {
    struct cmd_info *head;        /* first command (FIFO)                  */
    struct cmd_info *tail;        /* last command — O(1) append            */
    uint32_t count;               /* commands in group                     */

    struct txn_id txn;            /* from MSET anchor (valid when closed)  */
    bool closed;                  /* true if MSET anchor present           */
    uint8_t origin_shard;         /* all commands from this shard          */

    /* Intrusive sorted doubly-linked list (closed groups in wait queue) */
    struct wait_group *queue_next;
    struct wait_group *queue_prev;
};

/* ── Lock Entry (Per-key lock state in lock manager) ─────────────────── */

/*
 * One entry per locked key in the lock manager hashtable.
 *
 * Key identity: obj_ptr (kv_obj* in main HT).
 * Updated on FIN when new_obj swapped in.
 *
 * Wait queue: sorted doubly-linked list of closed wait_groups
 * (head = highest priority). Plus per-shard open_groups array.
 */
struct lock_entry {
    struct kv_obj *obj_ptr;       /* current kv_obj* in main HT            */
    uint64_t hash56;              /* reused from main HT meta >> 8         */

    struct cmd_info *holder;      /* MSET cmd_info owning the lock         */

    /* Sorted queue of closed wait groups (head = highest priority) */
    struct wait_group *queue_head;
    struct wait_group *queue_tail;

    /* Per-shard open wait groups: open_groups[shard_id] or NULL.
     * Array of num_shards pointers, allocated with lock_entry. */
    struct wait_group **open_groups;
    uint32_t num_shards;          /* size of open_groups array             */
};

/* ── Helpers for cmd_info / wait_group allocation ────────────────────── */

static inline struct cmd_info *cmd_info_alloc(struct slab_allocator *pool) {
    struct cmd_info *ci = (struct cmd_info *)slab_obj_alloc(pool, sizeof(struct cmd_info));
    if (ci) memset(ci, 0, sizeof(*ci));
    return ci;
}

static inline void cmd_info_free(struct slab_allocator *pool, struct cmd_info *ci) {
    if (!ci) return;
    if (ci->req_nb) {
        net_buf_unref(pool, ci->req_nb);
        ci->req_nb = NULL;
    }
    slab_obj_free(pool, ci);
}

static inline struct wait_group *wait_group_alloc(struct slab_allocator *pool) {
    struct wait_group *wg = (struct wait_group *)slab_obj_alloc(pool, sizeof(struct wait_group));
    if (wg) memset(wg, 0, sizeof(*wg));
    return wg;
}

static inline void wait_group_free(struct slab_allocator *pool, struct wait_group *wg) {
    if (wg) slab_obj_free(pool, wg);
}

#endif /* TXN_H */
