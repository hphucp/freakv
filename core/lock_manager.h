#ifndef LOCK_MANAGER_H
#define LOCK_MANAGER_H

/*
 * lock_manager.h — Key-level lock manager for async MSET 2PC.
 *
 * SwissTable-style hashtable mapping kv_obj* -> lock_entry.
 * Capacity: power-of-2, allocated from slab_allocator.
 *
 * Integration: one bit in main HT bucket meta (HT_META_LOCK_BIT)
 * gates whether lock manager is consulted. bit=0 → fast path skip.
 *
 * All operations are single-threaded (called from shard's own thread).
 */

#include "txn.h"
#include "../memory/slab.h"

#include <stdbool.h>
#include <stdint.h>
#include <string.h>

/* ── Lock bit in main HT meta word ───────────────────────────────────── */

/*
 * Meta word layout (from htable.h):
 *   bits[63:8] = hash56
 *   bits[ 7:0] = val_type tag (0-4 used)
 *
 * We use bit 7 for the lock flag. val_type uses bits 0-2 (max value 4).
 * Bits 3-7 are unused, so bit 7 is safe.
 */
#define HT_META_LOCK_BIT (1ULL << 7)

static inline void ht_meta_lock_set(uint64_t *meta) {
    *meta |= HT_META_LOCK_BIT;
}

static inline void ht_meta_lock_clear(uint64_t *meta) {
    *meta &= ~HT_META_LOCK_BIT;
}

static inline bool ht_meta_is_locked(uint64_t meta) {
    return (meta & HT_META_LOCK_BIT) != 0;
}

/* ── Lock Manager ────────────────────────────────────────────────────── */

#define LOCK_TABLE_ORDER      16
#define LOCK_TABLE_CAPACITY   (1u << LOCK_TABLE_ORDER)
#define LOCK_TABLE_MASK       (LOCK_TABLE_CAPACITY - 1)
#define LOCK_TABLE_GROUP_SIZE 16
#define LOCK_TABLE_GROUP_MASK (LOCK_TABLE_GROUP_SIZE - 1)
#define LOCK_TABLE_NUM_GROUPS (LOCK_TABLE_CAPACITY / LOCK_TABLE_GROUP_SIZE)
#define LOCK_TABLE_GROUP_IDX_MASK (LOCK_TABLE_NUM_GROUPS - 1)
#define LOCK_TABLE_MAX_OCCUPANCY (LOCK_TABLE_CAPACITY * 3 / 4)
#define LOCK_TABLE_MAX_USED_CTRL (LOCK_TABLE_CAPACITY * 7 / 8)

#define LOCK_CTRL_EMPTY   0xFFu
#define LOCK_CTRL_DELETED 0x80u

struct lock_manager {
    struct lock_entry *entries;   /* array[LOCK_TABLE_CAPACITY]            */
    uint8_t *ctrl;                /* array[LOCK_TABLE_CAPACITY + 16]       */
    uint32_t count;               /* occupied entries                      */
    uint32_t deleted_count;       /* tombstones in ctrl array              */
    uint32_t capacity;            /* always LOCK_TABLE_CAPACITY            */
    uint32_t num_shards;          /* for open_groups sizing                */
    struct slab_allocator *pool;  /* control-plane allocator               */
};

/* ── API ─────────────────────────────────────────────────────────────── */

bool lock_manager_init(struct lock_manager *lm, uint32_t num_shards,
                       struct slab_allocator *pool);
void lock_manager_destroy(struct lock_manager *lm);

/*
 * Insert a new lock entry. The key must NOT already be in the table.
 * Sets up holder, empty wait queue, and allocates open_groups array.
 * Returns the newly created lock_entry, or NULL on failure.
 */
struct lock_entry *lock_manager_insert(struct lock_manager *lm,
                                       struct kv_obj *obj_ptr,
                                       uint64_t hash56,
                                       struct cmd_info *holder);

/*
 * Lookup by obj_ptr. Returns lock_entry or NULL.
 * Uses hash56 for initial probe, then linear scan comparing obj_ptr.
 */
struct lock_entry *lock_manager_lookup(struct lock_manager *lm,
                                       struct kv_obj *obj_ptr,
                                       uint64_t hash56);

/*
 * Update obj_ptr in an existing entry (called during FIN swap).
 * Finds by old_ptr, replaces with new_ptr.
 * Must rehash if the new pointer maps to a different slot — but since
 * we hash by hash56 (which doesn't change), only the stored obj_ptr
 * changes. No rehash needed.
 */
void lock_manager_update_ptr(struct lock_manager *lm,
                             struct kv_obj *old_ptr,
                             struct kv_obj *new_ptr,
                             uint64_t hash56);

/*
 * Remove lock entry and free its open_groups array.
 * Uses backshift deletion for linear probing correctness.
 */
void lock_manager_remove(struct lock_manager *lm,
                         struct kv_obj *obj_ptr,
                         uint64_t hash56);

/* ── Wait Queue Operations ───────────────────────────────────────────── */

/*
 * Add a regular (non-MSET) command to the wait queue of a locked key.
 * Appends to the open_group for cmd's origin_shard, creating one if needed.
 */
void lock_wq_add_regular(struct lock_manager *lm,
                         struct lock_entry *le,
                         struct cmd_info *cmd);

/*
 * Add an MSET command to the wait queue.
 * Closes the open_group for cmd's origin_shard (if any) and inserts
 * the closed group into the sorted queue.
 */
void lock_wq_add_mset(struct lock_manager *lm,
                      struct lock_entry *le,
                      struct cmd_info *cmd);

/*
 * Find a cmd_info matching txn_id in the wait queue.
 * Used for duplicate detection within same MSET.
 * Returns the cmd_info and sets *out_group to its containing group,
 * or returns NULL if not found.
 */
struct cmd_info *lock_wq_find_txn(struct lock_entry *le,
                                  const struct txn_id *txn,
                                  struct wait_group **out_group);

/*
 * Remove a specific cmd_info from its wait_group.
 * If the group becomes empty, removes and frees it.
 * If the removed cmd was the MSET anchor, the group reopens
 * (or is destroyed if empty).
 */
void lock_wq_remove_cmd(struct lock_manager *lm,
                        struct lock_entry *le,
                        struct wait_group *wg,
                        struct cmd_info *cmd);

#endif /* LOCK_MANAGER_H */
