#include "lock_manager.h"
#include "../mem/mem_arena.h"
#include "../mem/mem_api.h"

#if defined(__SSE2__)
#include <emmintrin.h>
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── Hash helper ─────────────────────────────────────────────────────── */

static inline uint8_t lm_h2(uint64_t hash56) {
    return (uint8_t)(hash56 & 0x7Fu);
}

static inline uint32_t lm_group(uint64_t hash56) {
    return (uint32_t)((hash56 >> 7) & LOCK_TABLE_GROUP_IDX_MASK);
}

static inline uint32_t lm_group_base(uint32_t group) {
    return group * LOCK_TABLE_GROUP_SIZE;
}

static inline uint32_t lm_match_byte_mask(const uint8_t *ctrl, uint8_t byte) {
#if defined(__SSE2__)
    __m128i needle = _mm_set1_epi8((char)byte);
    __m128i hay = _mm_loadu_si128((const __m128i *)ctrl);
    return (uint32_t)_mm_movemask_epi8(_mm_cmpeq_epi8(hay, needle));
#else
    uint32_t mask = 0;
    for (uint32_t i = 0; i < LOCK_TABLE_GROUP_SIZE; i++) {
        if (ctrl[i] == byte)
            mask |= 1u << i;
    }
    return mask;
#endif
}

static inline uint32_t lm_find_slot(struct lock_manager *lm,
                                    struct kv_obj *obj_ptr,
                                    uint64_t hash56) {
    uint8_t h2 = lm_h2(hash56);
    uint32_t group = lm_group(hash56);

    for (uint32_t probe = 0; probe < LOCK_TABLE_NUM_GROUPS; probe++) {
        uint32_t base = lm_group_base(group);
        uint32_t full_mask = lm_match_byte_mask(&lm->ctrl[base], h2);

        while (full_mask) {
            uint32_t bit = (uint32_t)__builtin_ctz(full_mask);
            uint32_t slot = base + bit;
            if (lm->entries[slot].obj_ptr == obj_ptr)
                return slot;
            full_mask &= full_mask - 1;
        }

        if (lm_match_byte_mask(&lm->ctrl[base], LOCK_CTRL_EMPTY))
            return UINT32_MAX;

        group = (group + 1) & LOCK_TABLE_GROUP_IDX_MASK;
    }
    return UINT32_MAX;
}

static inline void lm_reset_ctrl_if_empty(struct lock_manager *lm) {
    if (lm->count != 0)
        return;
    memset(lm->ctrl, LOCK_CTRL_EMPTY,
           LOCK_TABLE_CAPACITY + LOCK_TABLE_GROUP_SIZE);
    lm->deleted_count = 0;
}

static struct lock_entry *lm_insert_at(struct lock_manager *lm, uint32_t slot,
                                       struct kv_obj *obj_ptr,
                                       uint64_t hash56,
                                       struct cmd_info *holder,
                                       uint8_t h2) {
    struct lock_entry *le = &lm->entries[slot];

    memset(le, 0, sizeof(*le));
    le->open_groups = (struct wait_group **)slab_obj_calloc(
        lm->pool, lm->num_shards, sizeof(struct wait_group *));
    if (!le->open_groups)
        return NULL;

    le->obj_ptr = obj_ptr;
    le->hash56 = hash56;
    le->holder = holder;
    le->num_shards = lm->num_shards;

    if (lm->ctrl[slot] == LOCK_CTRL_DELETED)
        lm->deleted_count--;
    lm->ctrl[slot] = h2;
    lm->count++;
    return le;
}

/* ── Init / Destroy ──────────────────────────────────────────────────── */

bool lock_manager_init(struct lock_manager *lm, uint32_t num_shards,
                       struct slab_allocator *pool) {
    memset(lm, 0, sizeof(*lm));
    lm->capacity = LOCK_TABLE_CAPACITY;
    lm->num_shards = num_shards;
    lm->pool = pool;

    uintptr_t base = thread_arena_base(mem_get_thread_id()) +
                     g_mem_arena.per_thread_arena_size -
                     ARENA_LINEAR_CTRL_RESERVE;
    if (!linear_arena_init_manual(&lm->arena, mem_get_thread_id(),
                                  (void *)base, ARENA_LINEAR_CTRL_RESERVE))
        return false;
    linear_arena_set_accounting(&lm->arena, false);

    lm->entries = (struct lock_entry *)linear_arena_alloc(
        &lm->arena, LOCK_TABLE_CAPACITY * sizeof(struct lock_entry),
        _Alignof(struct lock_entry));
    if (!lm->entries)
        return false;
    memset(lm->entries, 0, LOCK_TABLE_CAPACITY * sizeof(struct lock_entry));

    lm->ctrl = (uint8_t *)linear_arena_alloc(
        &lm->arena, LOCK_TABLE_CAPACITY + LOCK_TABLE_GROUP_SIZE,
        _Alignof(uint8_t));
    if (!lm->ctrl) {
        linear_arena_destroy(&lm->arena);
        memset(lm, 0, sizeof(*lm));
        return false;
    }
    memset(lm->ctrl, LOCK_CTRL_EMPTY,
           LOCK_TABLE_CAPACITY + LOCK_TABLE_GROUP_SIZE);

    return true;
}

void lock_manager_destroy(struct lock_manager *lm) {
    if (!lm || !lm->entries)
        return;

    /* Free any remaining open_groups arrays */
    for (uint32_t i = 0; i < lm->capacity; i++) {
        struct lock_entry *le = &lm->entries[i];
        if (lm->ctrl && lm->ctrl[i] < LOCK_CTRL_DELETED && le->open_groups)
            slab_obj_free(lm->pool, le->open_groups);
    }

    linear_arena_destroy(&lm->arena);
    lm->ctrl = NULL;
    lm->entries = NULL;
    lm->count = 0;
    lm->deleted_count = 0;
}

/* ── Lookup ──────────────────────────────────────────────────────────── */

struct lock_entry *lock_manager_lookup(struct lock_manager *lm,
                                       struct kv_obj *obj_ptr,
                                       uint64_t hash56) {
    if (!obj_ptr)
        return NULL;
    uint32_t slot = lm_find_slot(lm, obj_ptr, hash56);
    return slot == UINT32_MAX ? NULL : &lm->entries[slot];
}

/* ── Insert ──────────────────────────────────────────────────────────── */

struct lock_entry *lock_manager_insert(struct lock_manager *lm,
                                       struct kv_obj *obj_ptr,
                                       uint64_t hash56,
                                       struct cmd_info *holder) {
    if (lm->count >= LOCK_TABLE_MAX_OCCUPANCY) {
        fprintf(stderr, "[lock_manager] table full (%u live, %u deleted, %u cap)\n",
                lm->count, lm->deleted_count, lm->capacity);
        return NULL;
    }

    uint8_t h2 = lm_h2(hash56);
    uint32_t group = lm_group(hash56);
    uint32_t first_deleted = UINT32_MAX;

    for (uint32_t probe = 0; probe < LOCK_TABLE_NUM_GROUPS; probe++) {
        uint32_t base = lm_group_base(group);
        uint32_t full_mask = lm_match_byte_mask(&lm->ctrl[base], h2);

        while (full_mask) {
            uint32_t bit = (uint32_t)__builtin_ctz(full_mask);
            uint32_t slot = base + bit;
            if (lm->entries[slot].obj_ptr == obj_ptr)
                return &lm->entries[slot];
            full_mask &= full_mask - 1;
        }

        uint32_t deleted_mask =
            lm_match_byte_mask(&lm->ctrl[base], LOCK_CTRL_DELETED);
        if (first_deleted == UINT32_MAX && deleted_mask)
            first_deleted = base + (uint32_t)__builtin_ctz(deleted_mask);

        uint32_t empty_mask = lm_match_byte_mask(&lm->ctrl[base], LOCK_CTRL_EMPTY);
        if (empty_mask) {
            uint32_t slot = first_deleted != UINT32_MAX
                                ? first_deleted
                                : base + (uint32_t)__builtin_ctz(empty_mask);
            return lm_insert_at(lm, slot, obj_ptr, hash56, holder, h2);
        }

        group = (group + 1) & LOCK_TABLE_GROUP_IDX_MASK;
    }

    if (first_deleted != UINT32_MAX)
        return lm_insert_at(lm, first_deleted, obj_ptr, hash56, holder, h2);

    return NULL;
}

/* ── Update pointer ──────────────────────────────────────────────────── */

void lock_manager_update_ptr(struct lock_manager *lm,
                             struct kv_obj *old_ptr,
                             struct kv_obj *new_ptr,
                             uint64_t hash56) {
    struct lock_entry *le = lock_manager_lookup(lm, old_ptr, hash56);
    if (le)
        le->obj_ptr = new_ptr;
}

/* ── Remove (with backshift) ─────────────────────────────────────────── */

void lock_manager_remove(struct lock_manager *lm,
                         struct kv_obj *obj_ptr,
                         uint64_t hash56) {
    uint32_t found = lm_find_slot(lm, obj_ptr, hash56);
    if (found == UINT32_MAX)
        return;

    struct lock_entry *le = &lm->entries[found];
    if (le->open_groups) {
        slab_obj_free(lm->pool, le->open_groups);
    }

    memset(le, 0, sizeof(*le));
    lm->ctrl[found] = LOCK_CTRL_DELETED;
    lm->count--;
    lm->deleted_count++;
    lm_reset_ctrl_if_empty(lm);
}

/* ── Wait Queue: sorted insert of closed group ──────────────────────── */

static void wq_sorted_insert(struct lock_entry *le, struct wait_group *wg) {
    /* Optimization: check tail first (most common case — new arrival
     * has lower priority than existing groups) */
    if (!le->queue_head) {
        /* Queue empty */
        le->queue_head = wg;
        le->queue_tail = wg;
        wg->queue_prev = NULL;
        wg->queue_next = NULL;
        return;
    }

    if (txn_id_cmp(&wg->txn, &le->queue_tail->txn) >= 0) {
        /* Append to tail (wg has lower or equal priority) */
        wg->queue_prev = le->queue_tail;
        wg->queue_next = NULL;
        le->queue_tail->queue_next = wg;
        le->queue_tail = wg;
        return;
    }

    /* Walk from head to find insertion point */
    struct wait_group *cur = le->queue_head;
    while (cur && txn_id_cmp(&cur->txn, &wg->txn) < 0)
        cur = cur->queue_next;

    /* Insert before cur */
    wg->queue_next = cur;
    wg->queue_prev = cur ? cur->queue_prev : NULL;

    if (wg->queue_prev)
        wg->queue_prev->queue_next = wg;
    else
        le->queue_head = wg;

    if (cur)
        cur->queue_prev = wg;
}

static void wq_detach(struct lock_entry *le, struct wait_group *wg) {
    if (wg->queue_prev)
        wg->queue_prev->queue_next = wg->queue_next;
    else
        le->queue_head = wg->queue_next;

    if (wg->queue_next)
        wg->queue_next->queue_prev = wg->queue_prev;
    else
        le->queue_tail = wg->queue_prev;

    wg->queue_prev = NULL;
    wg->queue_next = NULL;
}

/* ── Wait Queue: add regular command ─────────────────────────────────── */

void lock_wq_add_regular(struct lock_manager *lm,
                         struct lock_entry *le,
                         struct cmd_info *cmd) {
    uint8_t sid = cmd->origin_shard;
    struct wait_group *og = le->open_groups[sid];

    if (!og) {
        /* Create new open group for this shard */
        og = wait_group_alloc(lm->pool);
        if (!og) return;  /* OOM — drop command */

        og->closed = false;
        og->origin_shard = sid;
        og->head = cmd;
        og->tail = cmd;
        og->count = 1;
        cmd->next = NULL;
        le->open_groups[sid] = og;
        return;
    }

    /* Append to existing open group */
    cmd->next = NULL;
    og->tail->next = cmd;
    og->tail = cmd;
    og->count++;
}

/* ── Wait Queue: add MSET command ────────────────────────────────────── */

void lock_wq_add_mset(struct lock_manager *lm,
                      struct lock_entry *le,
                      struct cmd_info *cmd) {
    uint8_t sid = cmd->origin_shard;
    struct wait_group *og = le->open_groups[sid];

    if (og) {
        /* Close existing open group: append MSET as anchor */
        cmd->next = NULL;
        og->tail->next = cmd;
        og->tail = cmd;
        og->count++;
        og->txn = cmd->txn;
        og->closed = true;
        le->open_groups[sid] = NULL;

        /* Insert closed group into sorted queue */
        wq_sorted_insert(le, og);
    } else {
        /* No open group — create new closed group with just this MSET */
        struct wait_group *wg = wait_group_alloc(lm->pool);
        if (!wg) return;  /* OOM */

        wg->closed = true;
        wg->origin_shard = sid;
        wg->txn = cmd->txn;
        wg->head = cmd;
        wg->tail = cmd;
        wg->count = 1;
        cmd->next = NULL;

        wq_sorted_insert(le, wg);
    }
}

/* ── Wait Queue: find cmd_info by txn_id ─────────────────────────────── */

struct cmd_info *lock_wq_find_txn(struct lock_entry *le,
                                  const struct txn_id *txn,
                                  struct wait_group **out_group) {
    *out_group = NULL;

    /* Search sorted (closed) queue */
    struct wait_group *wg = le->queue_head;
    while (wg) {
        /* Closed groups have a txn — quick check */
        if (txn_id_eq(&wg->txn, txn)) {
            /* Found the group — find the MSET cmd_info */
            struct cmd_info *ci = wg->head;
            while (ci) {
                if (ci->is_mset && txn_id_eq(&ci->txn, txn)) {
                    *out_group = wg;
                    return ci;
                }
                ci = ci->next;
            }
        }
        wg = wg->queue_next;
    }

    /* Search open groups (in case of same-shard duplicate before close) */
    for (uint32_t i = 0; i < le->num_shards; i++) {
        struct wait_group *og = le->open_groups[i];
        if (!og) continue;
        struct cmd_info *ci = og->head;
        while (ci) {
            if (ci->is_mset && txn_id_eq(&ci->txn, txn)) {
                *out_group = og;
                return ci;
            }
            ci = ci->next;
        }
    }

    return NULL;
}

/* ── Wait Queue: remove specific cmd_info from group ─────────────────── */

void lock_wq_remove_cmd(struct lock_manager *lm,
                        struct lock_entry *le,
                        struct wait_group *wg,
                        struct cmd_info *cmd) {
    /* Unlink cmd from group's linked list */
    if (wg->head == cmd) {
        wg->head = cmd->next;
        if (wg->tail == cmd)
            wg->tail = NULL;
    } else {
        struct cmd_info *prev = wg->head;
        while (prev && prev->next != cmd)
            prev = prev->next;
        if (prev) {
            prev->next = cmd->next;
            if (wg->tail == cmd)
                wg->tail = prev;
        }
    }
    cmd->next = NULL;
    wg->count--;

    /* Handle group state after removal */
    if (wg->count == 0) {
        /* Group empty — remove from wherever it is */
        if (wg->closed) {
            wq_detach(le, wg);
        } else {
            /* Open group — clear from open_groups array */
            if (wg->origin_shard < le->num_shards)
                le->open_groups[wg->origin_shard] = NULL;
        }
        wait_group_free(lm->pool, wg);
        return;
    }

    /* If we removed the MSET anchor (last element), group reopens */
    if (wg->closed && cmd->is_mset) {
        wq_detach(le, wg);
        wg->closed = false;
        memset(&wg->txn, 0, sizeof(wg->txn));

        /* Move back to open_groups for this shard */
        uint8_t sid = wg->origin_shard;
        if (sid < le->num_shards) {
            if (le->open_groups[sid] == NULL) {
                le->open_groups[sid] = wg;
            } else {
                /* Merge into existing open group:
                 * prepend wg's commands before the existing group's head */
                struct wait_group *existing = le->open_groups[sid];
                wg->tail->next = existing->head;
                existing->head = wg->head;
                existing->count += wg->count;
                /* Free the now-empty wg shell */
                wg->head = wg->tail = NULL;
                wg->count = 0;
                wait_group_free(lm->pool, wg);
            }
        }
    }
}
