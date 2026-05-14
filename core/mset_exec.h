#ifndef MSET_EXEC_H
#define MSET_EXEC_H

/*
 * mset_exec.h — Async MSET 2PC execution engine.
 *
 * Public entry points (called by event loop / reactor only):
 *   mset_coordinator_dispatch  — parse & dispatch MSET from reactor
 *   mset_on_prepare            — handle MSG_CMD_MSET_KEY on exec shard
 *   mset_on_fin                — handle MSG_CMD_MSET_FIN on exec shard
 *   mset_on_ack                — handle MSG_CMD_MSET_ACK on coordinator
 *   mset_on_fin_ack            — handle MSG_CMD_MSET_FIN_ACK on coordinator
 *   mset_check_lock_defer      — check & defer regular commands behind locks
 *
 * Internal helpers (not exported):
 *   mset_prepare_key, mset_run_fin, mset_fin_one_cmd, mset_drain_waitqueue
 */

#include "lock_manager.h"
#include "txn.h"

#include <stdbool.h>
#include <stdint.h>

/* Forward declarations */
struct shard;
struct shard_engine;
struct reactor;
struct net_conn;
struct resp_parser;

/* ── Coordinator: dispatch ───────────────────────────────────────────── */

/*
 * Dispatch MSET from coordinator shard.
 * Parses key-value pairs, allocates mset_stat, sends PREPARE per key.
 * Returns 0 on success (pipeline slot is PSLOT_PENDING).
 */
int mset_coordinator_dispatch(struct reactor *r, struct net_conn *c,
                              struct resp_parser *p, uint32_t pipeline_seq);

/* ── Coordinator: incoming messages ─────────────────────────────────── */

/*
 * Handle ACK message on coordinator shard.
 * Sends FIN to participant shards; calls mset_run_fin for local shard.
 * Does NOT reply to client (Option B: reply deferred to fin_ack).
 */
void mset_on_ack(struct shard_engine *engine, struct shard *shard,
                 struct spsc_message *msg);

/*
 * Handle FIN_ACK message on coordinator shard (remote coordinator path).
 * Sets pipeline reply to +OK and frees mset_stat.
 * Only called when coordinator is on a different shard than the last FIN.
 */
void mset_on_fin_ack(struct shard_engine *engine, struct shard *shard,
                     struct spsc_message *msg);

/* ── Exec-shard entry points ─────────────────────────────────────────── */

/*
 * Handle PREPARE message on exec shard (or local call on coordinator).
 * Checks lock, performs wound-wait, updates lock manager.
 * If MSET is locally ready, drives FIN immediately via mset_run_fin.
 */
void mset_on_prepare(struct shard_engine *engine, struct shard *shard,
                     struct spsc_message *msg, uint64_t cur_ms);

/*
 * Handle MSG_CMD_MSET_KEY_BATCH on participant shard.
 * Iterates batch->entries[] and calls mset_on_prepare per key.
 * Does NOT free the batch — coordinator owns it, freed in stat_free_real.
 */
void mset_on_prepare_batch(struct shard_engine *engine, struct shard *shard,
                            struct spsc_message *msg, uint64_t cur_ms);

/*
 * Handle FIN message on exec shard.
 * Commits new_obj into HT, releases lock, drains wait queue.
 */
void mset_on_fin(struct shard_engine *engine, struct shard *shard,
                 struct spsc_message *msg, uint64_t cur_ms);

/* ── Regular command lock check ──────────────────────────────────────── */

/*
 * Check if a key is locked before executing a regular command.
 * If locked, defers the command into the wait queue and returns true.
 * If not locked, returns false (caller should execute normally).
 */
bool mset_check_lock_defer(struct shard *shard, struct spsc_message *msg,
                           const void *key, size_t klen);

#endif /* MSET_EXEC_H */
