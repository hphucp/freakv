#ifndef OBJECT_H
#define OBJECT_H

#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* ── Value type tag ───────────────────────────────────────────────────── */

enum val_type {
  VAL_TYPE_STRING = 0,
  VAL_TYPE_LIST = 1,
  VAL_TYPE_HASH = 2,
  VAL_TYPE_SET = 3,
  VAL_TYPE_ZSET = 4,
};

/* ── Encoding upgrade thresholds ─────────────────────────────────────── */

#define LIST_LISTPACK_MAX_ENTRIES 128
#define LIST_LISTPACK_MAX_ELEM_LEN 64
#define HASH_LISTPACK_MAX_ENTRIES 128
#define HASH_LISTPACK_MAX_ELEM_LEN 64
#define SET_INTSET_MAX_ENTRIES 512
#define ZSET_LISTPACK_MAX_ENTRIES 128
#define ZSET_LISTPACK_MAX_ELEM_LEN 64

#define HEAP_IDX_NONE 0xFFFFFFFF

/* ── Snapshot flag values (1-byte atomic state machine) ──────────────── */

#define SNAP_FLAG_LIVE_FRESH 0   /* Object live, snap thread hasn't visited   */
#define SNAP_FLAG_DEAD_SAFE 1    /* Object dead, safe for snap to tombstone   */
#define SNAP_FLAG_DEAD_PENDING 2 /* Object died mid-snap, snap must save LIVE  \
                                  */
#define SNAP_FLAG_LIVE_VISITED 3 /* Snap thread visited and serialized LIVE */

/* ── kv_obj ────────────────────────────────────────────────────────────── */

struct kv_obj {
  uint32_t heap_idx;         /*  0: TTL heap index (HEAP_IDX_NONE = none)    */
  uint32_t pool_idx;         /*  4: pool class index (NR_SMALL_POOLS = big)  */
  struct kv_obj *lru_prev;   /*  8: previous in pool-class LRU (more recent) */
  struct kv_obj *lru_next;   /* 16: next in pool-class LRU (less recent)     */
  atomic_uint refcount;      /* 24: reference count for zero-copy safety     */
  uint32_t val_len;          /* 28: value length; 0 = complex obj pointer    */
  uint64_t expire_ms;        /* 32: embedded absolute expiry time for snap   */
  uint16_t key_len;          /* 40: key length in bytes                      */
  uint8_t marked_deletion;   /* 42: 1 if logically deleted (tombstone)    */
  _Atomic uint8_t snap_flag; /* 43: snapshot state (see SNAP_FLAG_*)     */
  uint8_t _pad8[4];          /* 44: keep 8-byte alignment                    */
  char data[];               /* 48: [key bytes][value bytes or pointer]      */
};

_Static_assert(sizeof(struct kv_obj) == 48, "kv_obj header must be 48 bytes");
_Static_assert(offsetof(struct kv_obj, data) == 48, "");

/* ── Accessors ────────────────────────────────────────────────────────── */

static inline const char *obj_key_get(const struct kv_obj *o) {
  return o->data;
}
static inline const char *obj_val_get(const struct kv_obj *o) {
  return o->data + o->key_len;
}
static inline uint32_t obj_val_len_get(const struct kv_obj *o) {
  return o->val_len;
}

static inline void *obj_val_ptr_get(const struct kv_obj *o) {
  void *p;
  memcpy(&p, o->data + o->key_len, sizeof(void *));
  return p;
}

static inline void obj_val_ptr_set(struct kv_obj *o, void *ptr) {
  memcpy(o->data + o->key_len, &ptr, sizeof(void *));
}

#endif /* OBJECT_H */
