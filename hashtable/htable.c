#include "htable.h"
#include "../core/lock_manager.h"
#include "../mem/mem_api.h"
#include "hash.h"

#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

static inline size_t ht_pow2_next(size_t n) {
  if (n == 0)
    return 1;
  n--;
  n |= n >> 1;
  n |= n >> 2;
  n |= n >> 4;
  n |= n >> 8;
  n |= n >> 16;
  n |= n >> 32;
  return n + 1;
}

/* ── Overflow pool ────────────────────────────────────────────────────── */

static struct bucket *ht_ovf_bucket_alloc(struct hash_table *ht) {
  if (!ht->ovf_free) {
    uint32_t old = ht->ovf_pool_cap;
    size_t maxcap = ht->ovf_arena.reserved / sizeof(struct bucket);
    if ((size_t)old >= maxcap)
      return NULL;

    size_t newcap_sz = old ? (size_t)old * 2 : 16;
    if (newcap_sz > maxcap)
      newcap_sz = maxcap;
    if (newcap_sz > UINT32_MAX)
      return NULL;
    uint32_t newcap = (uint32_t)newcap_sz;

    size_t new_total_bytes = (size_t)newcap * sizeof(struct bucket);
    if (!linear_arena_grow(&ht->ovf_arena, new_total_bytes))
      return NULL;
    ht->ovf_pool = (struct bucket *)linear_arena_base(&ht->ovf_arena);
    struct bucket *p = ht->ovf_pool;
    for (uint32_t i = old; i < newcap; i++) {
      p[i].meta = p[i].expire = 0;
      p[i].obj = NULL;
      p[i].next = ht->ovf_free;
      ht->ovf_free = &p[i];
    }
    ht->ovf_pool_cap = newcap;
  }
  struct bucket *b = ht->ovf_free;
  ht->ovf_free = b->next;
  b->meta = b->expire = 0;
  b->obj = NULL;
  b->next = NULL;
  return b;
}

static inline void ht_ovf_bucket_release(struct hash_table *ht,
                                         struct bucket *b) {
  b->meta = b->expire = 0;
  b->obj = NULL;
  b->next = ht->ovf_free;
  ht->ovf_free = b;
}

static void ht_ovf_pool_init(struct hash_table *ht) {
  struct bucket *p = ht->ovf_pool;
  uint32_t n = ht->ovf_pool_cap;
  for (uint32_t i = 0; i < n; i++) {
    p[i].meta = p[i].expire = 0;
    p[i].obj = NULL;
    p[i].next = (i + 1 < n) ? &p[i + 1] : NULL;
  }
  ht->ovf_free = n ? &p[0] : NULL;
}

/* ── Chain helpers ────────────────────────────────────────────────────── */

static struct bucket *ht_chain_node_find(struct bucket *base, size_t idx,
                                         uint64_t meta, const void *key,
                                         size_t klen,
                                         struct bucket **prev_out) {
  struct bucket *prev = NULL, *cur = &base[idx];
  while (cur) {
    if ((cur->meta & ~HT_META_LOCK_BIT) == meta && cur->obj->key_len == (uint16_t)klen &&
        memcmp(cur->obj->data, key, klen) == 0) {
      if (prev_out)
        *prev_out = prev;
      return cur;
    }
    prev = cur;
    cur = cur->next;
  }
  return NULL;
}

static bool ht_chain_node_insert(struct hash_table *ht, struct bucket *base,
                                 size_t idx, uint64_t meta, uint64_t expire,
                                 struct kv_obj *obj) {
  struct bucket *b = &base[idx];
  if (ht_meta_empty(b->meta)) {
    b->meta = meta;
    b->expire = expire;
    b->obj = obj;
    return true;
  }
  while (b->next)
    b = b->next;
  struct bucket *nb = ht_ovf_bucket_alloc(ht);
  if (!nb)
    return false;
  nb->meta = meta;
  nb->expire = expire;
  nb->obj = obj;
  b->next = nb;
  return true;
}

/* ── Create / Destroy ─────────────────────────────────────────────────── */

struct hash_table *ht_table_create(struct thread_heap *heap, uint32_t thread_id,
                                   size_t initial_cap) {
  initial_cap =
      ht_pow2_next(initial_cap < HT_MIN_BUCKETS ? HT_MIN_BUCKETS : initial_cap);
  uint32_t ovf_cap = (uint32_t)(initial_cap / HT_OVF_RATIO);
  if (ovf_cap < 16)
    ovf_cap = 16;

  /* Allocate hash_table struct from thread heap */
  struct hash_table *ht =
      (struct hash_table *)mem_malloc(sizeof(struct hash_table));
  if (!ht)
    return NULL;
  memset(ht, 0, sizeof(*ht));

  /* Partition thread's arena_base region: HT/OVF, leaving a tail slice for
   * control-plane data that should live with the data-plane linear memory. */
  size_t usable_arena = g_mem_arena.per_thread_arena_size;
  if (usable_arena > ARENA_LINEAR_CTRL_RESERVE)
    usable_arena -= ARENA_LINEAR_CTRL_RESERVE;
  size_t arena_half = usable_arena / 2;
  void *ht_base = (void *)thread_arena_base(thread_id);
  void *ovf_base = (char *)ht_base + arena_half;

  /* Initialize HT arena */
  if (!linear_arena_init_manual(&ht->ht_arena, thread_id, ht_base, arena_half))
    goto fail;

  /* Initialize OVF arena */
  if (!linear_arena_init_manual(&ht->ovf_arena, thread_id, ovf_base,
                                 arena_half))
    goto fail;

  /* Grow HT arena to initial capacity */
  size_t ht_bytes = initial_cap * sizeof(struct bucket);
  if (!linear_arena_grow(&ht->ht_arena, ht_bytes))
    goto fail;

  /* Grow OVF arena to initial capacity */
  size_t ovf_bytes = ovf_cap * sizeof(struct bucket);
  if (!linear_arena_grow(&ht->ovf_arena, ovf_bytes))
    goto fail;

  ht->buckets = (struct bucket *)linear_arena_base(&ht->ht_arena);
  ht->cap = initial_cap;
  ht->old_cap = 0;
  ht->used = 0;
  ht->rehash_idx = -1;
  ht->rehash_disabled = false;
  ht->ovf_pool = (struct bucket *)linear_arena_base(&ht->ovf_arena);
  ht->ovf_pool_cap = ovf_cap;
  ht->ovf_free = NULL;
  ht->heap = heap;

  ht_ovf_pool_init(ht);

  return ht;

fail:
  mem_free(ht);
  return NULL;
}

void ht_table_destroy(struct hash_table *ht) {
  if (!ht)
    return;
  size_t total = ht->cap;
  for (size_t i = 0; i < total; i++) {
    struct bucket *b = &ht->buckets[i];
    while (b && !ht_meta_empty(b->meta)) {
      mem_free(b->obj);
      b = b->next;
    }
  }
  mem_free(ht);
}

/* ── Rehash ───────────────────────────────────────────────────────────── */

static bool ht_rehash_start(struct hash_table *ht) {
  size_t old_cap = ht->cap, new_cap = old_cap * 2;
  size_t new_total_bytes = new_cap * sizeof(struct bucket);
  if (!linear_arena_grow(&ht->ht_arena, new_total_bytes))
    return false;
  ht->buckets = (struct bucket *)linear_arena_base(&ht->ht_arena);
  ht->old_cap = old_cap;
  ht->cap = new_cap;
  ht->rehash_idx = 0;
  return true;
}

static void ht_rehash_tick(struct hash_table *ht) {
  size_t new_mask = ht->cap - 1;

  for (int step = 0;
       step < HT_REHASH_STEP && ht->rehash_idx < (int64_t)ht->old_cap; step++) {

    size_t i = (size_t)ht->rehash_idx++;
    struct bucket *pri = &ht->buckets[i];
    if (ht_meta_empty(pri->meta))
      continue;

    struct bucket *chain = pri->next;
    pri->next = NULL;
    size_t new_idx = (pri->meta >> 8) & new_mask;

    if (new_idx != i) {
      struct bucket *dst = &ht->buckets[new_idx];
      if (ht_meta_empty(dst->meta)) {
        dst->meta = pri->meta;
        dst->expire = pri->expire;
        dst->obj = pri->obj;
      } else {
        ht_chain_node_insert(ht, ht->buckets, new_idx, pri->meta, pri->expire,
                             pri->obj);
      }
      pri->meta = pri->expire = 0;
      pri->obj = NULL;
    }

    while (chain) {
      struct bucket *nxt = chain->next;
      chain->next = NULL;
      size_t dst_idx = (chain->meta >> 8) & new_mask;
      struct bucket *dst = &ht->buckets[dst_idx];
      if (ht_meta_empty(dst->meta)) {
        dst->meta = chain->meta;
        dst->expire = chain->expire;
        dst->obj = chain->obj;
        ht_ovf_bucket_release(ht, chain);
      } else {
        struct bucket *tail = dst;
        while (tail->next)
          tail = tail->next;
        tail->next = chain;
      }
      chain = nxt;
    }
  }

  if (ht->rehash_idx >= (int64_t)ht->old_cap) {
    ht->old_cap = 0;
    ht->rehash_idx = -1;
  }
}

/* ── Lookup routing ───────────────────────────────────────────────────── */

static struct bucket *ht_key_lookup(struct hash_table *ht, uint64_t meta,
                                    const void *key, size_t klen,
                                    struct bucket **prev_out) {
  size_t hash56 = meta >> 8;

  if (ht->rehash_idx < 0)
    return ht_chain_node_find(ht->buckets, hash56 & (ht->cap - 1), meta, key,
                              klen, prev_out);

  size_t old_idx = hash56 & (ht->old_cap - 1);
  size_t new_idx = hash56 & (ht->cap - 1);

  if ((int64_t)old_idx < ht->rehash_idx)
    return ht_chain_node_find(ht->buckets, new_idx, meta, key, klen, prev_out);

  struct bucket *f =
      ht_chain_node_find(ht->buckets, old_idx, meta, key, klen, prev_out);
  if (f)
    return f;
  return ht_chain_node_find(ht->buckets, new_idx, meta, key, klen, prev_out);
}

/* ── struct bucket unlink (does not free obj) ─────────────────────────────────
 */

static void ht_bucket_node_unlink(struct hash_table *ht, struct bucket *prev,
                                  struct bucket *cur) {
  ht->used--;
  if (!prev) {
    if (cur->next) {
      struct bucket *s = cur->next;
      cur->meta = s->meta;
      cur->expire = s->expire;
      cur->obj = s->obj;
      cur->next = s->next;
      ht_ovf_bucket_release(ht, s);
    } else {
      cur->meta = cur->expire = 0;
      cur->obj = NULL;
    }
  } else {
    prev->next = cur->next;
    ht_ovf_bucket_release(ht, cur);
  }
}

/* ── Public API ───────────────────────────────────────────────────────── */

struct kv_obj *ht_bucket_put(struct hash_table *ht, struct kv_obj *obj,
                             enum val_type type, uint64_t expire_ms,
                             uint32_t put_flags) {
  const void *key = obj_key_get(obj);
  size_t klen = obj->key_len;

  if (ht->rehash_idx >= 0)
    ht_rehash_tick(ht);
  if (!ht->rehash_disabled && ht->rehash_idx < 0 &&
      (double)ht->used / (double)ht->cap > HT_LOAD_MAX) {
    if (!ht_rehash_start(ht))
      ht->rehash_disabled = true;
  }

  uint64_t meta = ht_meta_pack(ht_key_hash(key, klen), type);
  struct bucket *found = ht_key_lookup(ht, meta, key, klen, NULL);

  if (put_flags & HT_PUT_NX) {
    if (found)
      return (struct kv_obj *)(uintptr_t)-2;
  } else if (put_flags & HT_PUT_XX) {
    if (!found)
      return (struct kv_obj *)(uintptr_t)-2;
  }

  if (found) {
    struct kv_obj *old_obj = found->obj;
    found->obj = obj;
    found->expire = expire_ms;
    found->meta = meta;
    return old_obj;
  }

  /* New key insert */
  size_t idx = (meta >> 8) & (ht->cap - 1);
  if (!ht_chain_node_insert(ht, ht->buckets, idx, meta, expire_ms, obj))
    return (struct kv_obj *)(uintptr_t)-1;
  ht->used++;

  return NULL;
}

struct kv_obj *ht_bucket_get(struct hash_table *ht, const void *key,
                             size_t klen, enum val_type type,
                             uint64_t net_time_ms_get) {
  uint64_t meta = ht_meta_pack(ht_key_hash(key, klen), type);
  struct bucket *f = ht_key_lookup(ht, meta, key, klen, NULL);
  if (!f)
    return NULL;
  if (f->expire != 0 && net_time_ms_get != 0 && net_time_ms_get >= f->expire)
    return NULL;
  return f->obj;
}

struct kv_obj *ht_bucket_get_lazy(struct hash_table *ht, const void *key,
                                  size_t klen, enum val_type type,
                                  uint64_t net_time_ms_get,
                                  struct kv_obj **expired_out) {
  *expired_out = NULL;
  uint64_t meta = ht_meta_pack(ht_key_hash(key, klen), type);
  struct bucket *prev = NULL;
  struct bucket *f = ht_key_lookup(ht, meta, key, klen, &prev);
  if (!f)
    return NULL;
  if (f->expire == 0 || net_time_ms_get == 0 || net_time_ms_get < f->expire)
    return f->obj;
  /* Key expired — but if locked by MSET, skip expiry */
  if (ht_meta_is_locked(f->meta))
    return f->obj;
  *expired_out = f->obj;
  ht_bucket_node_unlink(ht, prev, f);
  return NULL;
}

bool ht_bucket_set_lock_status(struct hash_table *ht, const void *key,
                               size_t klen, enum val_type type, bool locked) {
  uint64_t meta = ht_meta_pack(ht_key_hash(key, klen), type);
  struct bucket *f = ht_key_lookup(ht, meta, key, klen, NULL);
  if (!f)
    return false;
  if (locked)
    ht_meta_lock_set(&f->meta);
  else
    ht_meta_lock_clear(&f->meta);
  return true;
}

bool ht_bucket_is_locked(struct hash_table *ht, const void *key, size_t klen,
                         enum val_type type) {
  uint64_t meta = ht_meta_pack(ht_key_hash(key, klen), type);
  struct bucket *f = ht_key_lookup(ht, meta, key, klen, NULL);
  if (!f)
    return false;
  return ht_meta_is_locked(f->meta);
}

struct kv_obj *ht_bucket_take(struct hash_table *ht, const void *key,
                               size_t klen, enum val_type type) {
  uint64_t meta = ht_meta_pack(ht_key_hash(key, klen), type);
  struct bucket *prev = NULL;
  struct bucket *cur = ht_key_lookup(ht, meta, key, klen, &prev);
  if (!cur)
    return NULL;
  struct kv_obj *obj = cur->obj;
  ht_bucket_node_unlink(ht, prev, cur);
  return obj;
}

struct kv_obj *ht_bucket_take_if_unlocked(struct hash_table *ht,
                                          const void *key, size_t klen,
                                          enum val_type type,
                                          struct kv_obj *expected_obj,
                                          bool *locked_out) {
  if (locked_out)
    *locked_out = false;

  uint64_t meta = ht_meta_pack(ht_key_hash(key, klen), type);
  struct bucket *prev = NULL;
  struct bucket *cur = ht_key_lookup(ht, meta, key, klen, &prev);
  if (!cur || cur->obj != expected_obj)
    return NULL;
  if (ht_meta_is_locked(cur->meta)) {
    if (locked_out)
      *locked_out = true;
    return NULL;
  }

  struct kv_obj *obj = cur->obj;
  ht_bucket_node_unlink(ht, prev, cur);
  return obj;
}

bool ht_bucket_delete(struct hash_table *ht, const void *key, size_t klen,
                      enum val_type type) {
  uint64_t meta = ht_meta_pack(ht_key_hash(key, klen), type);
  struct bucket *prev = NULL;
  struct bucket *cur = ht_key_lookup(ht, meta, key, klen, &prev);
  if (!cur)
    return false;
  ht_bucket_node_unlink(ht, prev, cur);
  return true;
}

int ht_bucket_expire(struct hash_table *ht, size_t idx,
                     uint64_t net_time_ms_get, struct kv_obj **out,
                     int out_cap) {
  if (idx >= ht->cap)
    return 0;
  int found = 0;
  struct bucket *b = &ht->buckets[idx];

  while (!ht_meta_empty(b->meta) && b->expire != 0 &&
          net_time_ms_get >= b->expire) {
    if (found < out_cap)
      out[found] = b->obj;
    found++;
    ht_bucket_node_unlink(ht, NULL, b);
  }
  if (!ht_meta_empty(b->meta)) {
    struct bucket *prev = b, *cur = b->next;
    while (cur) {
      if (cur->expire != 0 && net_time_ms_get >= cur->expire) {
        if (found < out_cap)
          out[found] = cur->obj;
        found++;
        struct bucket *nxt = cur->next;
        ht_bucket_node_unlink(ht, prev, cur);
        cur = nxt;
      } else {
        prev = cur;
        cur = cur->next;
      }
    }
  }
  return found;
}

uint64_t ht_expire_get(struct hash_table *ht, const void *key, size_t klen,
                       enum val_type type) {
  uint64_t meta = ht_meta_pack(ht_key_hash(key, klen), type);
  struct bucket *f = ht_key_lookup(ht, meta, key, klen, NULL);
  if (!f)
    return (uint64_t)-1;
  return f->expire;
}

bool ht_rehash_status(struct hash_table *ht) { return ht->rehash_idx >= 0; }

void ht_table_flush(struct hash_table *ht) {
  if (!ht)
    return;
  size_t total = ht->old_cap ? ht->old_cap + ht->cap : ht->cap;
  for (size_t i = 0; i < total; i++) {
    struct bucket *b = &ht->buckets[i];
    if (ht_meta_empty(b->meta))
      continue;
    mem_free(b->obj);
    struct bucket *ovf = b->next;
    while (ovf) {
      struct bucket *nxt = ovf->next;
      mem_free(ovf->obj);
      ht_ovf_bucket_release(ht, ovf);
      ovf = nxt;
    }
    b->meta = b->expire = 0;
    b->obj = NULL;
    b->next = NULL;
  }
  if (ht->old_cap > 0) {
    ht->old_cap = 0;
    ht->rehash_idx = -1;
  }
  ht->used = 0;
}

struct bucket *ht_bucket_random(struct hash_table *ht) {
  if (ht->used == 0)
    return NULL;
  for (int i = 0; i < 5; i++) {
    size_t idx = (size_t)rand() % ht->cap;
    struct bucket *b = &ht->buckets[idx];
    if (!ht_meta_empty(b->meta))
      return b;
  }
  for (size_t i = 0; i < ht->cap; i++)
    if (!ht_meta_empty(ht->buckets[i].meta))
      return &ht->buckets[i];
  return NULL;
}
