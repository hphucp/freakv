#ifndef MESSAGE_H
#define MESSAGE_H

#include <stddef.h>
#include <stdint.h>

struct resp_cmd {
  uint32_t argc;
  char **argv;    /* dynamically allocated */
  size_t *arglen; /* dynamically allocated */
};

struct net_buf;
struct mset_stat;

/*
 * spsc_message op (16-bit):
 * [High 4 bits]: System Op (MSG_SYS_*)
 * [Low 12 bits]: Command ID (MSG_CMD_*)
 */

#define MSG_SYS_SHIFT 12
#define MSG_SYS_MASK (0xF << MSG_SYS_SHIFT)
#define MSG_CMD_MASK 0x0FFF

#define MSG_PACK_OP(sys, cmd)                                                  \
  (((sys) << MSG_SYS_SHIFT) | ((cmd) & MSG_CMD_MASK))
#define MSG_GET_SYS(op) (((op) & MSG_SYS_MASK) >> MSG_SYS_SHIFT)
#define MSG_GET_CMD(op) ((op) & MSG_CMD_MASK)

/* System Opcodes (High 4 bits) */
enum spsc_sys_op {
  MSG_SYS_REQ = 0x1,
  MSG_SYS_REPLY = 0x2,
  MSG_SYS_RELEASE = 0x3,
};

/* Command / Sub-Opcodes (Low 12 bits) */
enum resp_cmd_id {
  MSG_CMD_GET = 1,
  MSG_CMD_SET = 2,
  MSG_CMD_DEL = 3,
  MSG_CMD_EXISTS = 4,
  MSG_CMD_TTL = 5,
  MSG_CMD_PTTL = 6,
  MSG_CMD_TYPE = 7,
  MSG_CMD_INCR = 8,
  MSG_CMD_STRLEN = 9,
  MSG_CMD_DBSIZE = 10, // Added MSG_CMD_DBSIZE

  MSG_CMD_EXPIRE = 11,    // Shifted to avoid conflict with MSG_CMD_DBSIZE
  MSG_CMD_PEXPIRE = 12,   // Shifted
  MSG_CMD_PERSIST = 13,   // Shifted
  MSG_CMD_EXPIREAT = 14,  // Shifted
  MSG_CMD_PEXPIREAT = 15, // Shifted

  /* Internal split ops for MGET/MSET/DEL */
  MSG_CMD_MGET_PART = 100,
  MSG_CMD_MSET_PART = 101,
  MSG_CMD_SET_NX = 102,
  MSG_CMD_SET_XX = 103,
  MSG_CMD_DEL_PART = 104,

  /* ── Old synchronous MSET protocol (DEPRECATED — kept for enum stability) */
  MSG_CMD_MSET_PREPARE = 200,      /* DEPRECATED — do not use              */
  MSG_CMD_MSET_DONE = 201,         /* DEPRECATED — do not use              */
  MSG_CMD_MSET_FAIL = 202,         /* DEPRECATED — do not use              */

  /* ── Async MSET 2PC protocol (new) ──────────────────────────────────── */
  MSG_CMD_MSET_FIN = 203,          /* coordinator → exec: commit           */
  MSG_CMD_MSET_KEY = 210,          /* coordinator → exec: per-key PREPARE  */
  MSG_CMD_MSET_ACK = 211,          /* last exec → coordinator: all done    */
  MSG_CMD_MSET_FIN_ACK = 212,      /* last exec → coordinator: FIN complete*/
  MSG_CMD_MSET_KEY_BATCH = 213,    /* coordinator → exec: all keys for one shard */

  /* DEPRECATED */
  MSG_CMD_MSET_ROLLBACK = 204,     /* DEPRECATED — do not use              */
  MSG_CMD_MSET_ROLLBACK_ACK = 205, /* DEPRECATED — do not use              */
};

/*
 * Batch descriptor for atomic MSET PREPARE.
 * Allocated by coordinator, pointed to by spsc_message.key_ptr.
 * Contains all key-value pairs destined for one participant shard.
 */
struct mset_batch_entry {
  const char *key;
  size_t      klen;
  const char *val;
  size_t      vlen;
  uint32_t    sub_idx; /* original pair index in MSET — needed for cmd_info tracking */
};

/*
 * Per-shard batch: coordinator allocates one of these per remote shard.
 * Header + entries are a single slab allocation; coordinator frees via
 * stat->batch_cleanup_head linked list in stat_free_real.
 * Participant reads entries[] read-only; never frees.
 */
struct mset_batch {
  struct mset_batch *cleanup_next; /* intrusive list for stat_free_real    */
  uint32_t           count;        /* number of entries                    */
  struct mset_batch_entry entries[]; /* C99 flexible array — entries follow */
};

enum msg_reply_type {
  REPLY_TYPE_NONE = 0,
  REPLY_TYPE_OK = 1,
  REPLY_TYPE_NIL = 2,
  REPLY_TYPE_INT = 3,
  REPLY_TYPE_ERR = 4,
  REPLY_TYPE_BUF = 5,
  REPLY_TYPE_PONG = 6,
  REPLY_TYPE_KV_OBJ = 7,
  REPLY_TYPE_BUF_STATIC = 8,
};

struct spsc_message {
  uint16_t op;         /* Packed system op + command id            */
  uint8_t shard_owner; /* Shard that originated the message        */
  uint8_t _pad0;
  union {
    struct {
      struct resp_cmd cmd; /* Parsed command (passed by value)       */
      void *conn_ptr;
      struct net_buf *req_nb; /* input buffer refcount                 */
      uint32_t pipeline_idx;  /* Sequence number in proxy pipeline     */
    } regular_req;

    struct {
      void *key_ptr;
      void *conn_ptr;
      struct net_buf *req_nb;
      uint32_t key_len;
      uint32_t pipeline_idx;
      uint32_t sub_idx;
    } key_part_req;

    struct {
      void *conn_ptr;
      uint32_t pipeline_idx;
      uint32_t sub_idx;
    } dbsize_req;

    struct {
      void *conn_ptr;
      struct net_buf *req_nb;
      void *kv_obj_ptr; /* Ref'd object for zero-copy reply         */
      void *reply_buf;  /* RESP binary reply                        */
      int64_t reply_int; /* For INT replies                         */
      uint32_t pipeline_idx;
      uint32_t sub_idx;
      uint32_t reply_len;
      uint8_t reply_type; /* enum msg_reply_type                     */
    } reply;

    struct {
      void *kv_obj_ptr;
      void *reply_buf;
    } release;

    struct {
      struct mset_batch *batch;
      struct mset_stat *stat;
    } mset_prepare_batch;

    struct {
      void *conn_ptr;
      uint32_t pipeline_idx;
    } mset_ack;

    struct {
      struct mset_stat *stat;
    } mset_stat;
  } u;
};

#endif /* MESSAGE_H */
