#ifndef REACTOR_H
#define REACTOR_H

#include "conn.h"
#include "shard.h"

#include <stdint.h>

#define MAX_EVENTS 64
/* epoll_wait timeout: how often the reactor wakes to drain SPSC queues and
 * flush pending pipeline replies when no network events fire.
 * Keep at 1ms for acceptable cross-shard reply latency. */
#define EPOLL_TIMEOUT_MS 1

struct reactor {
  int epoll_fd;
  int listen_fd;        /* per-shard port: base_port + shard_id  */
  int shared_listen_fd; /* shared port: base_port (SO_REUSEPORT) */
  int wake_fd;
  int timer_fd;
  uint64_t proxy_wake_mask;
  uint16_t port;      /* per-shard port                        */
  uint16_t base_port; /* shared single-port entry point        */
  char bind_ip[64];
  struct shard_engine *engine;
  struct shard *shard;
  uint64_t conn_generation;    /* per-reactor, single-threaded access */
  struct net_conn *dirty_head; /* flushing and pipeline drain (FD open) */
  struct net_conn *dead_head;  /* waiting for pipeline drain (FD closed) */
};

#define EV_MARK_SPECIAL (1ULL << 63)
#define EV_TYPE_LISTEN 1
#define EV_TYPE_SHARED 2
#define EV_TYPE_TIMER 3
#define EV_TYPE_WAKE 4

#define EV_PACK(type, fd)                                                      \
  (EV_MARK_SPECIAL | ((uint64_t)(fd) << 8) | (uint64_t)(type))
#define EV_GET_TYPE(u64) ((u64) & 0xFF)
#define EV_GET_FD(u64) (((u64) >> 8) & 0xFFFFFFFF)

int net_reactor_init(struct reactor *r, struct shard_engine *engine,
                     struct shard *shard, uint16_t base_port,
                     uint16_t shard_port, const char *advertise_ip);
void net_reactor_run(struct reactor *r);
void net_reactor_destroy(struct reactor *r);

#endif /* REACTOR_H */
