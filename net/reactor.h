#ifndef REACTOR_H
#define REACTOR_H

#include "../io/event_source.h"
#include "conn.h"
#include "shard.h"

#include <stdbool.h>
#include <stdint.h>

#define MAX_EVENTS 64
/* epoll_wait timeout: how often the reactor wakes to drain SPSC queues and
 * flush pending pipeline replies when no network events fire.
 * Keep at 1ms for acceptable cross-shard reply latency. */
#define EPOLL_TIMEOUT_MS 1

typedef struct reactor_listener_source {
  int fd;
  bool shared;
  event_source_t event_src;
  reactor_t *reactor;
} reactor_listener_source_t;

typedef struct reactor_spsc_source {
  int fd;
  event_source_t event_src;
  reactor_t *reactor;
} reactor_spsc_source_t;

typedef struct reactor_timer_source {
  int fd;
  event_source_t event_src;
  reactor_t *reactor;
} reactor_timer_source_t;

typedef struct reactor {
  int epoll_fd;
  int listen_fd;        /* per-shard port: base_port + shard_id  */
  int shared_listen_fd; /* shared port: base_port (SO_REUSEPORT) */
  int wake_fd;
  int timer_fd;
  reactor_listener_source_t listen_source;
  reactor_listener_source_t shared_listen_source;
  reactor_spsc_source_t spsc_source;
  reactor_timer_source_t timer_source;
  uint64_t proxy_wake_mask;
  uint16_t port;      /* per-shard port                        */
  uint16_t base_port; /* shared single-port entry point        */
  char bind_ip[64];
  struct shard_engine *engine;
  struct shard *shard;
  uint64_t conn_generation;    /* per-reactor, single-threaded access */
  struct net_conn *dirty_head; /* flushing and pipeline drain (FD open) */
  struct net_conn *dead_head;  /* waiting for pipeline drain (FD closed) */
} reactor_t;

int net_reactor_init(struct reactor *r, struct shard_engine *engine,
                     struct shard *shard, uint16_t base_port,
                     uint16_t shard_port, const char *advertise_ip);
void net_reactor_run(struct reactor *r);
void net_reactor_destroy(struct reactor *r);

#endif /* REACTOR_H */
