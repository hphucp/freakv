#include "event_source.h"

#include "../net/reactor.h"

#include <string.h>
#include <sys/epoll.h>

void event_source_init(event_source_t *src, int fd, uint32_t event_mask,
                       event_source_io_fn read_fn,
                       event_source_io_fn write_fn,
                       event_source_error_fn error_fn) {
  if (!src)
    return;
  memset(src, 0, sizeof(*src));
  src->fd = fd;
  src->event_mask = event_mask;
  src->on_readable = read_fn;
  src->on_writable = write_fn;
  src->on_error = error_fn;
}

int event_source_add_to_epoll(reactor_t *r, event_source_t *src) {
  if (!r || !src || src->fd < 0)
    return -1;

  struct epoll_event ev;
  memset(&ev, 0, sizeof(ev));
  ev.events = src->event_mask;
  ev.data.ptr = src;
  return epoll_ctl(r->epoll_fd, EPOLL_CTL_ADD, src->fd, &ev);
}

int event_source_remove_from_epoll(reactor_t *r, event_source_t *src) {
  if (!r || !src || src->fd < 0)
    return -1;
  return epoll_ctl(r->epoll_fd, EPOLL_CTL_DEL, src->fd, NULL);
}

static int event_source_update_epoll(reactor_t *r, event_source_t *src,
                                     uint32_t event_mask) {
  if (!r || !src || src->fd < 0)
    return -1;

  struct epoll_event ev;
  memset(&ev, 0, sizeof(ev));
  src->event_mask = event_mask;
  ev.events = src->event_mask;
  ev.data.ptr = src;
  return epoll_ctl(r->epoll_fd, EPOLL_CTL_MOD, src->fd, &ev);
}

int event_source_want_write(reactor_t *r, event_source_t *src) {
  if (!src)
    return -1;
  return event_source_update_epoll(r, src, src->event_mask | EPOLLOUT);
}

int event_source_drop_write(reactor_t *r, event_source_t *src) {
  if (!src)
    return -1;
  return event_source_update_epoll(r, src, src->event_mask & ~EPOLLOUT);
}
