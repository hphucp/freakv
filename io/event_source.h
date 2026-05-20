#ifndef IO_EVENT_SOURCE_H
#define IO_EVENT_SOURCE_H

#include <stddef.h>
#include <stdint.h>

typedef struct reactor reactor_t;
typedef struct event_source event_source_t;

typedef void (*event_source_io_fn)(event_source_t *src);
typedef void (*event_source_error_fn)(event_source_t *src, uint32_t events);

/*
 * Generic epoll-backed source.
 *
 * Concrete owners embed event_source_t and callbacks recover the owner with
 * container_of(). The source has no separate allocation and no ctx pointer.
 */
struct event_source {
  int fd;
  uint32_t event_mask;
  event_source_io_fn on_readable;
  event_source_io_fn on_writable;
  event_source_error_fn on_error;
};

/* Same contract as the Linux kernel helper: ptr must point at member. */
#ifndef container_of
#define container_of(ptr, type, member)                                        \
  ((type *)((char *)(ptr) - offsetof(type, member)))
#endif

void event_source_init(event_source_t *src, int fd, uint32_t event_mask,
                       event_source_io_fn read_fn,
                       event_source_io_fn write_fn,
                       event_source_error_fn error_fn);

static inline void event_source_set_event_mask(event_source_t *src,
                                               uint32_t event_mask) {
  if (src)
    src->event_mask = event_mask;
}

int event_source_add_to_epoll(reactor_t *r, event_source_t *src);
int event_source_remove_from_epoll(reactor_t *r, event_source_t *src);

int event_source_want_write(reactor_t *r, event_source_t *src);
int event_source_drop_write(reactor_t *r, event_source_t *src);

#endif
