#ifndef RESP_H
#define RESP_H

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

#include "../memory/slab.h"

#define RESP_RBUF_INIT 4096

enum resp_state {
  RESP_PARSE_ARRAY_LEN = 0, /* reading *N\r\n            */
  RESP_PARSE_BULK_LEN = 1,  /* reading $N\r\n            */
  RESP_PARSE_BULK_DATA = 2, /* reading N bytes + \r\n    */
};

struct resp_parser {
  /* Buffer position tracking (buffer owned by net_conn) */
  size_t rbuf_parsed; /* bytes consumed by parser             */
  size_t cmd_start;   /* offset where current command begins  */

  /* Command assembly */
  enum resp_state parse_state;
  int argc_expected; /* from *N line                */
  int argc_got;      /* args assembled so far       */

  /* Dynamic argv — grows as needed, no hard limit */
  char **argv;
  size_t *arglen;
  int argv_cap;

  /* Bulk string being read */
  int bulk_len;          /* from $N line, -1 = nil           */
  size_t bulk_data_read; /* bytes of current bulk read so far*/

  /* Allocator for argv/arglen */
  struct slab_allocator *allocator;
};

/* Ensure argv/arglen can hold at least `needed` entries */
static inline bool resp_argv_ensure(struct resp_parser *p, int needed) {
  if (needed <= p->argv_cap)
    return true;
  int newcap = p->argv_cap ? p->argv_cap * 2 : 8;
  while (newcap < needed)
    newcap *= 2;

  /* Use custom allocator */
  char **nv = (char **)slab_obj_realloc(p->allocator, p->argv,
                                        (size_t)newcap * sizeof(char *));
  if (!nv)
    return false;
  p->argv = nv;
  size_t *nl = (size_t *)slab_obj_realloc(p->allocator, p->arglen,
                                          (size_t)newcap * sizeof(size_t));
  if (!nl)
    return false;
  p->arglen = nl;
  p->argv_cap = newcap;
  return true;
}

static inline void resp_parser_free(struct resp_parser *p) {
  if (p->allocator) {
    slab_obj_free(p->allocator, p->argv);
    p->argv = NULL;
    slab_obj_free(p->allocator, p->arglen);
    p->arglen = NULL;
    p->argv_cap = 0;
  }
}

#endif /* RESP_H */
