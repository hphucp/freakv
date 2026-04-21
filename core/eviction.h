#ifndef EVICTION_H
#define EVICTION_H

#include <stdbool.h>
#include <stddef.h>

struct shard;

bool shard_mem_evict(struct shard *s, size_t size_req);

#endif /* EVICTION_H */
