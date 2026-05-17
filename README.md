# FreakV

FreakV started as a simple university course project, built primarily for
learning and experimentation. The original goal was not to create a production
database, but to understand how a high-performance in-memory key-value store can
be designed from the ground up: networking, sharding, memory management,
concurrency, persistence, and command execution.

Over time, the project grew into a Redis-compatible string store with several
systems-level components implemented directly in C:

- **Shared-nothing sharding:** data is partitioned across independent shards, so
  each shard owns its hash table, memory pool, TTL index, LRU state, and command
  execution path.
- **Custom memory management:** FreakV uses custom allocators and slab-style
  allocation paths to reduce allocator overhead and keep hot data structures
  predictable.
- **Asynchronous shard communication:** cross-shard work is dispatched through
  SPSC queues, allowing shards to communicate without a global shared execution
  lock.
- **Zero-lock hot paths:** normal single-shard operations are designed to avoid
  shared locks in the command execution path.
- **Zero-copy reply path:** value replies can reference stored objects directly,
  avoiding unnecessary value copies when sending responses back to clients.
- **Persistence support:** FreakV supports snapshotting so the in-memory state
  can be periodically written out without turning the cache-only fast path into
  a fully synchronous persistence path.
- **Cache features:** TTL expiration and LRU eviction are implemented as part of
  the shard-owned state.
- **Redis-style string commands:** the server supports common Redis commands for
  string values, including basic read/write/delete, existence checks, TTL
  operations, multi-key reads/deletes, and database size queries.
- **Async MSET transaction path:** `MSET` uses an asynchronous two-phase protocol
  with a lock manager, per-key prepare/finish phases, batched cross-shard
  prepare messages, and deferred execution for commands blocked behind active
  MSET locks.

## Quick Start

FreakV is a Linux-focused C project. The server uses Linux networking primitives
such as `epoll`, `eventfd`, and `timerfd`, and the current build is intended for
x86-64 Linux hosts.

### Requirements

- Linux environment with POSIX threads.
- GCC with C11 support. The default Makefile uses `gcc`.
- `make`.
- No external runtime libraries are required beyond libc and pthreads.

Optional tools for local testing:

- `redis-cli` for manual command checks.
- `redis-benchmark` or `memtier_benchmark` for load testing.

### Build

```bash
make clean
make
```

The build produces a `freakv` binary in the repository root.

Useful build flags:

```bash
# AddressSanitizer build
make clean
make ASAN=1

# Enable profiling counters/logging paths compiled behind FREAKV_PROFILE
make clean
make PROFILE=1

# Enable MSET debug instrumentation
make clean
make MSET_DEBUG=1
```

### Run

```bash
./freakv --port 7379 --shards 4
```

By default, FreakV listens on port `7379` and auto-detects the shard count from
the host CPU count. A few commonly used options:

```bash
./freakv --help
./freakv --port 6379 --shards 32
./freakv --port 6379 --shards 32 --maxmemory 64G
./freakv --port 6379 --shards 32 --snapshot-dir /tmp/freakv --snapshot-interval 10000
```

Manual smoke test:

```bash
redis-cli -p 7379 SET hello world
redis-cli -p 7379 GET hello
redis-cli -p 7379 MSET a 1 b 2 c 3
redis-cli -p 7379 MGET a b c
```

## Client Compatibility

FreakV speaks Redis' RESP2 request/response protocol for the supported command
subset. Standard Redis clients can connect directly to the configured port
without custom client-side code, including `redis-cli`, `redis-py`, ioredis,
and Jedis.

If a client defaults to RESP3, configure it to use RESP2. FreakV responds to
`HELLO` with protocol metadata for RESP2 compatibility, but it does not aim to
be a full RESP3 server.

## Supported Commands

FreakV is currently a Redis-compatible string/key-value subset, not a complete
Redis replacement.

String and key operations:

- `GET`
- `SET` with `NX`, `XX`, `EX`, `PX`, and `KEEPTTL`
- `DEL`
- `MGET`
- `MSET`
- `STRLEN`

Metadata and expiration:

- `EXISTS`
- `TTL`
- `PTTL`
- `EXPIRE`
- `PEXPIRE`
- `EXPIREAT`
- `PEXPIREAT`
- `PERSIST`
- `TYPE`
- `DBSIZE`

Connection and compatibility commands:

- `PING`
- `ECHO`
- `HELLO`
- `COMMAND`
- `INFO`
- `CLIENT`
- `SELECT`
- `CLUSTER`
- `FLUSHDB`
- `FLUSHALL`

Unsupported Redis features include non-string data types, counters, Lua/scripts,
streams, pub/sub, transactions with `MULTI`/`EXEC`, replication, Redis Cluster
mode, ACLs, modules, and RESP3-specific response semantics.

## Benchmark — FreakV vs Dragonfly

This benchmark is not meant to present FreakV as a mature replacement for
existing databases. FreakV only supports Redis string commands, while Dragonfly
(v1.25.2) is a production-grade system with replication, cluster mode, search,
sorted sets, and many more data types and features. The comparison here is
limited to raw GET/SET/MSET throughput and latency, which is a narrow slice of
what a real-world workload looks like.

That said, the results below record how far a learning-focused project can be
pushed when the implementation pays close attention to shard ownership, memory
layout, lock avoidance, and cross-shard coordination.

### Environment

| Component | Specification |
|-----------|---------------|
| **Instance Type** | AWS `c6in.16xlarge` × 2 (one server, one client) |
| **vCPUs** | 64 (Intel Xeon 8375C Ice Lake, 3.5 GHz all-core turbo) |
| **Memory** | 128 GiB DDR4 |
| **Network** | 100 Gbps, connected via private IP within the same VPC |
| **OS** | Amazon Linux 2023 |
| **Benchmark Tool** | [memtier_benchmark](https://github.com/RedisLabs/memtier_benchmark) |
| **Server Worker Threads** | 32 (FreakV shards / Dragonfly proactor threads) |
| **Client Load Generator** | 60 memtier threads on the client instance |

### Server Configuration

**FreakV (cache mode):**
```bash
freakv --shards 32 --port 6379
```

**FreakV (snapshot mode):**
```bash
freakv --shards 32 --port 6379 --snapshot-dir /tmp/freakv --snapshot-interval 10000
```

**Dragonfly v1.25.2 (cache mode):**
```bash
dragonfly --bind 0.0.0.0 --port 6379 \
  --proactor_threads=32 \
  --cache_mode \
  --dbfilename "" \
  --maxmemory=128gb
```

### Benchmark Methodology

The benchmark was run from a separate client instance against the server's
private IP address. FreakV was run with 32 shards, while Dragonfly was run with
32 proactor threads.

The commands below use memtier's thread/client model:

- `-t 60` means 60 client-side memtier threads.
- `-c 20` means 20 client connections per thread.
- `-c 5 --pipeline=10` means fewer connections but 10 in-flight requests per
  connection.

No preload step was used for the SET/GET cache-mode comparison. The GET tests
ran after the SET tests in the same server session, so the measured GET workload
read from data written earlier in that session.

### memtier_benchmark Commands

```bash
# SET, pipeline 1
memtier_benchmark -s $SERVER --ratio 1:0 -t 60 -c 20 -n 200000 \
  --distinct-client-seed --hide-histogram \
  --print-percentiles=50,99,99.9,100

# GET, pipeline 1
memtier_benchmark -s $SERVER --ratio 0:1 -t 60 -c 20 -n 200000 \
  --distinct-client-seed --hide-histogram \
  --print-percentiles=50,99,99.9,100

# SET, pipeline 10
memtier_benchmark -s $SERVER --ratio 1:0 -t 60 -c 5 -n 2000000 \
  --distinct-client-seed --hide-histogram --pipeline=10 \
  --print-percentiles=50,99,99.9,100

# GET, pipeline 10
memtier_benchmark -s $SERVER --ratio 0:1 -t 60 -c 5 -n 2000000 \
  --distinct-client-seed --hide-histogram --pipeline=10 \
  --print-percentiles=50,99,99.9,100

# MSET — 10 keys per command
memtier_benchmark -s $SERVER \
  --command="MSET __key__ __data__" \
  --multi-key=10 \
  -t 60 -c 20 -n 200000 \
  --distinct-client-seed --hide-histogram \
  --print-percentiles=50,99,99.9,100
```

For the cache-mode SET/GET comparison, each system was started once and the four
measured tests were run sequentially in this exact order: SET pipeline 10 → SET
pipeline 1 → GET pipeline 10 → GET pipeline 1. The server was **not** restarted
between those four tests. The GET results therefore depend on the keyspace and
data accumulated by the earlier SET runs, reflecting a continuous-operation
session rather than isolated single-test runs.

The MSET and snapshot results were separate sessions from the SET/GET cache-mode
run. The snapshot result was collected only for FreakV with snapshots enabled
every 10 seconds.

The numbers below are from the recorded benchmark session. For formal
regression tracking, run each test multiple times and report median/min/max;
the `p100` value in particular is sensitive to one-off scheduler and network
events.

### Results — Cache Mode (No Persistence)

**SET**

| Pipeline | System | Ops/sec | Avg Latency | p50 | p99 | p99.9 | p100 (Max) |
|----------|--------|--------:|------------:|----:|----:|------:|----------:|
| 1 | FreakV | **4,856,863** | 0.272 ms | 0.239 ms | 0.943 ms | 1.479 ms | 21.247 ms |
| 1 | Dragonfly | 4,744,278 | 0.284 ms | 0.247 ms | 0.783 ms | 1.151 ms | 16.639 ms |
| 10 | FreakV | **13,649,228** | 1.079 ms | 0.959 ms | 3.535 ms | 6.751 ms | 104.959 ms |
| 10 | Dragonfly | 10,102,369 | 1.360 ms | 1.319 ms | 2.239 ms | 3.023 ms | 231.423 ms |

**GET**

| Pipeline | System | Ops/sec | Avg Latency | p50 | p99 | p99.9 | p100 (Max) |
|----------|--------|--------:|------------:|----:|----:|------:|----------:|
| 1 | FreakV | 4,813,355 | 0.281 ms | 0.255 ms | 0.807 ms | 1.175 ms | 15.167 ms |
| 1 | Dragonfly | **4,936,196** | 0.280 ms | 0.247 ms | 0.767 ms | 1.151 ms | 21.247 ms |
| 10 | FreakV | **12,283,156** | 1.153 ms | 1.095 ms | 2.447 ms | 3.647 ms | 25.087 ms |
| 10 | Dragonfly | 9,715,692 | 1.326 ms | 1.279 ms | 2.079 ms | 2.575 ms | 6.431 ms |

### Results — MSET (10 Keys per Command, No Pipeline)

| System | Ops/sec | Avg Latency | p50 | p99 | p99.9 | p100 (Max) |
|--------|--------:|------------:|----:|----:|------:|----------:|
| Dragonfly | **3,984,998** | 0.306 ms | 0.271 ms | 0.783 ms | 1.119 ms | 231.423 ms |
| FreakV | 3,418,863 | 0.383 ms | 0.359 ms | 0.799 ms | 1.463 ms | 456.703 ms |

Dragonfly leads by ~17% on MSET throughput with tighter latency across all
percentiles. FreakV's MSET path uses an asynchronous two-phase transaction
protocol with cross-shard coordination, which adds overhead compared to
single-key SET. This is an initial implementation that proves correctness of
the transaction mechanism; there is still room for optimization in the
cross-shard prepare/finish pipeline, batching strategy, and lock contention
handling.

### Results — FreakV with Snapshot (every 10s)

| Operation | Pipeline | Ops/sec | Avg Latency | p50 | p99 | p99.9 | p100 (Max) |
|-----------|----------|--------:|------------:|----:|----:|------:|----------:|
| SET | 10 | **10,768,637** | 1.170 ms | 1.039 ms | 3.567 ms | 6.527 ms | 143.359 ms |

Other combinations (SET pipeline 1, GET with snapshot) were not tested in this
session.

Enabling snapshots reduces FreakV SET throughput by ~21% (13.6M → 10.8M ops/sec)
under pipeline 10, while p99 and p99.9 remain comparable.

### Summary

At 32 threads on identical hardware, FreakV delivers **35% higher SET
throughput** and **26% higher GET throughput** than Dragonfly under pipeline 10.
At pipeline 1 (no batching), both systems perform comparably at ~4.8–4.9M
ops/sec, with Dragonfly showing slightly tighter tail latency at p99 and p99.9.

Even with snapshots enabled every 10 seconds, FreakV maintains **10.7M SET
ops/sec** — still higher than Dragonfly's cache-mode peak of 10.1M.
