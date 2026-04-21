#ifndef MEM_BARRIER_H
#define MEM_BARRIER_H

/*
 * mem_barrier.h — Memory pressure detection via /proc/meminfo
 *
 * Provides a lightweight O(1) check of system MemAvailable to guide
 * mmap decisions with 4 pressure levels:
 *
 *   MEM_PRESSURE_NONE   — MemAvailable > 50% of MemTotal
 *   MEM_PRESSURE_LOW    — 20–50%
 *   MEM_PRESSURE_HIGH   — 5–20%
 *   MEM_PRESSURE_CRIT   — < 5%
 *
 * Usage:
 *   MemPressure p = mem_pressure_check();
 *   if (p >= MEM_PRESSURE_CRIT) { refuse mmap; }
 *
 * The check reads /proc/meminfo which is a kernel virtual file — no disk I/O.
 * Typical cost: ~1–2 µs.  Cache the result per-tick if needed.
 */

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* ── Pressure levels ────────────────────────────────────────────────── */

typedef enum {
  MEM_PRESSURE_NONE = 0, /* > 50% available — mmap freely              */
  MEM_PRESSURE_LOW = 1,  /* 20–50% — mmap OK, enable background evict  */
  MEM_PRESSURE_HIGH = 2, /* 5–20%  — evict first, mmap small if needed */
  MEM_PRESSURE_CRIT = 3, /* < 5%   — no mmap, reuse freed slots only   */
} MemPressure;

/* ── Snapshot of system memory state ────────────────────────────────── */

typedef struct {
  uint64_t mem_total_kb;     /* MemTotal from /proc/meminfo       */
  uint64_t mem_available_kb; /* MemAvailable from /proc/meminfo   */
  double available_ratio;    /* mem_available / mem_total          */
  MemPressure pressure;      /* derived pressure level            */
} MemInfo;

/* ── API ────────────────────────────────────────────────────────────── */

/* Read /proc/meminfo and return the current pressure level.
 * This is the fast-path — only reads two lines from procfs.          */
MemPressure mem_pressure_check(void);

/* Full snapshot — fills out all fields.  Slightly more expensive.     */
bool mem_pressure_snapshot(MemInfo *out);

/* Returns how many bytes can safely be mmap'd given current pressure:
 *   NONE → requested_bytes (full amount)
 *   LOW  → requested_bytes (full amount, but caller should evict bg)
 *   HIGH → min(requested_bytes, available * 0.5)
 *   CRIT → 0 (refuse)
 */
size_t mem_barrier_allowed_bytes(size_t requested_bytes);

/* Cache: update once per reactor tick to avoid repeated /proc reads.  */
void mem_pressure_refresh(void);
MemPressure mem_pressure_cached(void);
const MemInfo *mem_pressure_cached_info(void);

/* Human-readable name for logging */
const char *mem_pressure_name(MemPressure p);

#endif /* MEM_BARRIER_H */
