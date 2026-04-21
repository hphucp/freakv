#include "mem_barrier.h"

#include <stdio.h>
#include <string.h>

/* ── Cached state (one per process — updated by reactor tick) ──────── */

static MemInfo g_cached_info = {0};

/* ── /proc/meminfo parser ─────────────────────────────────────────── */

static bool parse_meminfo(MemInfo *out) {
  FILE *f = fopen("/proc/meminfo", "r");
  if (!f)
    return false;

  char line[256];
  bool got_total = false, got_avail = false;

  while (fgets(line, sizeof(line), f)) {
    if (!got_total && strncmp(line, "MemTotal:", 9) == 0) {
      sscanf(line + 9, " %lu", &out->mem_total_kb);
      got_total = true;
    } else if (!got_avail && strncmp(line, "MemAvailable:", 13) == 0) {
      sscanf(line + 13, " %lu", &out->mem_available_kb);
      got_avail = true;
    }
    if (got_total && got_avail)
      break;
  }

  fclose(f);

  if (!got_total || !got_avail || out->mem_total_kb == 0)
    return false;

  out->available_ratio =
      (double)out->mem_available_kb / (double)out->mem_total_kb;

  if (out->available_ratio > 0.50)
    out->pressure = MEM_PRESSURE_NONE;
  else if (out->available_ratio > 0.20)
    out->pressure = MEM_PRESSURE_LOW;
  else if (out->available_ratio > 0.05)
    out->pressure = MEM_PRESSURE_HIGH;
  else
    out->pressure = MEM_PRESSURE_CRIT;

  return true;
}

/* ── Public API ───────────────────────────────────────────────────── */

MemPressure mem_pressure_check(void) {
  MemInfo info;
  if (!parse_meminfo(&info))
    return MEM_PRESSURE_CRIT; /* fail-safe */
  return info.pressure;
}

bool mem_pressure_snapshot(MemInfo *out) { return parse_meminfo(out); }

size_t mem_barrier_allowed_bytes(size_t requested_bytes) {
  MemInfo info;
  if (!parse_meminfo(&info))
    return 0; /* fail-safe: refuse */

  switch (info.pressure) {
  case MEM_PRESSURE_NONE:
  case MEM_PRESSURE_LOW:
    return requested_bytes;

  case MEM_PRESSURE_HIGH: {
    /* Allow at most half of what's available */
    size_t avail_bytes = info.mem_available_kb * 1024ULL;
    size_t cap = avail_bytes / 2;
    return requested_bytes < cap ? requested_bytes : cap;
  }

  case MEM_PRESSURE_CRIT:
    return 0; /* refuse — caller must reuse freed slots */
  }

  return 0;
}

void mem_pressure_refresh(void) { parse_meminfo(&g_cached_info); }

MemPressure mem_pressure_cached(void) { return g_cached_info.pressure; }

const MemInfo *mem_pressure_cached_info(void) { return &g_cached_info; }

const char *mem_pressure_name(MemPressure p) {
  switch (p) {
  case MEM_PRESSURE_NONE:
    return "NONE (>50%)";
  case MEM_PRESSURE_LOW:
    return "LOW (20-50%)";
  case MEM_PRESSURE_HIGH:
    return "HIGH (5-20%)";
  case MEM_PRESSURE_CRIT:
    return "CRITICAL (<5%)";
  }
  return "UNKNOWN";
}
