#!/usr/bin/env python3
"""Correlate stall.sh artifacts: probe.csv, sizes.csv, markers.txt.

Usage: stall_report.py <dir>
Prints: probe latency stats during load, spike timeline (tx > 1s) with
DB/WAL size + CPU at that moment, and whether /noop spiked too.
"""
import csv
import sys
from pathlib import Path

d = Path(sys.argv[1])
marks = {}
for line in (d / "markers.txt").read_text().splitlines():
    t, what = line.split(maxsplit=1)
    marks[what] = float(t)
load_start = marks["load_start"]
load_end = marks["load_end"]

probe = []
with open(d / "probe.csv") as f:
    for row in csv.reader(f):
        try:
            probe.append((float(row[0]), float(row[1]), float(row[2])))
        except (ValueError, IndexError):
            pass
during = [(t, n, x) for (t, n, x) in probe if load_start <= t <= load_end]

sizes = []
with open(d / "sizes.csv") as f:
    for row in csv.reader(f):
        try:
            sizes.append((float(row[0]), int(row[1]), int(row[2]), row[3]))
        except (ValueError, IndexError):
            pass


def nearest_size(t):
    if not sizes:
        return None
    return min(sizes, key=lambda r: abs(r[0] - t))


def pct(xs, p):
    xs = sorted(xs)
    if not xs:
        return float("nan")
    k = max(0, min(len(xs) - 1, int(round(p / 100 * (len(xs) - 1)))))
    return xs[k]


txs = [x for _, _, x in during if x >= 0]
noops = [n for _, n, _ in during if n >= 0]
print(f"probes during load: {len(during)}  (~{len(during)/(load_end-load_start):.1f}/s)")
print(f"probe tx :  p50={pct(txs,50):.3f}s  p90={pct(txs,90):.3f}s  p99={pct(txs,99):.3f}s  max={max(txs):.2f}s")
print(f"probe noop: p50={pct(noops,50):.3f}s  p90={pct(noops,90):.3f}s  p99={pct(noops,99):.3f}s  max={max(noops):.2f}s")

spikes = [(t, n, x) for (t, n, x) in during if x > 1.0]
print(f"\nspikes (probe tx > 1s): {len(spikes)}")
print(f"{'elapsed':>8}  {'tx_s':>8}  {'noop_s':>7}  {'db_MB':>8}  {'wal_MB':>7}  {'cpu%':>5}")
for t, n, x in spikes:
    s = nearest_size(t)
    db = s[1] / 1048576 if s else 0
    wal = s[2] / 1048576 if s else 0
    cpu = s[3] if s else "?"
    print(f"{t - load_start:8.1f}  {x:8.2f}  {n:7.3f}  {db:8.0f}  {wal:7.0f}  {cpu:>5}")

# WAL restarts: any 1s sample where wal shrank by >100MB vs previous sample
print("\nWAL restarts seen by monitor (wal drops >100MB between 1s samples):")
drops = 0
for i in range(1, len(sizes)):
    if sizes[i][2] + 100 * 1048576 < sizes[i - 1][2] and sizes[i][0] >= load_start:
        drops += 1
        print(f"  t+{sizes[i][0]-load_start:6.1f}s  {sizes[i-1][2]/1048576:7.0f}MB -> {sizes[i][2]/1048576:5.0f}MB  db={sizes[i][1]/1048576:.0f}MB cpu={sizes[i][3]}")
print(f"  total: {drops}")

print("\nDB growth / CPU over load (10s buckets):")
buckets = {}
for t, db, wal, cpu in sizes:
    if t < load_start:
        continue
    b = int((t - load_start) // 10) * 10
    buckets.setdefault(b, []).append((db, wal, cpu))
for b in sorted(buckets):
    rows = buckets[b]
    db = rows[-1][0] / 1048576
    wal = max(r[1] for r in rows) / 1048576
    cpus = [float(r[2]) for r in rows if r[2] not in ("?", "")]
    cpu = f"{sum(cpus)/len(cpus):5.0f}" if cpus else "    ?"
    print(f"  t+{b:3d}s  db={db:7.0f}MB  wal_peak={wal:6.0f}MB  cpu~{cpu}%")
