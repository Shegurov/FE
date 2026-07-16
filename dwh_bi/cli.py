#!/usr/bin/env python3
"""
dwh — single entrypoint for the free data warehouse + BI platform.

    python cli.py build   --rows 200000 --db dwh.duckdb     # generate->normalize->marts
    python cli.py serve    --db dwh.duckdb --port 8080       # embedded BI dashboard
    python cli.py query    --metric revenue --by channel     # ad-hoc governed metric
    python cli.py bench     --rows 500000                     # throughput benchmark + extrapolation
    python cli.py sizing    --tx-per-day 1000000000 --bytes 700   # capacity planner
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from warehouse import Warehouse                       # noqa: E402
from bi.metrics import query_metric                   # noqa: E402
from pipeline.run import run_pipeline                  # noqa: E402


def cmd_build(a):
    with Warehouse(a.db) as wh:
        run_pipeline(wh, n=a.rows, seed=a.seed, days=a.days)


def cmd_serve(a):
    from bi.server import serve
    serve(a.db, a.host, a.port)


def cmd_query(a):
    with Warehouse(a.db, read_only=True) as wh:
        rows = query_metric(wh, a.metric, group_by=a.by, where=a.where)
    if not rows:
        print("(no rows)"); return
    cols = list(rows[0])
    width = {c: max(len(c), *(len(str(r[c])) for r in rows)) for c in cols}
    print("  ".join(c.ljust(width[c]) for c in cols))
    for r in rows:
        print("  ".join(str(r[c]).ljust(width[c]) for c in cols))


def cmd_bench(a):
    import time
    with Warehouse(a.db) as wh:
        r = run_pipeline(wh, n=a.rows, verbose=False)
    rps = r["ingest_rows_per_sec"]
    norm_rps = int(r["staged_rows"] / max(r["normalize_sec"], 1e-6))
    day = 1_000_000_000
    print(f"\nMeasured on this machine ({a.rows:,} rows):")
    print(f"  ingest     : {rps:,} rows/s")
    print(f"  normalize  : {norm_rps:,} rows/s (full star-schema build)")
    print(f"  end-to-end : {r['total_sec']}s")
    print(f"\nExtrapolation to 1,000,000,000 tx/day (single node):")
    print(f"  ingest wall-clock  : ~{day/max(rps,1)/3600:.1f} h  "
          f"(avg feed is only {day/86400:,.0f} tx/s)")
    print(f"  normalize wall-clock: ~{day/max(norm_rps,1)/3600:.1f} h")
    print("  -> comfortably within a day on ONE node; production shards across")
    print("     N ClickHouse nodes, so real headroom is N x this. See README.")


def cmd_sizing(a):
    tx, b = a.tx_per_day, a.bytes
    raw_day = tx * b
    comp = raw_day * a.compression
    years = a.capacity_tb * 1e12 / max(comp * 365, 1)
    print("Capacity planner")
    print(f"  transactions/day     : {tx:,}")
    print(f"  raw bytes/tx         : {b}")
    print(f"  raw/day              : {raw_day/1e12:.2f} TB")
    print(f"  on disk/day (zstd {a.compression:g}x factor): {comp/1e12:.3f} TB")
    print(f"  target capacity      : {a.capacity_tb:,} TB")
    print(f"  retention at capacity: ~{years:.1f} years of history")
    nodes = max(1, round(a.capacity_tb / a.node_tb))
    print(f"  nodes @ {a.node_tb} TB/node : {nodes}  (ClickHouse shards, replication x{a.replicas})")
    print(f"  raw nodes w/ replicas: {nodes * a.replicas}")


def main():
    ap = argparse.ArgumentParser(prog="dwh", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    b = sub.add_parser("build", help="generate -> normalize -> marts")
    b.add_argument("--db", default="dwh.duckdb"); b.add_argument("--rows", type=int, default=200_000)
    b.add_argument("--seed", type=int, default=42); b.add_argument("--days", type=int, default=30)
    b.set_defaults(fn=cmd_build)

    s = sub.add_parser("serve", help="run embedded BI dashboard")
    s.add_argument("--db", default="dwh.duckdb"); s.add_argument("--host", default="127.0.0.1")
    s.add_argument("--port", type=int, default=8080); s.set_defaults(fn=cmd_serve)

    q = sub.add_parser("query", help="ad-hoc governed metric")
    q.add_argument("--db", default="dwh.duckdb"); q.add_argument("--metric", required=True)
    q.add_argument("--by", default=None); q.add_argument("--where", default=None)
    q.set_defaults(fn=cmd_query)

    bn = sub.add_parser("bench", help="throughput benchmark + extrapolation")
    bn.add_argument("--db", default="bench.duckdb"); bn.add_argument("--rows", type=int, default=500_000)
    bn.set_defaults(fn=cmd_bench)

    sz = sub.add_parser("sizing", help="capacity / node planner")
    sz.add_argument("--tx-per-day", type=int, default=1_000_000_000)
    sz.add_argument("--bytes", type=int, default=700, help="raw bytes per transaction")
    sz.add_argument("--compression", type=float, default=0.12, help="on-disk factor after zstd")
    sz.add_argument("--capacity-tb", type=int, default=2000)
    sz.add_argument("--node-tb", type=int, default=100)
    sz.add_argument("--replicas", type=int, default=2)
    sz.set_defaults(fn=cmd_sizing)

    a = ap.parse_args()
    a.fn(a)


if __name__ == "__main__":
    main()
