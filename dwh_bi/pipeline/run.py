"""
End-to-end orchestration: generate -> ingest -> normalize -> marts.

This is the whole warehouse in one function so it is easy to test and to read.
`run_pipeline` is idempotent: run it twice and the mart totals are identical,
because dedup + INSERT OR REPLACE make every stage exactly-once.
"""
from __future__ import annotations

import time

from warehouse import Warehouse, build_schema, ingest_batch, normalize_all
from bi.metrics import build_marts
from .generate import generate


def run_pipeline(
    wh: Warehouse,
    *,
    n: int = 100_000,
    seed: int = 42,
    days: int = 30,
    batch_size: int = 50_000,
    verbose: bool = True,
) -> dict:
    """Run the full pipeline and return a metrics summary."""
    t0 = time.time()
    build_schema(wh)

    # Micro-batch ingestion (mirrors streaming: many small appends).
    ingested, batch = 0, []
    for row in generate(n, seed=seed, days=days):
        batch.append(row)
        if len(batch) >= batch_size:
            ingest_batch(wh, batch, source="generator")
            ingested += len(batch)
            batch.clear()
    if batch:
        ingest_batch(wh, batch, source="generator")
        ingested += len(batch)
    t_ingest = time.time()

    summary = normalize_all(wh, strict=True)
    t_norm = time.time()

    mart_counts = build_marts(wh)
    t_done = time.time()

    result = {
        "rows_ingested": ingested,
        "ingest_sec": round(t_ingest - t0, 3),
        "normalize_sec": round(t_norm - t_ingest, 3),
        "marts_sec": round(t_done - t_norm, 3),
        "total_sec": round(t_done - t0, 3),
        "ingest_rows_per_sec": int(ingested / max(t_ingest - t0, 1e-6)),
        **summary,
        "marts": mart_counts,
    }
    if verbose:
        _print_summary(result)
    return result


def _print_summary(r: dict) -> None:
    print("\n=== pipeline complete ===")
    print(f"  ingested        : {r['rows_ingested']:,} raw rows "
          f"in {r['ingest_sec']}s  ({r['ingest_rows_per_sec']:,} rows/s)")
    print(f"  staged (deduped): {r['staged_rows']:,}")
    print(f"  fact rows       : {r['fact_rows']:,}")
    print(f"  dims            : {r['dim_customer']:,} cust / "
          f"{r['dim_merchant']:,} merch / {r['dim_currency']:,} ccy")
    print(f"  quality gate    : {'PASS' if not r['quality_violations'] else 'FAIL'}")
    print(f"  marts           : {', '.join(r['marts'])}")
    print(f"  total time      : {r['total_sec']}s")
