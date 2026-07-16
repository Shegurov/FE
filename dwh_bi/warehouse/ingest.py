"""
Ingestion.

Landing is deliberately dumb and fast: append rows to raw.transactions with
lineage columns, no parsing, no validation. That is what lets the ingest path
sustain high throughput — all the expensive work happens later, in set-based
normalization, off the hot path.

Two entry points:
    ingest_batch()   in-process append of an iterable of dict rows (used by the
                     synthetic generator and by micro-batch stream consumers).
    ingest_parquet() zero-copy load of a Parquet file/glob from the lake tier.

In production the same raw table is a ClickHouse `MergeTree` fed by a Kafka
engine table (or Redpanda); the contract — append raw, normalize downstream —
is identical.
"""
from __future__ import annotations

import uuid
from typing import Iterable, Mapping

import pyarrow as pa

from .engine import Warehouse

RAW_COLUMNS = [
    "transaction_id", "event_ts", "customer_id", "merchant_id", "merchant_name",
    "merchant_category", "account_id", "amount", "currency", "fee", "status",
    "country", "channel",
]


def ingest_batch(
    wh: Warehouse,
    rows: Iterable[Mapping],
    *,
    source: str = "batch",
    batch_id: str | None = None,
) -> str:
    """Append rows to the raw landing table. Returns the batch id.

    Rows are loaded through an Arrow table registered as a view, so this is a
    single vectorized INSERT regardless of batch size — no per-row round trips.
    """
    batch_id = batch_id or uuid.uuid4().hex
    cols = {c: [] for c in RAW_COLUMNS}
    for r in rows:
        for c in RAW_COLUMNS:
            v = r.get(c)
            cols[c].append(None if v is None else str(v))
    if not cols["transaction_id"]:
        return batch_id

    tbl = pa.table({c: pa.array(cols[c], type=pa.string()) for c in RAW_COLUMNS})
    wh.con.register("_incoming", tbl)
    try:
        wh.con.execute(
            f"""
            INSERT INTO raw.transactions
                ({", ".join(RAW_COLUMNS)}, _source, _batch_id)
            SELECT {", ".join(RAW_COLUMNS)}, '{source}', '{batch_id}'
            FROM _incoming
            """
        )
    finally:
        wh.con.unregister("_incoming")
    return batch_id


def ingest_parquet(wh: Warehouse, path: str, *, source: str = "lake") -> str:
    """Load a Parquet file or glob (e.g. 's3://.../date=*/*.parquet') into raw."""
    batch_id = uuid.uuid4().hex
    present = wh.rows(
        f"SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet('{path}'))"
    )
    present_cols = {r[0] for r in present}
    projected = ", ".join(
        f"CAST({c} AS VARCHAR) AS {c}" if c in present_cols else f"NULL AS {c}"
        for c in RAW_COLUMNS
    )
    wh.con.execute(
        f"""
        INSERT INTO raw.transactions ({", ".join(RAW_COLUMNS)}, _source, _batch_id)
        SELECT {projected}, '{source}', '{batch_id}'
        FROM read_parquet('{path}')
        """
    )
    return batch_id
