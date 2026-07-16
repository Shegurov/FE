"""
Deterministic synthetic transaction generator.

Purposefully includes the mess a real feed has — duplicates, blank fields,
mixed currencies, occasional declines, a customer that changes country — so the
normalization layer has something real to clean. Deterministic via a fixed seed
so tests and benchmarks are reproducible.
"""
from __future__ import annotations

import random
from datetime import datetime, timedelta
from typing import Iterator

MERCHANTS = [
    ("M001", "Aurora Coffee", "FOOD"),
    ("M002", "Nimbus Cloud", "SAAS"),
    ("M003", "Orion Electronics", "RETAIL"),
    ("M004", "Vega Airlines", "TRAVEL"),
    ("M005", "Lyra Grocery", "FOOD"),
    ("M006", "Draco Fuel", "ENERGY"),
    ("M007", "Cygnus Books", "RETAIL"),
    ("M008", "Phoenix Health", "HEALTH"),
]
CURRENCIES = ["USD", "USD", "USD", "EUR", "EUR", "GBP", "JPY", "RUB", "CNY", "INR"]
CHANNELS = ["web", "mobile", "pos", "api"]
STATUSES = ["settled", "settled", "settled", "settled", "declined", "refunded"]
COUNTRIES = ["US", "GB", "DE", "FR", "JP", "RU", "CN", "IN"]


def generate(
    n: int,
    *,
    seed: int = 42,
    start: datetime | None = None,
    days: int = 30,
    dup_rate: float = 0.02,
    dirty_rate: float = 0.03,
) -> Iterator[dict]:
    """Yield ~n transaction dicts (a few extra from injected duplicates)."""
    rng = random.Random(seed)
    start = start or datetime(2026, 6, 1)
    span = timedelta(days=days)

    for i in range(n):
        cust_n = rng.randint(1, max(2, n // 20))  # ~20 tx per customer
        customer_id = f"C{cust_n:06d}"
        merchant_id, merchant_name, category = rng.choice(MERCHANTS)
        currency = rng.choice(CURRENCIES)
        ts = start + timedelta(seconds=rng.random() * span.total_seconds())
        amount = round(rng.lognormvariate(3.2, 1.0), 2)  # skewed, realistic
        # A single customer relocates mid-window -> exercises SCD-2.
        country = "CA" if (cust_n == 7 and ts.day > 15) else rng.choice(COUNTRIES)

        row = {
            "transaction_id": f"T{i:012d}",
            "event_ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "customer_id": customer_id,
            "merchant_id": merchant_id,
            "merchant_name": merchant_name,
            "merchant_category": category,
            "account_id": f"A{cust_n:06d}",
            "amount": amount,
            "currency": currency,
            "fee": round(amount * 0.015, 2),
            "status": rng.choice(STATUSES),
            "country": country,
            "channel": rng.choice(CHANNELS),
        }

        # Inject dirt so normalization has real work: blank optional fields,
        # whitespace, lowercase currency.
        if rng.random() < dirty_rate:
            f = rng.choice(["merchant_name", "channel", "country"])
            row[f] = " " if rng.random() < 0.5 else None
            if rng.random() < 0.3:
                row["currency"] = row["currency"].lower()

        yield row

        # Re-emit some rows as at-least-once duplicates.
        if rng.random() < dup_rate:
            yield dict(row)


def to_parquet(path: str, n: int, **kw) -> str:
    """Write a synthetic batch to Parquet (for the lake-ingest path)."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    rows = list(generate(n, **kw))
    cols = {k: [str(r.get(k)) if r.get(k) is not None else None for r in rows]
            for k in rows[0]}
    pq.write_table(pa.table(cols), path, compression="zstd")
    return path
