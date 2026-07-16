"""
End-to-end and unit tests — this is what backs the 'without errors' promise.

Run:  python -m pytest dwh_bi/tests -q      (or: python dwh_bi/tests/test_pipeline.py)
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from warehouse import Warehouse, build_schema, ingest_batch, normalize_all  # noqa: E402
from warehouse.normalize import assert_quality, DataQualityError            # noqa: E402
from bi.metrics import build_marts, query_metric                            # noqa: E402
from pipeline.generate import generate                                      # noqa: E402
from pipeline.run import run_pipeline                                       # noqa: E402


def fresh(tmp) -> Warehouse:
    wh = Warehouse(tmp)
    build_schema(wh)
    return wh


def test_end_to_end(tmp_path):
    with Warehouse(tmp_path / "e2e.duckdb") as wh:
        r = run_pipeline(wh, n=20_000, verbose=False)
    assert r["rows_ingested"] >= 20_000
    assert r["fact_rows"] > 0
    assert r["quality_violations"] == []          # clean run == no errors
    assert set(r["marts"]) == {
        "mart_daily_revenue", "mart_merchant_performance",
        "mart_currency_mix", "mart_channel_status", "mart_customer_360",
    }


def test_dedup_is_idempotent(tmp_path):
    """Duplicates in the feed must not double-count; re-running must not either."""
    with fresh(tmp_path / "dd.duckdb") as wh:
        rows = list(generate(5_000, seed=7))
        ingest_batch(wh, rows)
        ingest_batch(wh, rows)                     # ingest the SAME batch twice
        normalize_all(wh)
        fact1 = wh.count("core.fact_transaction")
        distinct = wh.one("SELECT count(DISTINCT transaction_id) FROM staging.transactions")
        assert fact1 == distinct                   # exactly-once at the fact grain
        normalize_all(wh)                          # re-normalize
        assert wh.count("core.fact_transaction") == fact1


def test_currency_normalization(tmp_path):
    """Mixed-currency inputs must reconcile to a single base currency."""
    with fresh(tmp_path / "ccy.duckdb") as wh:
        ingest_batch(wh, [
            {"transaction_id": "T1", "event_ts": "2026-06-01 10:00:00",
             "customer_id": "C1", "merchant_id": "M1", "merchant_name": "X",
             "merchant_category": "R", "account_id": "A1", "amount": "100",
             "currency": "USD", "fee": "1", "status": "settled",
             "country": "US", "channel": "web"},
            {"transaction_id": "T2", "event_ts": "2026-06-01 11:00:00",
             "customer_id": "C1", "merchant_id": "M1", "merchant_name": "X",
             "merchant_category": "R", "account_id": "A1", "amount": "100",
             "currency": "EUR", "fee": "1", "status": "settled",
             "country": "US", "channel": "web"},
        ])
        normalize_all(wh)
        # 100 USD -> 100 base; 100 EUR -> 108 base (rate 1.08).
        base = {r["currency"]: float(r["amount_base"]) for r in wh.records(
            "SELECT s.currency, f.amount_base FROM core.fact_transaction f "
            "JOIN staging.transactions s USING(transaction_id)")}
        assert base["USD"] == 100.0
        assert abs(base["EUR"] - 108.0) < 1e-6


def test_scd2_customer(tmp_path):
    """A customer changing country produces two dim rows, one current."""
    with fresh(tmp_path / "scd.duckdb") as wh:
        base = {"merchant_id": "M1", "merchant_name": "X", "merchant_category": "R",
                "account_id": "A1", "amount": "50", "currency": "USD", "fee": "0",
                "status": "settled", "channel": "web"}
        ingest_batch(wh, [{"transaction_id": "T1", "event_ts": "2026-06-01 10:00:00",
                           "customer_id": "C9", "country": "US", **base}])
        normalize_all(wh)
        ingest_batch(wh, [{"transaction_id": "T2", "event_ts": "2026-06-20 10:00:00",
                           "customer_id": "C9", "country": "CA", **base}])
        normalize_all(wh)
        versions = wh.records(
            "SELECT country, is_current FROM core.dim_customer WHERE customer_id='C9' ORDER BY valid_from")
        assert len(versions) == 2
        assert sum(1 for v in versions if v["is_current"]) == 1
        assert versions[-1]["country"] == "CA" and versions[-1]["is_current"]


def test_bad_data_is_rejected_not_silently_dropped(tmp_path):
    """Unparseable rows never reach the fact table; the gate stays green."""
    with fresh(tmp_path / "bad.duckdb") as wh:
        ingest_batch(wh, [
            {"transaction_id": "GOOD", "event_ts": "2026-06-01 10:00:00",
             "customer_id": "C1", "merchant_id": "M1", "merchant_name": "X",
             "merchant_category": "R", "account_id": "A1", "amount": "10",
             "currency": "USD", "fee": "0", "status": "settled",
             "country": "US", "channel": "web"},
            {"transaction_id": "BADTS", "event_ts": "not-a-date", "amount": "10",
             "currency": "USD"},                                   # bad timestamp
            {"transaction_id": "BADAMT", "event_ts": "2026-06-01 10:00:00",
             "amount": "abc", "currency": "USD"},                  # bad amount
            {"transaction_id": "", "event_ts": "2026-06-01 10:00:00",
             "amount": "10", "currency": "USD"},                   # missing id
        ])
        normalize_all(wh)
        ids = {r["transaction_id"] for r in wh.records(
            "SELECT transaction_id FROM core.fact_transaction")}
        assert ids == {"GOOD"}
        assert assert_quality(wh) == []


def test_metric_semantic_layer(tmp_path):
    with Warehouse(tmp_path / "m.duckdb") as wh:
        run_pipeline(wh, n=10_000, verbose=False)
        by_channel = query_metric(wh, "revenue", group_by="channel")
        assert by_channel and "revenue" in by_channel[0]
        total = query_metric(wh, "tx_count")[0]["tx_count"]
        assert total > 0


def test_quality_gate_raises_on_injected_violation(tmp_path):
    """Prove the gate actually fails loud — not just returns green by luck."""
    with fresh(tmp_path / "gate.duckdb") as wh:
        run_pipeline(wh, n=2_000, verbose=False)
        wh.con.execute(
            "INSERT INTO core.fact_transaction (transaction_id, amount_base, "
            "currency_key, status) VALUES ('NEG', -5, NULL, 'settled')")
        problems = assert_quality(wh)
        assert any("negative" in p for p in problems)
        assert any("currency" in p for p in problems)


if __name__ == "__main__":
    import tempfile, traceback
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    passed = 0
    for t in tests:
        with tempfile.TemporaryDirectory() as d:
            try:
                t(Path(d))
                print(f"PASS  {t.__name__}")
                passed += 1
            except Exception:
                print(f"FAIL  {t.__name__}")
                traceback.print_exc()
    print(f"\n{passed}/{len(tests)} passed")
    sys.exit(0 if passed == len(tests) else 1)
