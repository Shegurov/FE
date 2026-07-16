"""
Embedded BI semantic layer + marts.

`MARTS` are the query-ready aggregate tables the dashboard reads. They are built
from the star schema with plain GROUP BYs — cheap to refresh, trivial to port.

`METRICS` is a small semantic layer: named, governed metric definitions so the
dashboard and any ad-hoc query agree on what "revenue" means (settled base-
currency amount) instead of every chart re-deriving it.
"""
from __future__ import annotations

from warehouse.engine import Warehouse

# Marts: name -> SELECT that (re)builds it from core.*
MARTS: dict[str, str] = {
    # Daily revenue / volume trend — the headline time series.
    "mart_daily_revenue": """
        SELECT
            f.event_date,
            d.year, d.month, d.is_weekend,
            count(*)                                          AS tx_count,
            count(DISTINCT f.customer_key)                    AS active_customers,
            sum(f.amount_base)                                AS gross_amount,
            sum(CASE WHEN f.status = 'settled' THEN f.amount_base ELSE 0 END) AS revenue,
            sum(f.fee_base)                                   AS fees,
            avg(f.amount_base)                                AS avg_ticket
        FROM core.fact_transaction f
        JOIN core.dim_date d ON d.date_key = f.date_key
        GROUP BY f.event_date, d.year, d.month, d.is_weekend
    """,
    # Merchant performance leaderboard.
    "mart_merchant_performance": """
        SELECT
            m.merchant_id, m.merchant_name, m.category,
            count(*)                                          AS tx_count,
            sum(f.amount_base)                                AS gross_amount,
            sum(CASE WHEN f.status = 'settled' THEN f.amount_base ELSE 0 END) AS revenue,
            sum(f.fee_base)                                   AS fees,
            round(100.0 * sum(CASE WHEN f.status = 'declined' THEN 1 ELSE 0 END)
                  / count(*), 2)                              AS decline_rate_pct
        FROM core.fact_transaction f
        JOIN core.dim_merchant m ON m.merchant_key = f.merchant_key
        GROUP BY m.merchant_id, m.merchant_name, m.category
    """,
    # Revenue split by currency (shows the base-currency normalization working).
    "mart_currency_mix": """
        SELECT
            c.currency_code,
            count(*)                  AS tx_count,
            sum(f.amount)             AS gross_native,
            sum(f.amount_base)        AS gross_base
        FROM core.fact_transaction f
        JOIN core.dim_currency c ON c.currency_key = f.currency_key
        GROUP BY c.currency_code
    """,
    # Channel + status breakdown for operational monitoring.
    "mart_channel_status": """
        SELECT
            f.channel, f.status,
            count(*)            AS tx_count,
            sum(f.amount_base)  AS gross_amount
        FROM core.fact_transaction f
        GROUP BY f.channel, f.status
    """,
    # Customer 360 — one row per customer, current attributes + lifetime value.
    "mart_customer_360": """
        SELECT
            cust.customer_id,
            cust.country,
            count(*)                        AS tx_count,
            sum(f.amount_base)              AS lifetime_value,
            min(f.event_date)               AS first_seen,
            max(f.event_date)               AS last_seen
        FROM core.fact_transaction f
        JOIN core.dim_customer cust ON cust.customer_key = f.customer_key
        GROUP BY cust.customer_id, cust.country
    """,
}

# Governed metric definitions for ad-hoc use (name -> SQL expression over fact).
METRICS: dict[str, str] = {
    "revenue": "sum(CASE WHEN status = 'settled' THEN amount_base ELSE 0 END)",
    "gross_amount": "sum(amount_base)",
    "tx_count": "count(*)",
    "avg_ticket": "avg(amount_base)",
    "active_customers": "count(DISTINCT customer_key)",
    "fees": "sum(fee_base)",
    "decline_rate_pct":
        "round(100.0 * sum(CASE WHEN status='declined' THEN 1 ELSE 0 END)/count(*), 2)",
}


def build_marts(wh: Warehouse) -> dict[str, int]:
    """(Re)build every mart as a table. Idempotent — CREATE OR REPLACE."""
    counts = {}
    for name, select in MARTS.items():
        wh.con.execute(f"CREATE OR REPLACE TABLE mart.{name} AS {select}")
        counts[name] = wh.count(f"mart.{name}")
    return counts


def query_metric(
    wh: Warehouse,
    metric: str,
    *,
    group_by: str | None = None,
    where: str | None = None,
) -> list[dict]:
    """Evaluate a governed metric against the fact table, optionally grouped."""
    if metric not in METRICS:
        raise KeyError(f"unknown metric {metric!r}; known: {sorted(METRICS)}")
    expr = METRICS[metric]
    sel = f"{group_by + ', ' if group_by else ''}{expr} AS {metric}"
    sql = f"SELECT {sel} FROM core.fact_transaction"
    if where:
        sql += f" WHERE {where}"
    if group_by:
        sql += f" GROUP BY {group_by} ORDER BY {metric} DESC"
    return wh.records(sql)
