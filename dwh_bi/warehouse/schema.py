"""
Layered warehouse schema (medallion + dimensional).

    raw_*      landing zone, one row per event as received, nothing thrown away
    stg_*      typed / cleaned / de-duplicated, still at event grain
    dim_* fact_*   normalized core: 3NF conformed dimensions + star-schema facts
    mart_*     query-ready aggregates for the BI layer

Partitioning: every large table is keyed on event_date. In production that is a
ClickHouse `PARTITION BY toYYYYMMDD(event_ts)` / a Hive-partitioned Parquet lake;
locally DuckDB keeps event_date as a first-class column so the exact same
predicates prune the exact same partitions.
"""
from __future__ import annotations

from .engine import Warehouse

# --- raw landing zone -------------------------------------------------------
RAW_DDL = """
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.transactions (
    transaction_id   VARCHAR,
    event_ts         VARCHAR,        -- as-received, unparsed on purpose
    customer_id      VARCHAR,
    merchant_id      VARCHAR,
    merchant_name    VARCHAR,
    merchant_category VARCHAR,
    account_id       VARCHAR,
    amount           VARCHAR,
    currency         VARCHAR,
    fee              VARCHAR,
    status           VARCHAR,
    country          VARCHAR,
    channel          VARCHAR,
    -- ingestion lineage
    _ingested_at     TIMESTAMP DEFAULT now(),
    _source          VARCHAR,
    _batch_id        VARCHAR
);
"""

# --- staging (typed, clean, deduped) ---------------------------------------
STAGING_DDL = """
CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.transactions (
    transaction_id    VARCHAR PRIMARY KEY,
    event_ts          TIMESTAMP,
    event_date        DATE,
    customer_id       VARCHAR,
    merchant_id       VARCHAR,
    merchant_name     VARCHAR,
    merchant_category VARCHAR,
    account_id        VARCHAR,
    amount            DECIMAL(18,2),
    currency          VARCHAR,
    fee               DECIMAL(18,2),
    status            VARCHAR,
    country           VARCHAR,
    channel           VARCHAR,
    _ingested_at      TIMESTAMP,
    _batch_id         VARCHAR
);
"""

# --- normalized core: conformed dimensions + fact --------------------------
CORE_DDL = """
CREATE SCHEMA IF NOT EXISTS core;

-- Date dimension (generated, conformed across all facts).
CREATE TABLE IF NOT EXISTS core.dim_date (
    date_key     INTEGER PRIMARY KEY,   -- yyyymmdd
    date         DATE,
    year         SMALLINT,
    quarter      TINYINT,
    month        TINYINT,
    day          TINYINT,
    day_of_week  TINYINT,
    is_weekend   BOOLEAN
);

CREATE TABLE IF NOT EXISTS core.dim_currency (
    currency_key   INTEGER PRIMARY KEY,
    currency_code  VARCHAR UNIQUE,
    rate_to_base   DECIMAL(18,6)         -- 1 unit -> base currency (USD)
);

CREATE TABLE IF NOT EXISTS core.dim_merchant (
    merchant_key   INTEGER PRIMARY KEY,
    merchant_id    VARCHAR UNIQUE,
    merchant_name  VARCHAR,
    category       VARCHAR
);

-- Customer dimension is SCD Type 2: history is preserved, one current row.
CREATE TABLE IF NOT EXISTS core.dim_customer (
    customer_key   INTEGER PRIMARY KEY,
    customer_id    VARCHAR,
    country        VARCHAR,
    valid_from     TIMESTAMP,
    valid_to       TIMESTAMP,
    is_current     BOOLEAN
);

CREATE TABLE IF NOT EXISTS core.dim_account (
    account_key    INTEGER PRIMARY KEY,
    account_id     VARCHAR UNIQUE,
    customer_id    VARCHAR
);

-- The fact: one row per transaction, all measures in base currency too.
CREATE TABLE IF NOT EXISTS core.fact_transaction (
    transaction_id  VARCHAR PRIMARY KEY,
    date_key        INTEGER,
    event_ts        TIMESTAMP,
    event_date      DATE,
    customer_key    INTEGER,
    merchant_key    INTEGER,
    account_key     INTEGER,
    currency_key    INTEGER,
    amount          DECIMAL(18,2),
    amount_base     DECIMAL(18,2),
    fee             DECIMAL(18,2),
    fee_base        DECIMAL(18,2),
    status          VARCHAR,
    channel         VARCHAR
);
"""

# --- marts (BI-facing) ------------------------------------------------------
MART_DDL = """
CREATE SCHEMA IF NOT EXISTS mart;
"""


def build_schema(wh: Warehouse) -> None:
    """Create every layer if it does not already exist (idempotent)."""
    for ddl in (RAW_DDL, STAGING_DDL, CORE_DDL, MART_DDL):
        wh.script(ddl)
