-- ============================================================================
-- Production scale-out schema for ClickHouse (Apache 2.0, free).
--
-- This is the SAME layered model as the local DuckDB build, expressed in
-- ClickHouse DDL and sized for 1e9 transactions/day and multi-PB retention.
-- The Python normalization SQL ports over almost verbatim; what changes here is
-- the physical design: MergeTree engines, partitioning, ordering keys, TTL and
-- tiered storage (hot SSD -> cold object storage), which is where the scale
-- actually comes from.
-- ============================================================================

CREATE DATABASE IF NOT EXISTS dwh;

-- ---------------------------------------------------------------------------
-- RAW landing. Fed by a Kafka/Redpanda engine table (see kafka_ingest.sql).
-- Ordered by ingest time; partitioned by day so old partitions drop cheaply.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.raw_transactions
(
    transaction_id     String,
    event_ts           String,
    customer_id        String,
    merchant_id        String,
    merchant_name      String,
    merchant_category  String,
    account_id         String,
    amount             String,
    currency           String,
    fee                String,
    status             String,
    country            String,
    channel            String,
    _ingested_at       DateTime DEFAULT now(),
    _source            LowCardinality(String),
    _batch_id          String
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(_ingested_at)
ORDER BY (_ingested_at, transaction_id)
TTL _ingested_at + INTERVAL 7 DAY;         -- raw is transient; keep 7 days

-- ---------------------------------------------------------------------------
-- FACT. ReplacingMergeTree gives free dedup on transaction_id (keeps the row
-- with the max _version), so at-least-once delivery is handled by the engine.
-- Ordering key drives both partition pruning and the columnar min/max index.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.fact_transaction
(
    transaction_id  String,
    event_date      Date,
    event_ts        DateTime,
    date_key        UInt32,
    customer_key    UInt64,
    merchant_key    UInt32,
    account_key     UInt64,
    currency_key    UInt16,
    amount          Decimal(18, 2),
    amount_base     Decimal(18, 2),
    fee             Decimal(18, 2),
    fee_base        Decimal(18, 2),
    status          LowCardinality(String),
    channel         LowCardinality(String),
    _version        DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_version)
PARTITION BY toYYYYMM(event_date)          -- monthly parts: ~30 x 1e9 rows each
ORDER BY (event_date, merchant_key, customer_key, transaction_id)
-- Tiered storage: recent data on fast disks, older data flows to object store.
TTL event_date + INTERVAL 90 DAY TO VOLUME 'cold',
    event_date + INTERVAL 3650 DAY DELETE
SETTINGS storage_policy = 'hot_cold', index_granularity = 8192;

-- ---------------------------------------------------------------------------
-- Conformed dimensions (small; fully replicated to every shard as needed).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.dim_customer
(
    customer_key UInt64, customer_id String, country LowCardinality(String),
    valid_from DateTime, valid_to DateTime, is_current UInt8
) ENGINE = ReplacingMergeTree ORDER BY (customer_id, valid_from);

CREATE TABLE IF NOT EXISTS dwh.dim_merchant
(
    merchant_key UInt32, merchant_id String,
    merchant_name String, category LowCardinality(String)
) ENGINE = ReplacingMergeTree ORDER BY merchant_id;

CREATE TABLE IF NOT EXISTS dwh.dim_currency
(
    currency_key UInt16, currency_code LowCardinality(String),
    rate_to_base Decimal(18, 6)
) ENGINE = ReplacingMergeTree ORDER BY currency_code;

-- ---------------------------------------------------------------------------
-- BI mart as an incrementally-maintained materialized view: the daily rollup
-- is updated as facts land, so the dashboard reads a tiny pre-aggregated table
-- (AggregatingMergeTree) instead of scanning a billion rows per page load.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.mart_daily_revenue
(
    event_date Date,
    tx_count            AggregateFunction(count),
    revenue             AggregateFunction(sum, Decimal(18, 2)),
    active_customers    AggregateFunction(uniq, UInt64)
) ENGINE = AggregatingMergeTree ORDER BY event_date;

CREATE MATERIALIZED VIEW IF NOT EXISTS dwh.mv_daily_revenue
TO dwh.mart_daily_revenue AS
SELECT
    event_date,
    countState()                                                  AS tx_count,
    sumState(if(status = 'settled', amount_base, toDecimal64(0, 2))) AS revenue,
    uniqState(customer_key)                                       AS active_customers
FROM dwh.fact_transaction
GROUP BY event_date;
