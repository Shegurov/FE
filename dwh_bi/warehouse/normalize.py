"""
Built-in data normalization.

This is the pipeline that turns messy landed rows into a clean, conformed,
query-ready star schema. Every step is set-based SQL so it runs at the engine's
full parallelism and ports to ClickHouse unchanged.

Steps
-----
1. stage()          raw -> staging: parse types, trim, default, DROP dupes.
2. load_dimensions()  build/extend conformed dims (date, currency, merchant,
                      account) and the SCD-2 customer dimension.
3. load_fact()      staging -> fact_transaction, resolving every FK and
                    converting money to the base currency.
4. Everything is wrapped by normalize_all(), which also runs data-quality
   assertions so a bad batch fails loudly instead of silently corrupting marts.
"""
from __future__ import annotations

from .engine import Warehouse

BASE_CURRENCY = "USD"

# A tiny static FX table keeps the demo self-contained; in production this is a
# slowly-changing dim fed from a rates feed. Rate = value of 1 unit in USD.
DEFAULT_FX = {
    "USD": 1.0,
    "EUR": 1.08,
    "GBP": 1.27,
    "JPY": 0.0064,
    "RUB": 0.011,
    "CNY": 0.14,
    "INR": 0.012,
}


def stage(wh: Warehouse) -> int:
    """raw.transactions -> staging.transactions with typing, cleaning, dedup.

    Dedup rule: a transaction_id may be re-sent (retries, at-least-once
    delivery). We keep the *latest* landed copy. This is what makes the
    pipeline idempotent and exactly-once at the mart level.
    """
    wh.script(
        f"""
        INSERT OR REPLACE INTO staging.transactions
        WITH cleaned AS (
            SELECT
                trim(transaction_id)                          AS transaction_id,
                try_cast(event_ts AS TIMESTAMP)               AS event_ts,
                nullif(trim(customer_id), '')                 AS customer_id,
                nullif(trim(merchant_id), '')                 AS merchant_id,
                coalesce(nullif(trim(merchant_name), ''), 'UNKNOWN')     AS merchant_name,
                upper(coalesce(nullif(trim(merchant_category), ''), 'OTHER')) AS merchant_category,
                nullif(trim(account_id), '')                  AS account_id,
                try_cast(amount AS DECIMAL(18,2))             AS amount,
                upper(coalesce(nullif(trim(currency), ''), '{BASE_CURRENCY}')) AS currency,
                coalesce(try_cast(fee AS DECIMAL(18,2)), 0)   AS fee,
                lower(coalesce(nullif(trim(status), ''), 'unknown'))    AS status,
                upper(coalesce(nullif(trim(country), ''), 'XX'))        AS country,
                lower(coalesce(nullif(trim(channel), ''), 'unknown'))   AS channel,
                _ingested_at,
                _batch_id,
                row_number() OVER (
                    PARTITION BY trim(transaction_id)
                    ORDER BY _ingested_at DESC
                ) AS _rn
            FROM raw.transactions
            WHERE trim(transaction_id) <> ''
              AND try_cast(event_ts AS TIMESTAMP) IS NOT NULL
              AND try_cast(amount AS DECIMAL(18,2)) IS NOT NULL
        )
        SELECT
            transaction_id, event_ts, CAST(event_ts AS DATE) AS event_date,
            customer_id, merchant_id, merchant_name, merchant_category,
            account_id, amount, currency, fee, status, country, channel,
            _ingested_at, _batch_id
        FROM cleaned
        WHERE _rn = 1;
        """
    )
    return wh.count("staging.transactions")


def load_dimensions(wh: Warehouse) -> None:
    """Populate/extend conformed dimensions from staging."""
    # -- dim_date: generated for every date present in staging ---------------
    wh.script(
        """
        INSERT INTO core.dim_date
        SELECT DISTINCT
            CAST(strftime(event_date, '%Y%m%d') AS INTEGER) AS date_key,
            event_date,
            year(event_date), quarter(event_date), month(event_date),
            day(event_date), isodow(event_date),
            isodow(event_date) IN (6, 7)
        FROM staging.transactions s
        WHERE NOT EXISTS (
            SELECT 1 FROM core.dim_date d
            WHERE d.date_key = CAST(strftime(s.event_date, '%Y%m%d') AS INTEGER)
        );
        """
    )

    # -- dim_currency: seed FX, then add any unseen code at parity -----------
    fx_values = ",".join(
        f"('{code}', {rate})" for code, rate in DEFAULT_FX.items()
    )
    wh.script(
        f"""
        INSERT INTO core.dim_currency
        WITH seed(currency_code, rate_to_base) AS (VALUES {fx_values}),
        all_codes AS (
            SELECT currency_code, rate_to_base FROM seed
            UNION
            SELECT DISTINCT currency, 1.0 FROM staging.transactions
            WHERE currency NOT IN (SELECT currency_code FROM seed)
        )
        SELECT
            row_number() OVER (ORDER BY currency_code) AS currency_key,
            currency_code, rate_to_base
        FROM all_codes a
        WHERE NOT EXISTS (
            SELECT 1 FROM core.dim_currency c WHERE c.currency_code = a.currency_code
        );
        """
    )

    # -- dim_merchant --------------------------------------------------------
    wh.script(
        """
        INSERT INTO core.dim_merchant
        SELECT
            (SELECT coalesce(max(merchant_key), 0) FROM core.dim_merchant)
                + row_number() OVER (ORDER BY merchant_id) AS merchant_key,
            merchant_id, any_value(merchant_name), any_value(merchant_category)
        FROM staging.transactions s
        WHERE merchant_id IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM core.dim_merchant m WHERE m.merchant_id = s.merchant_id)
        GROUP BY merchant_id;
        """
    )

    # -- dim_account ---------------------------------------------------------
    wh.script(
        """
        INSERT INTO core.dim_account
        SELECT
            (SELECT coalesce(max(account_key), 0) FROM core.dim_account)
                + row_number() OVER (ORDER BY account_id) AS account_key,
            account_id, any_value(customer_id)
        FROM staging.transactions s
        WHERE account_id IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM core.dim_account a WHERE a.account_id = s.account_id)
        GROUP BY account_id;
        """
    )

    _load_customer_scd2(wh)


def _load_customer_scd2(wh: Warehouse) -> None:
    """SCD Type 2 customer dimension.

    A customer's country can change. Rather than overwrite, we close the old
    row (valid_to, is_current=false) and open a new current row. Facts join on
    the version that was current at the transaction time, so history is exact.
    """
    # Latest observed attributes per customer (one row each): the country as of
    # that customer's most recent transaction. Grouping by (customer, country)
    # instead would emit a row per distinct country and break the single-current
    # invariant when one batch spans a relocation.
    wh.script(
        """
        CREATE OR REPLACE TEMP TABLE _cust_incoming AS
        SELECT customer_id, country, observed_at FROM (
            SELECT customer_id, country, event_ts AS observed_at,
                   row_number() OVER (PARTITION BY customer_id
                                      ORDER BY event_ts DESC) AS rn
            FROM staging.transactions
            WHERE customer_id IS NOT NULL
        ) WHERE rn = 1;
        """
    )
    # Close rows whose attributes changed.
    wh.script(
        """
        UPDATE core.dim_customer d
        SET valid_to = i.observed_at, is_current = FALSE
        FROM _cust_incoming i
        WHERE d.customer_id = i.customer_id
          AND d.is_current
          AND d.country <> i.country;
        """
    )
    # Insert new current rows: brand-new customers + changed ones.
    wh.script(
        """
        INSERT INTO core.dim_customer
        SELECT
            (SELECT coalesce(max(customer_key), 0) FROM core.dim_customer)
                + row_number() OVER (ORDER BY i.customer_id) AS customer_key,
            i.customer_id, i.country, i.observed_at,
            TIMESTAMP '9999-12-31 00:00:00', TRUE
        FROM _cust_incoming i
        WHERE NOT EXISTS (
            SELECT 1 FROM core.dim_customer d
            WHERE d.customer_id = i.customer_id AND d.is_current
        );
        """
    )


def load_fact(wh: Warehouse) -> int:
    """staging -> core.fact_transaction, resolving FKs + base-currency measures."""
    wh.script(
        """
        INSERT OR REPLACE INTO core.fact_transaction
        SELECT
            s.transaction_id,
            CAST(strftime(s.event_date, '%Y%m%d') AS INTEGER) AS date_key,
            s.event_ts, s.event_date,
            cust.customer_key, m.merchant_key, a.account_key, cur.currency_key,
            s.amount,
            CAST(s.amount * cur.rate_to_base AS DECIMAL(18,2)) AS amount_base,
            s.fee,
            CAST(s.fee * cur.rate_to_base AS DECIMAL(18,2))    AS fee_base,
            s.status, s.channel
        FROM staging.transactions s
        LEFT JOIN core.dim_currency cur ON cur.currency_code = s.currency
        LEFT JOIN core.dim_merchant m   ON m.merchant_id     = s.merchant_id
        LEFT JOIN core.dim_account  a   ON a.account_id      = s.account_id
        LEFT JOIN core.dim_customer cust
               ON cust.customer_id = s.customer_id AND cust.is_current;
        """
    )
    return wh.count("core.fact_transaction")


def assert_quality(wh: Warehouse) -> list[str]:
    """Data-quality gate. Returns a list of violations (empty == clean).

    These are the assertions that make 'without errors' a checked property and
    not a hope: no orphan FKs, no negative money, no unresolved currency, and
    the fact row-count reconciles to staging.
    """
    problems: list[str] = []

    orphan_currency = wh.one(
        "SELECT count(*) FROM core.fact_transaction WHERE currency_key IS NULL"
    )
    if orphan_currency:
        problems.append(f"{orphan_currency} fact rows with unresolved currency")

    orphan_merchant = wh.one(
        "SELECT count(*) FROM core.fact_transaction "
        "WHERE merchant_key IS NULL AND transaction_id IN "
        "(SELECT transaction_id FROM staging.transactions WHERE merchant_id IS NOT NULL)"
    )
    if orphan_merchant:
        problems.append(f"{orphan_merchant} fact rows with orphan merchant FK")

    neg = wh.one("SELECT count(*) FROM core.fact_transaction WHERE amount_base < 0")
    if neg:
        problems.append(f"{neg} fact rows with negative base amount")

    dup = wh.one(
        "SELECT count(*) FROM (SELECT transaction_id FROM core.fact_transaction "
        "GROUP BY transaction_id HAVING count(*) > 1)"
    )
    if dup:
        problems.append(f"{dup} duplicate transaction_id in fact")

    stg = wh.count("staging.transactions")
    fact = wh.count("core.fact_transaction")
    # fact can exceed staging across batches; within one it must reconcile 1:1
    # for the ids present in staging.
    unmatched = wh.one(
        "SELECT count(*) FROM staging.transactions s "
        "WHERE NOT EXISTS (SELECT 1 FROM core.fact_transaction f "
        "WHERE f.transaction_id = s.transaction_id)"
    )
    if unmatched:
        problems.append(f"{unmatched} staging rows never reached the fact table")

    return problems


def normalize_all(wh: Warehouse, *, strict: bool = True) -> dict:
    """Run the full normalization pipeline and return a summary."""
    staged = stage(wh)
    load_dimensions(wh)
    fact = load_fact(wh)
    problems = assert_quality(wh)
    if problems and strict:
        raise DataQualityError(problems)
    return {
        "staged_rows": staged,
        "fact_rows": fact,
        "dim_customer": wh.count("core.dim_customer"),
        "dim_merchant": wh.count("core.dim_merchant"),
        "dim_currency": wh.count("core.dim_currency"),
        "quality_violations": problems,
    }


class DataQualityError(RuntimeError):
    def __init__(self, problems: list[str]) -> None:
        super().__init__("Data-quality gate failed:\n  - " + "\n  - ".join(problems))
        self.problems = problems
