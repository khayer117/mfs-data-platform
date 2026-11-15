# Mini-MFS Transaction Data Platform — Version 2 Spec

**Version:** 2.0 (Enhancements)
**Base:** builds on [v1.0](mini-mfs-data-platform-spec.md) — all v1 components must be working before starting v2.

---

## Overview of changes

| Area | V1 | V2 |
|---|---|---|
| dbt models | Simple aggregations in `gold` schema | Full dimensional model: fact, dims, marts |
| Validation | 5 reject reasons | Adds `MISSING_REQUIRED_FIELD` |
| Testing | dbt schema tests only | Adds pytest unit tests for Spark batch job |
| Fraud batch rule | None | R3 pass-through (mule detection) |

---

## Enhancement 1 — Full dimensional model (dbt)

### 1.1 New folder layout

```
dbt/models/
  staging/
    sources.yml               (unchanged)
    stg_transactions.sql      (unchanged)
    schema.yml                (unchanged)
  intermediate/
    int_send_then_cashout.sql (R3 pass-through pattern)
  gold/
    daily_txn_summary.sql     (unchanged)
    wallet_activity.sql       (unchanged — rename to dim_wallet in v3)
    district_heatmap.sql      (unchanged)
    fct_transactions.sql      (NEW — incremental)
    dim_date.sql              (NEW)
    dim_wallet.sql            (NEW — replaces wallet_activity)
    dim_agent.sql             (NEW)
    mart_agent_liquidity.sql  (NEW — key MFS model)
    mart_fraud_summary.sql    (NEW)
    schema.yml                (extend with new models)
dbt/
  packages.yml                (NEW — adds dbt_utils)
tests/
  assert_fee_not_greater_than_amount.sql  (NEW — singular test)
```

### 1.2 New models

#### `fct_transactions` — incremental, `unique_key='txn_id'`

The central fact table. Incremental so only new data is processed each run; a 3-day lookback handles late-arriving rows.

```sql
{{ config(materialized='incremental', unique_key='txn_id') }}

SELECT
    txn_id,
    event_date,
    event_time,
    txn_type,
    sender_wallet,
    receiver_wallet,
    agent_id,
    merchant_id,
    amount,
    fee,
    channel,
    district,
    processed_at
FROM {{ ref('stg_transactions') }}

{% if is_incremental() %}
  WHERE processed_at > (SELECT MAX(processed_at) - INTERVAL '3 days' FROM {{ this }})
{% endif %}
```

**Why `unique_key`:** dbt merges on `txn_id` instead of appending, so reprocessing the lookback window never duplicates rows.

**Verification after build:**
```sql
SELECT
    (SELECT COUNT(*) FROM silver.transactions)   AS silver_rows,
    (SELECT COUNT(*) FROM gold.fct_transactions) AS fact_rows;
-- Both counts must be equal.
```

#### `dim_date`

Calendar table: one row per day from the earliest `event_date` in silver to today.

| Column | Type | Notes |
|---|---|---|
| `date_day` | DATE | Primary key |
| `day_of_week` | TEXT | Monday … Sunday |
| `day_of_week_num` | INT | 1 = Monday |
| `month_name` | TEXT | January … December |
| `month_num` | INT | 1–12 |
| `quarter` | INT | 1–4 |
| `year` | INT | |
| `is_weekend` | BOOLEAN | Bangladesh weekend: Friday + Saturday |

#### `dim_wallet`

One row per unique `sender_wallet`. Replaces `wallet_activity` with cleaner naming.

| Column | Type |
|---|---|
| `wallet_id` | TEXT (PK) |
| `first_txn_date` | DATE |
| `last_txn_date` | DATE |
| `home_district` | TEXT (most frequent district) |
| `total_txn_count` | INT |
| `total_sent` | NUMERIC |
| `distinct_txn_types` | INT |

#### `dim_agent`

One row per `agent_id`.

| Column | Type |
|---|---|
| `agent_id` | TEXT (PK) |
| `district` | TEXT |
| `first_active_date` | DATE |
| `last_active_date` | DATE |
| `total_cash_in` | NUMERIC |
| `total_cash_out` | NUMERIC |

#### `mart_agent_liquidity`

**The key MFS operations model.** One row per agent per day. Agents with large negative net cash positions need e-money top-up — this is a real daily operational question at bKash/Nagad.

| Column | Type | Description |
|---|---|---|
| `agent_id` | TEXT | |
| `event_date` | DATE | |
| `cash_in_total` | NUMERIC | Sum of CASH_IN amounts |
| `cash_out_total` | NUMERIC | Sum of CASH_OUT amounts |
| `net_cash_position` | NUMERIC | `cash_in_total − cash_out_total` |
| `txn_count` | INT | Total transactions |

```sql
SELECT
    agent_id,
    event_date,
    SUM(CASE WHEN txn_type = 'CASH_IN'  THEN amount ELSE 0 END) AS cash_in_total,
    SUM(CASE WHEN txn_type = 'CASH_OUT' THEN amount ELSE 0 END) AS cash_out_total,
    SUM(CASE WHEN txn_type = 'CASH_IN'  THEN amount ELSE 0 END)
  - SUM(CASE WHEN txn_type = 'CASH_OUT' THEN amount ELSE 0 END) AS net_cash_position,
    COUNT(*) AS txn_count
FROM {{ ref('fct_transactions') }}
WHERE agent_id IS NOT NULL
GROUP BY 1, 2
```

**Interview talking point:** agents with `net_cash_position < −50,000` on consecutive days are at risk of running out of physical cash or e-money float. A real operations team monitors this daily.

#### `mart_fraud_summary`

One row per day per rule. Connects streaming fraud alerts (from `silver.fraud_alerts`) with the batch R3 pass-through detections.

| Column | Type |
|---|---|
| `event_date` | DATE |
| `rule_id` | TEXT |
| `alert_count` | INT |
| `total_amount` | NUMERIC |
| `unique_wallets` | INT |

#### `int_send_then_cashout` — R3 pass-through (batch)

Detects mule wallets: receives SEND_MONEY then immediately cashes out ≥ 90% within 2 minutes.

```sql
SELECT
    r.receiver_wallet           AS wallet,
    r.txn_id                    AS received_txn_id,
    c.txn_id                    AS cashout_txn_id,
    r.amount                    AS received_amount,
    c.amount                    AS cashout_amount,
    r.event_time                AS received_at,
    c.event_time                AS cashed_out_at
FROM {{ ref('stg_transactions') }} r
JOIN {{ ref('stg_transactions') }} c
  ON  c.sender_wallet = r.receiver_wallet
 AND  c.txn_type      = 'CASH_OUT'
 AND  c.event_time    BETWEEN r.event_time AND r.event_time + INTERVAL '2 minutes'
 AND  c.amount        >= 0.9 * r.amount
WHERE r.txn_type = 'SEND_MONEY'
```

This feeds `mart_fraud_summary` (rule_id = `R3_PASS_THROUGH`).

### 1.3 dbt_utils package

Add to `dbt/packages.yml`:
```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.0.0", "<2.0.0"]
```

Run `dbt deps` once after adding.

Enables `dbt_utils.unique_combination_of_columns` test on `mart_agent_liquidity (agent_id, event_date)`.

### 1.4 Singular test

`dbt/tests/assert_fee_not_greater_than_amount.sql` — returns rows on failure:

```sql
SELECT txn_id, amount, fee
FROM {{ ref('stg_transactions') }}
WHERE fee > amount
```

### 1.5 Updated schema tests

| Model | Column | Test |
|---|---|---|
| `fct_transactions` | `txn_id` | `unique`, `not_null` |
| `fct_transactions` | `agent_id` | `relationships` → `dim_agent` (where not null) |
| `dim_wallet` | `wallet_id` | `unique`, `not_null` |
| `dim_agent` | `agent_id` | `unique`, `not_null` |
| `mart_agent_liquidity` | (`agent_id`, `event_date`) | `dbt_utils.unique_combination_of_columns` |
| singular | — | `assert_fee_not_greater_than_amount` |

---

## Enhancement 2 — `MISSING_REQUIRED_FIELD` validation

Add to `spark/jobs/bronze_to_silver.py` validation step:

| Check | `reject_reason` |
|---|---|
| `txn_type = CASH_OUT` or `CASH_IN` and `agent_id` is null | `MISSING_REQUIRED_FIELD` |
| `txn_type = MERCHANT_PAY` and `merchant_id` is null | `MISSING_REQUIRED_FIELD` |
| `txn_type = BILL_PAY` and `biller` is null | `MISSING_REQUIRED_FIELD` |

Place this check after `UNKNOWN_TXN_TYPE` in the validation chain.

---

## Enhancement 3 — pytest unit tests for bronze_to_silver

**File:** `spark/tests/test_bronze_to_silver.py`

Create a local `SparkSession` and test the core logic with small synthetic DataFrames.

| Test | Input | Expected |
|---|---|---|
| `test_deduplication` | Two rows with same `txn_id`, different offsets | 1 row in output (earlier offset kept) |
| `test_invalid_amount` | Row with `amount = -5` | Row in quarantine with `INVALID_AMOUNT` |
| `test_parse_error` | Row with `raw_value = "not-json"` | Row in quarantine with `PARSE_ERROR` |
| `test_late_event` | `event_time` 2 days before ingest | Row in silver with correct `event_date` (not ingest_date) |
| `test_missing_required_field` | CASH_OUT with null `agent_id` | Row in quarantine with `MISSING_REQUIRED_FIELD` |
| `test_idempotency` | Run validation twice on same input | Counts identical both times |

Run with:
```bash
python -m pytest spark/tests/test_bronze_to_silver.py -v
```

---

## Milestones and acceptance criteria

### V2 — Enhanced dbt + Testing

- [ ] `fct_transactions` row count equals `silver.transactions` row count
- [ ] `fct_transactions` is incremental — second run processes only the lookback window, not all rows
- [ ] `dim_wallet`, `dim_agent` unique key tests pass
- [ ] `mart_agent_liquidity` built — verify agents with negative net appear for days with more CASH_OUT than CASH_IN
- [ ] `mart_fraud_summary` shows R1/R2 alert counts matching `silver.fraud_alerts`
- [ ] Pass-through detections appear in `int_send_then_cashout`
- [ ] All 6 pytest unit tests pass
- [ ] `MISSING_REQUIRED_FIELD` records appear in quarantine for CASH_OUT rows missing `agent_id`
- [ ] dbt lineage graph screenshot added to README

---

## Data checks (SQL)

```sql
-- fct_transactions matches silver (must be equal)
SELECT
    (SELECT COUNT(*) FROM silver.transactions)   AS silver_rows,
    (SELECT COUNT(*) FROM gold.fct_transactions) AS fact_rows;

-- Agent liquidity: spot-check a negative net day
SELECT agent_id, event_date, cash_in_total, cash_out_total, net_cash_position
FROM gold.mart_agent_liquidity
WHERE net_cash_position < 0
ORDER BY net_cash_position ASC
LIMIT 10;

-- Pass-through detections
SELECT COUNT(*) FROM gold.int_send_then_cashout;

-- MISSING_REQUIRED_FIELD in quarantine
SELECT reject_reason, COUNT(*)
FROM parquet.`/data/lake/quarantine/transactions/*`
GROUP BY reject_reason;
```
