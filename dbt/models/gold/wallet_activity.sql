SELECT
    sender_wallet                       AS wallet_id,
    COUNT(*)                            AS txn_sent_count,
    SUM(amount)                         AS total_sent,
    SUM(fee)                            AS total_fee_paid,
    ROUND(AVG(amount)::NUMERIC, 2)      AS avg_txn_amount,
    MIN(event_date)                     AS first_txn_date,
    MAX(event_date)                     AS last_txn_date,
    COUNT(DISTINCT txn_type)            AS distinct_txn_types
FROM {{ ref('stg_transactions') }}
GROUP BY 1
