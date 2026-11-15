SELECT
    event_date,
    txn_type,
    COUNT(*)                        AS txn_count,
    SUM(amount)                     AS total_amount,
    SUM(fee)                        AS total_fee,
    ROUND(AVG(amount)::NUMERIC, 2)  AS avg_amount,
    COUNT(DISTINCT sender_wallet)   AS unique_senders,
    COUNT(DISTINCT district)        AS active_districts
FROM {{ ref('stg_transactions') }}
GROUP BY 1, 2
