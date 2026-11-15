SELECT
    COALESCE(district, 'Unknown')       AS district,
    event_date,
    txn_type,
    COUNT(*)                            AS txn_count,
    SUM(amount)                         AS total_amount,
    ROUND(AVG(amount)::NUMERIC, 2)      AS avg_amount
FROM {{ ref('stg_transactions') }}
GROUP BY 1, 2, 3
