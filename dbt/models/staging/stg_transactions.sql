SELECT
    txn_id,
    txn_type,
    sender_wallet,
    receiver_wallet,
    agent_id,
    merchant_id,
    biller,
    amount,
    fee,
    channel,
    district,
    event_time,
    event_date,
    source_ingest_date,
    processed_at,
    batch_run_id
FROM {{ source('silver', 'transactions') }}
