-- Transform: stg_customers → dim_customers
-- Cast tipe data, tambah derived columns, deduplikasi

TRUNCATE TABLE fact_transactions;

INSERT INTO fact_transactions (
    transaction_id,
    date_id,
    customer_id,
    account_id,
    branch_id,
    channel_id,
    amount,
    balance_before,
    balance_after,
    transaction_at,
    transaction_type ,
    status,
    reference_no
)
SELECT
    transaction_id::INTEGER,
    TO_CHAR(transaction_date::DATE, 'YYYYMMDD')::INTEGER as date_id,
    customer_id::INTEGER,
    account_id::INTEGER,
    branch_id::INTEGER,
    channel_id::INTEGER,
    amount::NUMERIC(18,2),
    balance_before::NUMERIC(18,2),
    balance_after::NUMERIC(18,2),
    transaction_at::TIMESTAMP,
    transaction_type,
    status,
    reference_no 
FROM stg_transactions
;
