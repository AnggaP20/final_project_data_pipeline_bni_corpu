-- Transform: stg_customers → dim_customers
-- Cast tipe data, tambah derived columns, deduplikasi

TRUNCATE TABLE dim_fraud_labels;

INSERT INTO dim_fraud_labels (
    transaction_id,
    transaction_code,
    is_fraud ,
    fraud_type,
    fraud_score,
    flagged_at
)
SELECT
    transaction_id,
    transaction_code,
    CASE WHEN LOWER(is_fraud) = 'true' THEN TRUE ELSE FALSE END AS is_fraud ,
    fraud_type,
    fraud_score::NUMERIC,
    flagged_at::DATE
FROM stg_fraud_labels
;
