UPDATE fact_transactions ft
SET
    is_fraud = dfl.is_fraud
FROM dim_fraud_labels dfl
WHERE ft.transaction_id = dfl.transaction_id;