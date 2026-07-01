-- Transform: stg_customers → dim_customers
-- Cast tipe data, tambah derived columns, deduplikasi

TRUNCATE TABLE dim_accounts;

INSERT INTO dim_accounts (
    account_id,
    account_no,
    account_type,
    product_name,
    currency,
    close_date,
    open_date,
    status,
    interest_rate,
    customer_id,
    branch_id
)

SELECT DISTINCT ON (account_id)
    account_id,
    account_no,
    account_type,
    product_name,
    currency,
    close_date::DATE,
    open_date::DATE,
    status,
    interest_rate::NUMERIC,
    customer_id,
    branch_id
FROM stg_accounts
WHERE account_id IS NOT NULL
ORDER BY account_id;
