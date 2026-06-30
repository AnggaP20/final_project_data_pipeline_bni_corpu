-- Transform: churn_staging → churn_clean
-- Cast tipe data, hapus duplikat, tambah derived columns

TRUNCATE TABLE churn_clean;

INSERT INTO churn_clean (
    customer_id,
    surname,
    credit_score,
    geography,
    gender,
    age,
    tenure,
    balance,
    num_of_products,
    has_cr_card,
    is_active_member,
    estimated_salary,
    exited,
    -- derived columns
    age_group,
    balance_segment,
    credit_score_segment
)
SELECT DISTINCT ON (customer_id)
    customer_id,
    surname,
    credit_score,
    geography,
    gender,
    age,
    tenure,
    balance,
    num_of_products,
    has_cr_card != 0,           -- SMALLINT (0/1) → BOOLEAN
    is_active_member != 0,      -- SMALLINT (0/1) → BOOLEAN
    estimated_salary,
    exited != 0,                -- SMALLINT (0/1) → BOOLEAN
    -- age group
    CASE
        WHEN age < 30 THEN 'Young'
        WHEN age BETWEEN 30 AND 45 THEN 'Middle'
        WHEN age BETWEEN 46 AND 60 THEN 'Senior'
        ELSE 'Elder'
    END AS age_group,
    -- balance segment
    CASE
        WHEN balance = 0          THEN 'Zero'
        WHEN balance < 50000      THEN 'Low'
        WHEN balance < 100000     THEN 'Medium'
        WHEN balance < 150000     THEN 'High'
        ELSE 'Very High'
    END AS balance_segment,
    -- credit score segment
    CASE
        WHEN credit_score < 580   THEN 'Poor'
        WHEN credit_score < 670   THEN 'Fair'
        WHEN credit_score < 740   THEN 'Good'
        WHEN credit_score < 800   THEN 'Very Good'
        ELSE 'Exceptional'
    END AS credit_score_segment
FROM churn_staging
WHERE customer_id IS NOT NULL
ORDER BY customer_id;
