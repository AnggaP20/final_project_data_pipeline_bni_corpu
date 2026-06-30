-- Transform: stg_customers → dim_customers
-- Cast tipe data, tambah derived columns, deduplikasi

TRUNCATE TABLE dim_customers;

INSERT INTO dim_customers (
    customer_id,
    customer_code,
    full_name,
    gender,
    birth_date,
    email,
    phone,
    segment,
    job_segment,
    city,
    province,
    registration_date,
    branch_id,
    is_active,
    credit_score,
    estimated_salary,
    -- derived columns
    age,
    credit_score_segment,
    salary_segment
)
SELECT DISTINCT ON (customer_id)
    customer_id,
    customer_code,
    full_name,
    gender,
    birth_date::DATE,
    email,
    phone,
    segment,
    job_segment,
    city,
    province,
    registration_date::DATE,
    branch_id,
    CASE WHEN LOWER(is_active) = 'true' THEN TRUE ELSE FALSE END AS is_active,
    credit_score,
    estimated_salary,
    -- usia dari birth_date
    DATE_PART('year', AGE(CURRENT_DATE, birth_date::DATE))::SMALLINT AS age,
    -- segmentasi credit score
    CASE
        WHEN credit_score < 580 THEN 'Poor'
        WHEN credit_score < 670 THEN 'Fair'
        WHEN credit_score < 740 THEN 'Good'
        WHEN credit_score < 800 THEN 'Very Good'
        ELSE 'Exceptional'
    END AS credit_score_segment,
    -- segmentasi gaji
    CASE
        WHEN estimated_salary <  5000000  THEN 'Low'
        WHEN estimated_salary < 15000000  THEN 'Lower Middle'
        WHEN estimated_salary < 30000000  THEN 'Middle'
        WHEN estimated_salary < 50000000  THEN 'Upper Middle'
        ELSE 'High'
    END AS salary_segment
FROM stg_customers
WHERE customer_id IS NOT NULL
ORDER BY customer_id;
