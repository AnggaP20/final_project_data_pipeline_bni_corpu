-- Transform: stg_customers → dim_customers
-- Cast tipe data, tambah derived columns, deduplikasi

TRUNCATE TABLE dim_dates;

INSERT INTO dim_dates (
    date_id,
    full_date,
    year,
    quarter,
    month,
    month_name,
    week_of_year,
    day_of_month,
    day_of_week,
    day_name,
    is_weekend,
    is_holiday
)
SELECT DISTINCT ON(date_id)
    date_id,
    full_date::DATE,
    EXTRACT(YEAR FROM full_date::DATE)::SMALLINT AS year,
    EXTRACT(QUARTER FROM full_date::DATE)::SMALLINT AS quarter,
    EXTRACT(MONTH FROM full_date::DATE)::SMALLINT AS month,
    TO_CHAR(full_date::DATE, 'Month') AS month_name,
    EXTRACT(WEEK FROM full_date::DATE)::SMALLINT AS week_of_year,
    EXTRACT(DAY FROM full_date::DATE)::SMALLINT AS day_of_month,
    EXTRACT(ISODOW FROM full_date::DATE)::SMALLINT AS day_of_week, -- 1=Mon, 7=Sun
    TO_CHAR(full_date::DATE, 'Day') AS day_name,
    CASE
        WHEN EXTRACT(ISODOW FROM full_date::DATE) IN (6,7) THEN TRUE
        ELSE FALSE
    END AS is_weekend,
    CASE WHEN LOWER(is_holiday) = 'true' THEN TRUE ELSE FALSE END AS is_holiday
FROM stg_dim_dates
WHERE date_id is not NULL
order by date_id
;
