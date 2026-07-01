"""
dag_etl_dim_dates.py
=====================
ETL pipeline: dim_dates.csv → stg_dim_dates → dim_dim_dates

Task flow:
    create_tables_dim_dates  (SQLExecuteQueryOperator) : DDL stg_dim_dates & dim_dim_dates
    extract_load_dim_dates   (@task Python)            : baca CSV → stg_dim_dates
    transform_dim_dates      (SQLExecuteQueryOperator) : stg_dim_dates → dim_dim_dates

Airflow Connection:
    conn_id = "postgres_etl"  (tipe: Postgres)
    Host: postgres-etl | Port: 5432 | DB: etl_db
"""

import os
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine, text

from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# ─── Konstanta ────────────────────────────────────────────────────────────────
CONN_ID     = "postgres_etl" # <-- ganti dengan koneksi database yang sudah dibuat di airflow
SOURCE_FILE = os.path.join(
    os.path.dirname(__file__), "..", "include", "dataset", "dim_date.csv"
)

DDL_STATEMENTS = """
CREATE TABLE IF NOT EXISTS stg_dim_dates (
    date_id       INTEGER, -- YYYYMMDD
    full_date     VARCHAR(20),
    year          VARCHAR(20),
    quarter       VARCHAR(20),
    month         VARCHAR(20),
    month_name    VARCHAR(20),
    week_of_year  VARCHAR(20),
    day_of_month  VARCHAR(20),
    day_of_week   VARCHAR(20),
    day_name      VARCHAR(20),
    is_weekend    VARCHAR(20),
    is_holiday    VARCHAR(20)

);

CREATE TABLE IF NOT EXISTS dim_dates (
    date_id       INTEGER PRIMARY KEY, -- YYYYMMDD
    full_date     DATE NOT NULL UNIQUE,
    year          SMALLINT NOT NULL,
    quarter       SMALLINT NOT NULL,
    month         SMALLINT NOT NULL,
    month_name    VARCHAR(20) NOT NULL,
    week_of_year  SMALLINT NOT NULL,
    day_of_month  SMALLINT NOT NULL,
    day_of_week   SMALLINT NOT NULL,
    day_name      VARCHAR(20) NOT NULL,
    is_weekend    BOOLEAN NOT NULL,
    is_holiday    BOOLEAN NOT NULL DEFAULT FALSE,
    etl_loaded_at        TIMESTAMP     DEFAULT NOW()
);
"""


# ─── DAG ──────────────────────────────────────────────────────────────────────
@dag(
    dag_id              = "dag_etl_dim_dates",
    description         = "ETL dim_dates.csv → stg_dim_dates → dim_dim_dates",
    default_args        = {
        "owner"           : "airflow",
        "retries"         : 1,
        "retry_delay"     : timedelta(minutes=5),
        "email_on_failure": False,
    },
    start_date          = datetime(2025, 1, 1),
    schedule            = None,
    catchup             = False,
    tags                = ["etl", "dim_dates", "dim", "postgresql"],
    template_searchpath = ["/opt/airflow/include/sql/dim_dates"],
)
def dag_etl_dim_dates():

    # ── Task 1: DDL ───────────────────────────────────────────────────────────
    create_tables_dim_dates = SQLExecuteQueryOperator(
        task_id = "create_tables_dim_dates",
        conn_id = CONN_ID,
        sql     = DDL_STATEMENTS,
    )

    # ── Task 2: Extract CSV → stg_dim_dates ──────────────────────────────────
    @task()
    def extract_load_dim_dates():
        from airflow.hooks.base import BaseHook

        conn     = BaseHook.get_connection(CONN_ID)
        conn_str = (
            f"postgresql+psycopg2://{conn.login}:{conn.password}"
            f"@{conn.host}:{conn.port}/{conn.schema}"
        )
        engine = create_engine(conn_str)

        df = pd.read_csv(SOURCE_FILE)

        with engine.connect() as c:
            c.execute(text("TRUNCATE TABLE stg_dim_dates"))
            c.commit()

        df.to_sql(
            name      = "stg_dim_dates",
            con       = engine,
            if_exists = "append",
            index     = False,
            method    = "multi",
            chunksize = 1000,
        )
        engine.dispose()
        return len(df)

    # ── Task 3: Transform stg_customers → dim_customers ──────────────────────
    transform_dim_dates = SQLExecuteQueryOperator(
        task_id = "transform_dim_dates",
        conn_id = CONN_ID,
        sql     = "01_transform.sql",
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    create_tables_dim_dates >> extract_load_dim_dates() >> transform_dim_dates


dag_etl_dim_dates()
