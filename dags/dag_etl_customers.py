"""
dag_etl_customers.py
=====================
ETL pipeline: customers.csv → stg_customers → dim_customers

Task flow:
    create_tables  (SQLExecuteQueryOperator) : DDL stg_customers & dim_customers
    extract_load   (@task Python)            : baca CSV → stg_customers
    transform      (SQLExecuteQueryOperator) : stg_customers → dim_customers

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
CONN_ID     = "neondb_postgres" # <-- ganti dengan koneksi database yang sudah dibuat di airflow
SOURCE_FILE = os.path.join(
    os.path.dirname(__file__), "..", "include", "dataset", "customers.csv"
)

DDL_STATEMENTS = """
CREATE TABLE IF NOT EXISTS stg_customers (
    customer_id       INTEGER,
    customer_code     VARCHAR(20),
    full_name         VARCHAR(150),
    gender            VARCHAR(5),
    birth_date        VARCHAR(20),
    email             VARCHAR(150),
    phone             VARCHAR(20),
    segment           VARCHAR(20),
    job_segment       VARCHAR(100),
    city              VARCHAR(100),
    province          VARCHAR(100),
    registration_date VARCHAR(20),
    branch_id         INTEGER,
    is_active         VARCHAR(10),
    credit_score      SMALLINT,
    estimated_salary  NUMERIC(18,2)
);

CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id          INTEGER       PRIMARY KEY,
    customer_code        VARCHAR(20),
    full_name            VARCHAR(150),
    gender               VARCHAR(5),
    birth_date           DATE,
    email                VARCHAR(150),
    phone                VARCHAR(20),
    segment              VARCHAR(20),
    job_segment          VARCHAR(100),
    city                 VARCHAR(100),
    province             VARCHAR(100),
    registration_date    DATE,
    branch_id            INTEGER,
    is_active            BOOLEAN,
    credit_score         SMALLINT,
    estimated_salary     NUMERIC(18,2),
    age                  SMALLINT,
    credit_score_segment VARCHAR(20),
    salary_segment       VARCHAR(20),
    etl_loaded_at        TIMESTAMP     DEFAULT NOW()
);
"""


# ─── DAG ──────────────────────────────────────────────────────────────────────
@dag(
    dag_id              = "dag_etl_customers",
    description         = "ETL customers.csv → stg_customers → dim_customers",
    default_args        = {
        "owner"           : "airflow",
        "retries"         : 1,
        "retry_delay"     : timedelta(minutes=5),
        "email_on_failure": False,
    },
    start_date          = datetime(2025, 1, 1),
    schedule            = None,
    catchup             = False,
    tags                = ["etl", "customers", "dim", "postgresql"],
    template_searchpath = ["/opt/airflow/include/sql/customers"],
)
def dag_etl_customers():

    # ── Task 1: DDL ───────────────────────────────────────────────────────────
    create_tables = SQLExecuteQueryOperator(
        task_id = "create_tables",
        conn_id = CONN_ID,
        sql     = DDL_STATEMENTS,
    )

    # ── Task 2: Extract CSV → stg_customers ──────────────────────────────────
    @task()
    def extract_load():
        from airflow.hooks.base import BaseHook

        conn     = BaseHook.get_connection(CONN_ID)
        conn_str = (
            f"postgresql+psycopg2://{conn.login}:{conn.password}"
            f"@{conn.host}:{conn.port}/{conn.schema}"
        )
        engine = create_engine(conn_str)

        df = pd.read_csv(SOURCE_FILE)

        with engine.connect() as c:
            c.execute(text("TRUNCATE TABLE stg_customers"))
            c.commit()

        df.to_sql(
            name      = "stg_customers",
            con       = engine,
            if_exists = "append",
            index     = False,
            method    = "multi",
            chunksize = 1000,
        )
        engine.dispose()
        return len(df)

    # ── Task 3: Transform stg_customers → dim_customers ──────────────────────
    transform = SQLExecuteQueryOperator(
        task_id = "transform",
        conn_id = CONN_ID,
        sql     = "01_transform.sql",
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    create_tables >> extract_load() >> transform


dag_etl_customers()
