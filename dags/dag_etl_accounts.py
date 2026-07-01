"""
dag_etl_accounts.py
=====================
ETL pipeline: accounts.csv → stg_accounts → dim_accounts

Task flow:
    create_tables_accounts  (SQLExecuteQueryOperator) : DDL stg_accounts & dim_accounts
    extract_load_accounts   (@task Python)            : baca CSV → stg_accounts
    transform_accounts      (SQLExecuteQueryOperator) : stg_accounts → dim_accounts

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
    os.path.dirname(__file__), "..", "include", "dataset", "accounts.csv"
)

DDL_STATEMENTS = """
CREATE TABLE IF NOT EXISTS stg_accounts (
    account_id     INTEGER,
    account_no     VARCHAR(30),
    account_type   VARCHAR(50),
    product_name   VARCHAR(100),
    currency       CHAR(3),
    open_date      VARCHAR(10),
    close_date     VARCHAR(10),
    status         VARCHAR(20),
    interest_rate  VARCHAR(10),
    customer_id    INTEGER,
    branch_id      INTEGER
);

CREATE TABLE IF NOT EXISTS dim_accounts (
    account_id     INTEGER PRIMARY KEY,
    account_no     VARCHAR(30) UNIQUE NOT NULL,
    account_type   VARCHAR(50) NOT NULL,
    product_name   VARCHAR(100) NOT NULL,
    currency       CHAR(3) NOT NULL,
    open_date      DATE NOT NULL,
    close_date     DATE,
    status         VARCHAR(20) NOT NULL,
    interest_rate  NUMERIC(5,2),
    customer_id    INTEGER NOT NULL REFERENCES dim_customers(customer_id),
    branch_id      INTEGER NOT NULL REFERENCES dim_branches(branch_id),
    etl_loaded_at        TIMESTAMP     DEFAULT NOW()
);
"""


# ─── DAG ──────────────────────────────────────────────────────────────────────
@dag(
    dag_id              = "dag_etl_accounts",
    description         = "ETL accounts.csv → stg_accounts → dim_accounts",
    default_args        = {
        "owner"           : "airflow",
        "retries"         : 1,
        "retry_delay"     : timedelta(minutes=5),
        "email_on_failure": False,
    },
    start_date          = datetime(2025, 1, 1),
    schedule            = None,
    catchup             = False,
    tags                = ["etl", "accounts", "dim", "postgresql"],
    template_searchpath = ["/opt/airflow/include/sql/accounts"],
)
def dag_etl_customers():

    # ── Task 1: DDL ───────────────────────────────────────────────────────────
    create_tables_accounts = SQLExecuteQueryOperator(
        task_id = "create_tables_accounts",
        conn_id = CONN_ID,
        sql     = DDL_STATEMENTS,
    )

    # ── Task 2: Extract CSV → stg_customers ──────────────────────────────────
    @task()
    def extract_load_accounts():
        from airflow.hooks.base import BaseHook

        conn     = BaseHook.get_connection(CONN_ID)
        conn_str = (
            f"postgresql+psycopg2://{conn.login}:{conn.password}"
            f"@{conn.host}:{conn.port}/{conn.schema}"
        )
        engine = create_engine(conn_str)

        df = pd.read_csv(SOURCE_FILE)

        with engine.connect() as c:
            c.execute(text("TRUNCATE TABLE stg_accounts"))
            c.commit()

        df.to_sql(
            name      = "stg_accounts",
            con       = engine,
            if_exists = "append",
            index     = False,
            method    = "multi",
            chunksize = 1000,
        )
        engine.dispose()
        return len(df)

    # ── Task 3: Transform stg_customers → dim_customers ──────────────────────
    transform_accounts = SQLExecuteQueryOperator(
        task_id = "transform_accounts",
        conn_id = CONN_ID,
        sql     = "01_transform.sql",
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    create_tables_accounts >> extract_load_accounts() >> transform_accounts


dag_etl_customers()
