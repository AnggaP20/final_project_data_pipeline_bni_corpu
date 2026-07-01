"""
dag_etl_branches.py
=====================
ETL pipeline: branches.csv → stg_branches → dim_branches

Task flow:
    create_tables_branches  (SQLExecuteQueryOperator) : DDL stg_branches & dim_branches
    extract_load_branches   (@task Python)            : baca CSV → stg_branches
    transform_branches      (SQLExecuteQueryOperator) : stg_branches → dim_branches

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
    os.path.dirname(__file__), "..", "include", "dataset", "branches.csv"
)

DDL_STATEMENTS = """
CREATE TABLE IF NOT EXISTS stg_branches (
    branch_id       INTEGER,
    branch_code     VARCHAR(20),
    branch_name         VARCHAR(150),
    city            VARCHAR(20),
    province        VARCHAR(20),
    region             VARCHAR(20),
    branch_type             VARCHAR(20),
    open_date           VARCHAR(20),
    is_active         VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS dim_branches (
branch_id       INTEGER PRIMARY KEY,
    branch_code     VARCHAR(20),
    branch_name         VARCHAR(150),
    city            VARCHAR(20),
    province        VARCHAR(20),
    region             VARCHAR(200),
    branch_type             VARCHAR(20),
    open_date           DATE,
    branch_age          SMALLINT,
    is_active         BOOLEAN,
    etl_loaded_at        TIMESTAMP     DEFAULT NOW()
);
"""


# ─── DAG ──────────────────────────────────────────────────────────────────────
@dag(
    dag_id              = "dag_etl_branches",
    description         = "ETL branches.csv → stg_branches → dim_branches",
    default_args        = {
        "owner"           : "airflow",
        "retries"         : 1,
        "retry_delay"     : timedelta(minutes=5),
        "email_on_failure": False,
    },
    start_date          = datetime(2025, 1, 1),
    schedule            = None,
    catchup             = False,
    tags                = ["etl", "branches", "dim", "postgresql"],
    template_searchpath = ["/opt/airflow/include/sql/branches"],
)
def dag_etl_customers():

    # ── Task 1: DDL ───────────────────────────────────────────────────────────
    create_tables_branches = SQLExecuteQueryOperator(
        task_id = "create_tables_branches",
        conn_id = CONN_ID,
        sql     = DDL_STATEMENTS,
    )

    # ── Task 2: Extract CSV → stg_customers ──────────────────────────────────
    @task()
    def extract_load_branches():
        from airflow.hooks.base import BaseHook

        conn     = BaseHook.get_connection(CONN_ID)
        conn_str = (
            f"postgresql+psycopg2://{conn.login}:{conn.password}"
            f"@{conn.host}:{conn.port}/{conn.schema}"
        )
        engine = create_engine(conn_str)

        df = pd.read_csv(SOURCE_FILE)

        with engine.connect() as c:
            c.execute(text("TRUNCATE TABLE stg_branches"))
            c.commit()

        df.to_sql(
            name      = "stg_branches",
            con       = engine,
            if_exists = "append",
            index     = False,
            method    = "multi",
            chunksize = 1000,
        )
        engine.dispose()
        return len(df)

    # ── Task 3: Transform stg_customers → dim_customers ──────────────────────
    transform_branches = SQLExecuteQueryOperator(
        task_id = "transform_branches",
        conn_id = CONN_ID,
        sql     = "01_transform.sql",
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    create_tables_branches >> extract_load_branches() >> transform_branches


dag_etl_customers()
