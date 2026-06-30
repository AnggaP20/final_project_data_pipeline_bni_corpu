"""
dag_el_churn.py
================
Pipeline EL + Transform untuk data churn.csv

Task flow:
    create_tables  (SQLExecuteQueryOperator) : buat tabel staging & clean kalau belum ada
    extract_load   (@task Python)            : baca churn.csv → load ke churn_staging
    transform      (SQLExecuteQueryOperator) : churn_staging → churn_clean (cast + derived cols)

Airflow Connection yang dibutuhkan:
    conn_id = "postgres_etl"  (tipe: Postgres)
    Host    : postgres-etl  |  Port: 5432  |  DB: etl_db
"""

import os
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine, text

from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# ─── Konstanta ────────────────────────────────────────────────────────────────
CONN_ID     = "neondb_postgres"
SOURCE_FILE = os.path.join(
    os.path.dirname(__file__), "..", "include", "dataset", "churn.csv"
)

DDL_STATEMENTS = """
CREATE TABLE IF NOT EXISTS churn_staging (
    row_number       INTEGER,
    customer_id      BIGINT,
    surname          VARCHAR(100),
    credit_score     SMALLINT,
    geography        VARCHAR(50),
    gender           VARCHAR(10),
    age              SMALLINT,
    tenure           SMALLINT,
    balance          NUMERIC(18,2),
    num_of_products  SMALLINT,
    has_cr_card      SMALLINT,
    is_active_member SMALLINT,
    estimated_salary NUMERIC(18,2),
    exited           SMALLINT
);

CREATE TABLE IF NOT EXISTS churn_clean (
    customer_id          BIGINT        PRIMARY KEY,
    surname              VARCHAR(100),
    credit_score         SMALLINT,
    geography            VARCHAR(50),
    gender               VARCHAR(10),
    age                  SMALLINT,
    tenure               SMALLINT,
    balance              NUMERIC(18,2),
    num_of_products      SMALLINT,
    has_cr_card          BOOLEAN,
    is_active_member     BOOLEAN,
    estimated_salary     NUMERIC(18,2),
    exited               BOOLEAN,
    age_group            VARCHAR(20),
    balance_segment      VARCHAR(20),
    credit_score_segment VARCHAR(20),
    etl_loaded_at        TIMESTAMP     DEFAULT NOW()
);
"""


# ─── DAG ──────────────────────────────────────────────────────────────────────
@dag(
    dag_id              = "dag_el_churn",
    description         = "EL pipeline: churn.csv → staging → transform → churn_clean",
    default_args        = {
        "owner"           : "airflow",
        "retries"         : 1,
        "retry_delay"     : timedelta(minutes=5),
        "email_on_failure": False,
    },
    start_date          = datetime(2025, 1, 1),
    schedule            = None,
    catchup             = False,
    tags                = ["el", "churn", "postgresql"],
    template_searchpath = ["/opt/airflow/include/sql/churn"],
)
def dag_el_churn():

    # ── Task 1: buat tabel jika belum ada ─────────────────────────────────────
    create_tables = SQLExecuteQueryOperator(
        task_id = "create_tables",
        conn_id = CONN_ID,
        sql     = DDL_STATEMENTS,
    )

    # ── Task 2: Extract + Load CSV → churn_staging ───────────────────────────
    @task()
    def extract_load():
        from airflow.hooks.base import BaseHook

        conn = BaseHook.get_connection(CONN_ID)
        conn_str = (
            f"postgresql+psycopg2://{conn.login}:{conn.password}"
            f"@{conn.host}:{conn.port}/{conn.schema}"
        )
        engine = create_engine(conn_str)

        df = pd.read_csv(SOURCE_FILE)

        # Rename kolom → snake_case agar sesuai DDL staging
        df = df.rename(columns={
            "RowNumber"      : "row_number",
            "CustomerId"     : "customer_id",
            "Surname"        : "surname",
            "CreditScore"    : "credit_score",
            "Geography"      : "geography",
            "Gender"         : "gender",
            "Age"            : "age",
            "Tenure"         : "tenure",
            "Balance"        : "balance",
            "NumOfProducts"  : "num_of_products",
            "HasCrCard"      : "has_cr_card",
            "IsActiveMember" : "is_active_member",
            "EstimatedSalary": "estimated_salary",
            "Exited"         : "exited",
        })

        with engine.connect() as c:
            c.execute(text("TRUNCATE TABLE churn_staging"))
            c.commit()

        df.to_sql(
            name      = "churn_staging",
            con       = engine,
            if_exists = "append",
            index     = False,
            method    = "multi",
            chunksize = 1000,
        )
        engine.dispose()
        return len(df)

    # ── Task 3: Transform staging → clean via SQL ─────────────────────────────
    transform = SQLExecuteQueryOperator(
        task_id = "transform",
        conn_id = CONN_ID,
        sql     = "01_transform.sql",
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    create_tables >> extract_load() >> transform


dag_el_churn()
