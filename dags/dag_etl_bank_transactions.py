"""
dag_etl_bank_transactions.py
==============================
ETL pipeline: bank_transactions_data_2.csv → PostgreSQL

Task flow:
    create_tables  (SQLExecuteQueryOperator) : buat tabel staging, clean, final
    extract        (@task Python)            : baca CSV → trx_raw (staging)
    transform      (SQLExecuteQueryOperator) : trx_raw → trx_clean
    load           (SQLExecuteQueryOperator) : trx_clean → trx_sample (upsert)

Airflow Connection yang dibutuhkan:
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
CONN_ID     = "postgres_etl"
SOURCE_FILE = os.path.join(
    os.path.dirname(__file__), "..", "include", "dataset", "bank_transactions_data_2.csv"
)

DDL_STATEMENTS = """
CREATE TABLE IF NOT EXISTS trx_raw (
    "TransactionID"           TEXT,
    "AccountID"               TEXT,
    "TransactionAmount"       TEXT,
    "TransactionDate"         TEXT,
    "TransactionType"         TEXT,
    "Location"                TEXT,
    "DeviceID"                TEXT,
    "IP Address"              TEXT,
    "MerchantID"              TEXT,
    "Channel"                 TEXT,
    "CustomerAge"             TEXT,
    "CustomerOccupation"      TEXT,
    "TransactionDuration"     TEXT,
    "LoginAttempts"           TEXT,
    "AccountBalance"          TEXT,
    "PreviousTransactionDate" TEXT
);

CREATE TABLE IF NOT EXISTS trx_clean (
    transaction_id            VARCHAR(20)   PRIMARY KEY,
    account_id                VARCHAR(20),
    transaction_amount        NUMERIC(18,2),
    transaction_date          TIMESTAMP,
    transaction_type          VARCHAR(50),
    location                  VARCHAR(100),
    device_id                 VARCHAR(20),
    ip_address                VARCHAR(45),
    merchant_id               VARCHAR(20),
    channel                   VARCHAR(50),
    customer_age              SMALLINT,
    customer_occupation       VARCHAR(100),
    transaction_duration      INTEGER,
    login_attempts            SMALLINT,
    account_balance           NUMERIC(18,2),
    previous_transaction_date TIMESTAMP
);

CREATE TABLE IF NOT EXISTS trx_sample (
    transaction_id            VARCHAR(20)   PRIMARY KEY,
    account_id                VARCHAR(20),
    transaction_amount        NUMERIC(18,2),
    transaction_date          TIMESTAMP,
    transaction_type          VARCHAR(50),
    location                  VARCHAR(100),
    device_id                 VARCHAR(20),
    ip_address                VARCHAR(45),
    merchant_id               VARCHAR(20),
    channel                   VARCHAR(50),
    customer_age              SMALLINT,
    customer_occupation       VARCHAR(100),
    transaction_duration      INTEGER,
    login_attempts            SMALLINT,
    account_balance           NUMERIC(18,2),
    previous_transaction_date TIMESTAMP,
    etl_loaded_at             TIMESTAMP     DEFAULT NOW()
);
"""


# ─── DAG ──────────────────────────────────────────────────────────────────────
@dag(
    dag_id              = "dag_etl_bank_transactions",
    description         = "ETL bank_transactions_data_2.csv → PostgreSQL trx_sample",
    default_args        = {
        "owner"           : "airflow",
        "retries"         : 1,
        "retry_delay"     : timedelta(minutes=5),
        "email_on_failure": False,
    },
    start_date          = datetime(2025, 1, 1),
    schedule            = None,
    catchup             = False,
    tags                = ["etl", "banking", "postgresql"],
    template_searchpath = ["/opt/airflow/include/sql/etl_banking"],
)
def dag_etl_bank_transactions():

    # ── Task 1: DDL ───────────────────────────────────────────────────────────
    create_tables = SQLExecuteQueryOperator(
        task_id = "create_tables",
        conn_id = CONN_ID,
        sql     = DDL_STATEMENTS,
    )

    # ── Task 2: Extract CSV → trx_raw ─────────────────────────────────────────
    @task()
    def extract():
        from airflow.hooks.base import BaseHook

        conn     = BaseHook.get_connection(CONN_ID)
        conn_str = (
            f"postgresql+psycopg2://{conn.login}:{conn.password}"
            f"@{conn.host}:{conn.port}/{conn.schema}"
        )
        engine = create_engine(conn_str)

        df = pd.read_csv(SOURCE_FILE)

        with engine.connect() as c:
            c.execute(text("TRUNCATE TABLE trx_raw"))
            c.commit()

        df.to_sql(
            name      = "trx_raw",
            con       = engine,
            if_exists = "append",
            index     = False,
            method    = "multi",
            chunksize = 1000,
        )
        engine.dispose()
        return len(df)

    # ── Task 3: Transform trx_raw → trx_clean ────────────────────────────────
    transform = SQLExecuteQueryOperator(
        task_id = "transform",
        conn_id = CONN_ID,
        sql     = "01_transform.sql",
    )

    # ── Task 4: Load trx_clean → trx_sample (upsert) ─────────────────────────
    load = SQLExecuteQueryOperator(
        task_id = "load",
        conn_id = CONN_ID,
        sql     = "02_load.sql",
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    create_tables >> extract() >> transform >> load


dag_etl_bank_transactions()
