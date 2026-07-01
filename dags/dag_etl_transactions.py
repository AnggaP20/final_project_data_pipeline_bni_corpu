"""
dag_etl_transactions.py
=====================
ETL pipeline: transactions.csv → stg_transactions → dim_transactions

Task flow:
    create_tables_transactions  (SQLExecuteQueryOperator) : DDL stg_transactions & dim_transactions
    extract_load_transactions   (@task Python)            : baca CSV → stg_transactions
    transform_transactions      (SQLExecuteQueryOperator) : stg_transactions → dim_transactions

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
SOURCE_FILE_TRX = os.path.join(
    os.path.dirname(__file__), "..", "include", "dataset", "transactions.csv"
)

SOURCE_FILE_FRAUD_LABELS = os.path.join(
    os.path.dirname(__file__), "..", "include", "dataset", "fraud_labels.csv"
)

DDL_STATEMENTS = """
CREATE TABLE IF NOT EXISTS stg_transactions (
    transaction_id      VARCHAR(100),
    transaction_code    VARCHAR(100),
    account_id          VARCHAR(100),
    customer_id         VARCHAR(100),
    branch_id           VARCHAR(100),
    channel_id          VARCHAR(100),
    transaction_date    VARCHAR(50),
    transaction_at      VARCHAR(50),
    transaction_type    VARCHAR(50),
    amount              VARCHAR(100),
    balance_before      VARCHAR(100),
    balance_after       VARCHAR(100),
    status              VARCHAR(50),
    reference_no        VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_id    INTEGER PRIMARY KEY,
    date_id           INTEGER NOT NULL,
    customer_id       INTEGER NOT NULL,
    account_id        INTEGER NOT NULL,
    branch_id         INTEGER NOT NULL,
    channel_id        SMALLINT NOT NULL,
    amount            NUMERIC(18,2) NOT NULL,
    balance_before    NUMERIC(18,2),
    balance_after     NUMERIC(18,2),
    transaction_at    TIMESTAMP NOT NULL,
    transaction_type  VARCHAR(50) NOT NULL,
    status            VARCHAR(20) NOT NULL,
    is_fraud          BOOLEAN NOT NULL DEFAULT FALSE,
    reference_no      VARCHAR(50) UNIQUE,
    etl_loaded_at        TIMESTAMP     DEFAULT NOW(),


    CONSTRAINT fk_fact_date
        FOREIGN KEY (date_id)
        REFERENCES dim_dates(date_id),

    CONSTRAINT fk_fact_customer
        FOREIGN KEY (customer_id)
        REFERENCES dim_customers(customer_id),

    CONSTRAINT fk_fact_account
        FOREIGN KEY (account_id)
        REFERENCES dim_accounts(account_id),

    CONSTRAINT fk_fact_branch
        FOREIGN KEY (branch_id)
        REFERENCES dim_branches(branch_id),

    CONSTRAINT fk_fact_channel
        FOREIGN KEY (channel_id)
        REFERENCES dim_channels(channel_id)
);
"""
DDL_STATEMENTS2 = """
    CREATE TABLE IF NOT EXISTS stg_fraud_labels (
    transaction_id    INTEGER,
    transaction_code  VARCHAR(255),
    is_fraud          VARCHAR(255),
    fraud_type        VARCHAR(255),
    fraud_score       VARCHAR(255),
    flagged_at        VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dim_fraud_labels (
    transaction_id    INTEGER NOT NULL,
    transaction_code  VARCHAR(50) NOT NULL,
    is_fraud          BOOLEAN DEFAULT FALSE,
    fraud_type        VARCHAR(100),
    fraud_score       NUMERIC,
    flagged_at        DATE
)
"""

# ─── DAG ──────────────────────────────────────────────────────────────────────
@dag(
    dag_id              = "dag_etl_master_trx",
    description         = "ETL transactions.csv,fraud_labels.csv → stg_transactions,stg_fraud_labels → fatc_transactions, dim_fraud_labels",
    default_args        = {
        "owner"           : "airflow",
        "retries"         : 1,
        "retry_delay"     : timedelta(minutes=5),
        "email_on_failure": False,
    },
    start_date          = datetime(2025, 1, 1),
    schedule            = None,
    catchup             = False,
    tags                = ["etl", "transactions", "fact", "postgresql"],
    template_searchpath = ["/opt/airflow/include/sql"],
)
def dag_etl_master_trx():
    ## TRANSACTIONS ##

    # ── Task 1: DDL ───────────────────────────────────────────────────────────
    create_tables_transactions = SQLExecuteQueryOperator(
        task_id = "create_tables_transactions",
        conn_id = CONN_ID,
        sql     = DDL_STATEMENTS,
    )

    # ── Task 2: Extract CSV → stg_transactions ──────────────────────────────────
    @task()
    def extract_load_transactions():
        from airflow.hooks.base import BaseHook

        conn     = BaseHook.get_connection(CONN_ID)
        conn_str = (
            f"postgresql+psycopg2://{conn.login}:{conn.password}"
            f"@{conn.host}:{conn.port}/{conn.schema}"
        )
        engine = create_engine(conn_str)

        df = pd.read_csv(SOURCE_FILE_TRX)

        with engine.connect() as c:
            c.execute(text("TRUNCATE TABLE stg_transactions"))
            c.commit()

        df.to_sql(
            name      = "stg_transactions",
            con       = engine,
            if_exists = "append",
            index     = False,
            method    = "multi",
            chunksize = 1000,
        )
        engine.dispose()
        return len(df)

    # ── Task 3: Transform stg_customers → dim_customers ──────────────────────
    transform_transactions = SQLExecuteQueryOperator(
        task_id = "transform_transactions",
        conn_id = CONN_ID,
        sql     = "transactions/01_transform.sql",
    )

    # ── Task 4: Update is_fraud fatc_trx ──────────────────────
    update_fraud_transactions = SQLExecuteQueryOperator(
        task_id = "update_fraud_transactions",
        conn_id = CONN_ID,
        sql     = "transactions/01_update.sql",
    )

    ## FRAUD_LABELS ##

     # ── Task 1: DDL ───────────────────────────────────────────────────────────
    create_tables_fraud_labels = SQLExecuteQueryOperator(
        task_id = "create_tables_fraud_labels",
        conn_id = CONN_ID,
        sql     = DDL_STATEMENTS2,
    )

    # ── Task 2: Extract CSV → stg_fraud_labels ──────────────────────────────────
    @task()
    def extract_fraud_labels():
        from airflow.hooks.base import BaseHook

        conn     = BaseHook.get_connection(CONN_ID)
        conn_str = (
            f"postgresql+psycopg2://{conn.login}:{conn.password}"
            f"@{conn.host}:{conn.port}/{conn.schema}"
        )
        engine = create_engine(conn_str)

        df = pd.read_csv(SOURCE_FILE_FRAUD_LABELS)

        with engine.connect() as c:
            c.execute(text("TRUNCATE TABLE stg_fraud_labels"))
            c.commit()

        df.to_sql(
            name      = "stg_fraud_labels",
            con       = engine,
            if_exists = "append",
            index     = False,
            method    = "multi",
            chunksize = 1000,
        )
        engine.dispose()
        return len(df)

    # ── Task 3: Transform stg_customers → dim_customers ──────────────────────
    transform_fraud_labels = SQLExecuteQueryOperator(
        task_id = "transform_fraud_labels",
        conn_id = CONN_ID,
        sql     = "fraud_labels/01_transform.sql",
    )

    trx_extract = extract_load_transactions()
    fraud_extract = extract_fraud_labels()

    create_tables_transactions >> trx_extract >> transform_transactions

    create_tables_fraud_labels >> fraud_extract >> transform_fraud_labels

    [transform_transactions, transform_fraud_labels] >> update_fraud_transactions


dag = dag_etl_master_trx()


