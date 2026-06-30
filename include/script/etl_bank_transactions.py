"""
etl_bank_transactions.py
=========================
ETL script: baca bank_transactions_data_2.csv → transform → load ke PostgreSQL tabel trx_sample.

Cara pakai standalone:
    python etl_bank_transactions.py

Dipanggil juga oleh DAG: dag_etl_bank_transactions
"""

import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
DATASET_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "dataset")
SOURCE_FILE = os.path.join(DATASET_DIR, "bank_transactions_data_2.csv")

TABLE_NAME = "trx_sample"


# Conn string dibaca saat runtime (bukan saat import)
# supaya DAG parser Airflow tidak error saat env vars belum di-set
def _get_conn_str() -> str:
    user   = os.getenv("ETL_POSTGRES_USER")
    passwd = os.getenv("ETL_POSTGRES_PASSWORD")
    host   = os.getenv("ETL_POSTGRES_HOST")
    port   = os.getenv("ETL_POSTGRES_PORT")
    dbname = os.getenv("ETL_POSTGRES_DB")

    missing = [k for k, v in {
        "ETL_POSTGRES_USER"    : user,
        "ETL_POSTGRES_PASSWORD": passwd,
        "ETL_POSTGRES_HOST"    : host,
        "ETL_POSTGRES_PORT"    : port,
        "ETL_POSTGRES_DB"      : dbname,
    }.items() if not v]
    if missing:
        raise EnvironmentError(f"Env vars berikut belum di-set: {', '.join(missing)}")

    return f"postgresql+psycopg2://{user}:{passwd}@{host}:{port}/{dbname}"


# ─── DDL ──────────────────────────────────────────────────────────────────────
DDL_CREATE_TABLE = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    transaction_id          VARCHAR(20)     PRIMARY KEY,
    account_id              VARCHAR(20),
    transaction_amount      NUMERIC(18, 2),
    transaction_date        TIMESTAMP,
    transaction_type        VARCHAR(50),
    location                VARCHAR(100),
    device_id               VARCHAR(20),
    ip_address              VARCHAR(45),
    merchant_id             VARCHAR(20),
    channel                 VARCHAR(50),
    customer_age            SMALLINT,
    customer_occupation     VARCHAR(100),
    transaction_duration    INTEGER,
    login_attempts          SMALLINT,
    account_balance         NUMERIC(18, 2),
    previous_transaction_date TIMESTAMP,
    etl_loaded_at           TIMESTAMP       DEFAULT NOW()
);
"""


# ─── Extract ──────────────────────────────────────────────────────────────────
def extract() -> pd.DataFrame:
    log.info(f"[EXTRACT] Membaca file: {SOURCE_FILE}")
    df = pd.read_csv(SOURCE_FILE)
    log.info(f"[EXTRACT] {len(df):,} baris dibaca, {len(df.columns)} kolom")
    return df


# ─── Transform ────────────────────────────────────────────────────────────────
def transform(df: pd.DataFrame) -> pd.DataFrame:
    log.info("[TRANSFORM] Mulai transformasi ...")

    # Rename kolom → snake_case
    df = df.rename(columns={
        "TransactionID"           : "transaction_id",
        "AccountID"               : "account_id",
        "TransactionAmount"       : "transaction_amount",
        "TransactionDate"         : "transaction_date",
        "TransactionType"         : "transaction_type",
        "Location"                : "location",
        "DeviceID"                : "device_id",
        "IP Address"              : "ip_address",
        "MerchantID"              : "merchant_id",
        "Channel"                 : "channel",
        "CustomerAge"             : "customer_age",
        "CustomerOccupation"      : "customer_occupation",
        "TransactionDuration"     : "transaction_duration",
        "LoginAttempts"           : "login_attempts",
        "AccountBalance"          : "account_balance",
        "PreviousTransactionDate" : "previous_transaction_date",
    })

    # Cast tipe data
    df["transaction_date"]          = pd.to_datetime(df["transaction_date"],          errors="coerce")
    df["previous_transaction_date"] = pd.to_datetime(df["previous_transaction_date"], errors="coerce")
    df["transaction_amount"]        = pd.to_numeric(df["transaction_amount"],  errors="coerce")
    df["account_balance"]           = pd.to_numeric(df["account_balance"],     errors="coerce")
    df["customer_age"]              = pd.to_numeric(df["customer_age"],        errors="coerce").astype("Int16")
    df["login_attempts"]            = pd.to_numeric(df["login_attempts"],      errors="coerce").astype("Int16")
    df["transaction_duration"]      = pd.to_numeric(df["transaction_duration"],errors="coerce").astype("Int32")

    # Hapus duplikat berdasarkan PK
    before = len(df)
    df = df.drop_duplicates(subset=["transaction_id"])
    after = len(df)
    if before != after:
        log.warning(f"[TRANSFORM] Duplikat dihapus: {before - after} baris")

    # Hapus baris yang transaction_id-nya null
    df = df.dropna(subset=["transaction_id"])

    log.info(f"[TRANSFORM] Selesai — {len(df):,} baris siap di-load")
    return df


# ─── Load ─────────────────────────────────────────────────────────────────────
def load(df: pd.DataFrame) -> int:
    conn_str = _get_conn_str()
    log.info(f"[LOAD] Koneksi ke database ...")
    engine = create_engine(conn_str)

    with engine.connect() as conn:
        conn.execute(text(DDL_CREATE_TABLE))
        conn.commit()
        log.info(f"[LOAD] Tabel '{TABLE_NAME}' siap")

    # Upsert: truncate + insert (idempotent)
    with engine.connect() as conn:
        conn.execute(text(f"TRUNCATE TABLE {TABLE_NAME}"))
        conn.commit()

    df.to_sql(
        name      = TABLE_NAME,
        con       = engine,
        if_exists = "append",
        index     = False,
        method    = "multi",
        chunksize = 1000,
    )

    log.info(f"[LOAD] {len(df):,} baris berhasil di-load ke tabel '{TABLE_NAME}'")
    engine.dispose()
    return len(df)


# ─── Main ─────────────────────────────────────────────────────────────────────
def run_etl() -> dict:
    log.info("=" * 55)
    log.info("  ETL Bank Transactions — START")
    log.info("=" * 55)

    df_raw        = extract()
    df_clean      = transform(df_raw)
    rows_loaded   = load(df_clean)

    result = {
        "source_file"  : SOURCE_FILE,
        "rows_extracted": len(df_raw),
        "rows_loaded"  : rows_loaded,
        "target_table" : TABLE_NAME,
    }

    log.info("=" * 55)
    log.info(f"  ETL SELESAI — {rows_loaded:,} baris ke '{TABLE_NAME}'")
    log.info("=" * 55)
    return result


if __name__ == "__main__":
    run_etl()
