from __future__ import annotations

import io
from datetime import datetime, timedelta

import polars as pl
import trino
from minio import Minio

from airflow import DAG
from airflow.operators.python import PythonOperator

MINIO_ENDPOINT   = "minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio1234"

LANDING_BUCKET   = "bck-landing"
BRONZE_BUCKET    = "bck-bronze"

CSV_LOCAL_PATH   = "/opt/airflow/dags/data_prueba_tecnica.csv"
CSV_MINIO_KEY    = "data/data_prueba_tecnica.csv"
PARQUET_KEY      = "master/data_prueba_tecnica.parquet"

TRINO_HOST       = "coordinator"
TRINO_PORT       = 8080
TRINO_USER       = "root"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

def _minio() -> Minio:
    return Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY,
                 secret_key=MINIO_SECRET_KEY, secure=False)


def _trino_cursor():
    conn = trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER)
    return conn, conn.cursor()


def ingest_to_landing() -> None:
    client = _minio()

    for bucket in (LANDING_BUCKET, BRONZE_BUCKET):
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            print(f"Bucket created: {bucket}")

    client.fput_object(LANDING_BUCKET, CSV_MINIO_KEY, CSV_LOCAL_PATH)
    print(f"Uploaded: {CSV_LOCAL_PATH} → {LANDING_BUCKET}/{CSV_MINIO_KEY}")


def clean_transform_save() -> None:
    client = _minio()

    response = client.get_object(LANDING_BUCKET, CSV_MINIO_KEY)
    df = pl.read_csv(io.BytesIO(response.read()))
    df.columns = [col.strip() for col in df.columns]
    print(f"Rows read from landing: {len(df)}")
    print(f"Null counts:\n{df.null_count()}")

    df = df.filter(
        pl.col("id").is_not_null() & (pl.col("id").str.len_chars() > 0)
    )

    df = df.with_columns(
        pl.col("name").str.strip_chars().str.to_lowercase(),
        pl.col("company_id").str.strip_chars(),
        pl.col("status").str.strip_chars().str.to_lowercase(),
    )

    before = len(df)
    df = df.unique(subset=["id"], keep="first")
    print(f"Duplicates removed: {before - len(df)}")

    df = df.with_columns(pl.col("amount").cast(pl.Float64))
    df = df.filter(pl.col("amount") > 0)

    q1 = df["amount"].quantile(0.25)
    q3 = df["amount"].quantile(0.75)
    iqr = q3 - q1
    lower, upper = q1 - 3 * iqr, q3 + 3 * iqr
    before = len(df)
    df = df.filter((pl.col("amount") >= lower) & (pl.col("amount") <= upper))
    print(f"Outliers removed: {before - len(df)}  (range [{lower:.2f}, {upper:.2f}])")

    df = df.with_columns(
        pl.col("created_at").str.strptime(pl.Date, "%Y-%m-%d", strict=False),
        pl.col("paid_at").str.strptime(pl.Date, "%Y-%m-%d", strict=False),
    )

    df = df.with_columns(
        pl.when(
            pl.col("paid_at").is_not_null() & (pl.col("paid_at") < pl.col("created_at"))
        )
        .then(pl.col("created_at"))
        .otherwise(pl.col("paid_at"))
        .alias("paid_at")
    )

    valid_statuses = {"paid", "voided", "pending_payment", "pre_authorized", "refunded"}
    df = df.filter(pl.col("status").is_in(list(valid_statuses)))

    agg = df.group_by(["name", "created_at"]).agg(
        pl.len().alias("daily_tx_count"),
        pl.col("amount").sum().alias("daily_total_amount"),
        pl.col("amount").mean().alias("daily_avg_amount"),
        pl.col("amount").max().alias("daily_max_amount"),
    )
    df = df.join(agg, on=["name", "created_at"], how="left")

    print(f"Rows after full cleaning: {len(df)}")
    print(df.describe())

    buf = io.BytesIO()
    df.write_parquet(buf)
    buf.seek(0)
    data = buf.getvalue()

    client.put_object(
        BRONZE_BUCKET,
        PARQUET_KEY,
        io.BytesIO(data),
        length=len(data),
        content_type="application/octet-stream",
    )
    print(f"Parquet saved → {BRONZE_BUCKET}/{PARQUET_KEY}  ({len(data):,} bytes)")


def create_trino_table() -> None:
    conn, cur = _trino_cursor()

    cur.execute(
        "CREATE SCHEMA IF NOT EXISTS bronze.prueba "
        "WITH (location = 's3a://bck-bronze/master/')"
    )
    print("Schema bronze.prueba ready.")

    cur.execute("DROP TABLE IF EXISTS bronze.prueba.tbl_data")

    cur.execute("""
        CREATE TABLE bronze.prueba.tbl_data (
            id                 VARCHAR,
            name               VARCHAR,
            company_id         VARCHAR,
            amount             DOUBLE,
            status             VARCHAR,
            created_at         DATE,
            paid_at            DATE,
            daily_tx_count     BIGINT,
            daily_total_amount DOUBLE,
            daily_avg_amount   DOUBLE,
            daily_max_amount   DOUBLE
        )
        WITH (
            external_location = 's3a://bck-bronze/master/',
            format = 'PARQUET'
        )
    """)
    print("Table bronze.prueba.tbl_data created.")

    cur.execute("SELECT COUNT(*) FROM bronze.prueba.tbl_data")
    count = cur.fetchone()[0]
    print(f"Row count in Trino: {count}")
    conn.close()


with DAG(
    dag_id="etl_engineer_challenge",
    default_args=default_args,
    description="ETL: CSV → MinIO Landing → Bronze Parquet → Trino",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "challenge"],
) as dag:

    task_ingest = PythonOperator(
        task_id="ingest_to_landing",
        python_callable=ingest_to_landing,
    )

    task_clean = PythonOperator(
        task_id="clean_transform_save",
        python_callable=clean_transform_save,
    )

    task_trino = PythonOperator(
        task_id="create_trino_table",
        python_callable=create_trino_table,
    )

    task_ingest >> task_clean >> task_trino
