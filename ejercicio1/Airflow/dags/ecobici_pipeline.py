from __future__ import annotations

import io
from datetime import datetime, timedelta

import polars as pl
import trino
from minio import Minio

from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Config ───────────────────────────────────────────────────────────────────
MINIO_ENDPOINT   = "minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio1234"
BRONZE_BUCKET    = "bck-bronze"
LOCAL_CSV        = "/opt/airflow/dags/ecobici_data.csv"
TRINO_HOST       = "coordinator"
TRINO_PORT       = 8080
TRINO_USER       = "root"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── Helpers ──────────────────────────────────────────────────────────────────
def _minio() -> Minio:
    return Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY,
                 secret_key=MINIO_SECRET_KEY, secure=False)


# ── Task 1: Read, Clean, Save partitioned parquets ───────────────────────────
def read_clean_save() -> None:
    client = _minio()

    # ── Read CSV ─────────────────────────────────────────────────────────────
    df = pl.read_csv(LOCAL_CSV, infer_schema_length=10000, ignore_errors=True)

    # Normalize column names: strip \r, lowercase, spaces → _
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Fix inconsistent column name: ciclo_estacionarribo → ciclo_estacion_arribo
    rename_map = {c: c.replace("ciclo_estacionarribo", "ciclo_estacion_arribo")
                  for c in df.columns if "ciclo_estacionarribo" in c}
    if rename_map:
        df = df.rename(rename_map)

    print(f"Rows read      : {len(df)}")
    print(f"Columns        : {df.columns}")
    print(f"Null counts    :\n{df.null_count()}")

    # ── Clean ────────────────────────────────────────────────────────────────

    # 1. Drop rows with nulls in key columns
    for col in ["genero_usuario", "fecha_retiro", "hora_retiro",
                "ciclo_estacion_retiro", "ciclo_estacion_arribo"]:
        if col in df.columns:
            df = df.filter(
                pl.col(col).is_not_null() &
                (pl.col(col).cast(pl.Utf8).str.len_chars() > 0)
            )

    # 2. Gender: keep only M / F, strip whitespace
    df = df.with_columns(pl.col("genero_usuario").str.strip_chars().str.to_uppercase())
    before = len(df)
    df = df.filter(pl.col("genero_usuario").is_in(["M", "F"]))
    print(f"Invalid gender removed : {before - len(df)}")

    # 3. Age: cast to int, keep valid range 15–90
    df = df.with_columns(pl.col("edad_usuario").cast(pl.Int32, strict=False))
    before = len(df)
    df = df.filter(
        pl.col("edad_usuario").is_not_null() &
        (pl.col("edad_usuario") >= 15) &
        (pl.col("edad_usuario") <= 90)
    )
    print(f"Invalid age removed    : {before - len(df)}")

    # 4. Parse dates DD/MM/YYYY
    df = df.with_columns(
        pl.col("fecha_retiro").str.strptime(pl.Date, "%d/%m/%Y", strict=False),
        pl.col("fecha_arribo").str.strptime(pl.Date, "%d/%m/%Y", strict=False),
    )
    before = len(df)
    df = df.filter(pl.col("fecha_retiro").is_not_null())
    print(f"Unparseable dates removed: {before - len(df)}")

    # 5. Compute trip duration in minutes from timestamps
    df = df.with_columns(
        pl.concat_str([pl.col("fecha_retiro").cast(pl.Utf8),
                       pl.lit(" "), pl.col("hora_retiro")])
          .str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False)
          .alias("dt_retiro"),
        pl.concat_str([pl.col("fecha_arribo").cast(pl.Utf8),
                       pl.lit(" "), pl.col("hora_arribo")])
          .str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False)
          .alias("dt_arribo"),
    )
    df = df.with_columns(
        ((pl.col("dt_arribo") - pl.col("dt_retiro")).dt.total_seconds() / 60.0)
        .alias("duracion_minutos")
    )
    # Remove trips with negative duration or longer than 24 h (outliers / bad data)
    before = len(df)
    df = df.filter(
        pl.col("duracion_minutos").is_not_null() &
        (pl.col("duracion_minutos") >= 0) &
        (pl.col("duracion_minutos") <= 1440)
    )
    print(f"Invalid duration removed : {before - len(df)}")
    df = df.drop(["dt_retiro", "dt_arribo"])

    # 6. Remove duplicate trips (same bike + departure station + date + time)
    before = len(df)
    df = df.unique(subset=["bici", "ciclo_estacion_retiro", "fecha_retiro", "hora_retiro"])
    print(f"Duplicates removed     : {before - len(df)}")

    # 7. Add partition columns (year / month based on departure date)
    df = df.with_columns(
        pl.col("fecha_retiro").dt.year().cast(pl.Utf8).alias("year"),
        pl.col("fecha_retiro").dt.month().cast(pl.Utf8).str.zfill(2).alias("month"),
    )

    print(f"Rows after cleaning    : {len(df)}")

    # ── Save partitioned parquets with append ─────────────────────────────────
    # Strategy: bck-bronze/ecobici/year=YYYY/month=MM/ecobici.parquet
    # Only ONE parquet per partition — existing data is read, merged, deduped, rewritten
    if not client.bucket_exists(BRONZE_BUCKET):
        client.make_bucket(BRONZE_BUCKET)

    partitions = df.select(["year", "month"]).unique().to_dicts()
    for p in partitions:
        year, month = p["year"], p["month"]
        partition_df = df.filter(
            (pl.col("year") == year) & (pl.col("month") == month)
        ).drop(["year", "month"])   # year/month live in the path, not inside the file

        _append_partition(client, partition_df, year, month)


def _append_partition(client: Minio, new_df: pl.DataFrame,
                      year: str, month: str) -> None:
    """Read existing parquet → append new rows → deduplicate → write back (single file)."""
    key = f"ecobici/year={year}/month={month}/ecobici.parquet"

    try:
        resp     = client.get_object(BRONZE_BUCKET, key)
        existing = pl.read_parquet(io.BytesIO(resp.read()))
        combined = pl.concat([existing, new_df]).unique(
            subset=["bici", "ciclo_estacion_retiro", "fecha_retiro", "hora_retiro"]
        )
        print(f"  [{year}/{month}] {len(existing)} existing + {len(new_df)} new → {len(combined)} total")
    except Exception:
        combined = new_df
        print(f"  [{year}/{month}] New partition with {len(new_df)} rows")

    buf  = io.BytesIO()
    combined.write_parquet(buf)
    buf.seek(0)
    data = buf.getvalue()

    client.put_object(BRONZE_BUCKET, key, io.BytesIO(data),
                      length=len(data), content_type="application/octet-stream")
    print(f"  Saved → {BRONZE_BUCKET}/{key}  ({len(data):,} bytes)")


# ── Task 2: Register / refresh Trino table ───────────────────────────────────
def register_trino() -> None:
    conn = trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER)
    cur  = conn.cursor()

    cur.execute(
        "CREATE SCHEMA IF NOT EXISTS bronze.ecobici "
        "WITH (location = 's3a://bck-bronze/ecobici/')"
    )
    print("Schema bronze.ecobici ready.")

    cur.execute("DROP TABLE IF EXISTS bronze.ecobici.tbl_viajes")

    # Partition columns (year, month) must go LAST and match partitioned_by
    cur.execute("""
        CREATE TABLE bronze.ecobici.tbl_viajes (
            genero_usuario        VARCHAR,
            edad_usuario          INTEGER,
            bici                  VARCHAR,
            ciclo_estacion_retiro VARCHAR,
            fecha_retiro          DATE,
            hora_retiro           VARCHAR,
            ciclo_estacion_arribo VARCHAR,
            fecha_arribo          DATE,
            hora_arribo           VARCHAR,
            duracion_minutos      DOUBLE,
            year                  VARCHAR,
            month                 VARCHAR
        )
        WITH (
            external_location = 's3a://bck-bronze/ecobici/',
            format            = 'PARQUET',
            partitioned_by    = ARRAY['year', 'month']
        )
    """)
    print("Table bronze.ecobici.tbl_viajes created.")

    # Sync partition metadata so Trino discovers the year=/month= directories
    try:
        cur.execute(
            "CALL bronze.system.sync_partition_metadata("
            "schema_name => 'ecobici', table_name => 'tbl_viajes', mode => 'FULL')"
        )
        print("Partition metadata synced.")
    except Exception as e:
        print(f"sync_partition_metadata skipped: {e}")

    cur.execute("SELECT COUNT(*) FROM bronze.ecobici.tbl_viajes")
    print(f"Row count in Trino: {cur.fetchone()[0]}")
    conn.close()


# ── DAG definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="ecobici_pipeline",
    default_args=default_args,
    description="Ecobici CDMX: CSV → Bronze parquet particionado (append) → Trino",
    schedule="*/30 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecobici", "adicional"],
) as dag:

    task_process = PythonOperator(
        task_id="read_clean_save",
        python_callable=read_clean_save,
    )

    task_trino = PythonOperator(
        task_id="register_trino",
        python_callable=register_trino,
    )

    task_process >> task_trino
