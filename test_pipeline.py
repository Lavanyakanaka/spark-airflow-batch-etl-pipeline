#!/usr/bin/env python
"""
Standalone test script to run the entire ETL pipeline for a specific date.
"""
import sys
from datetime import datetime

# Generate bronze data
print("=" * 80)
print("STEP 1: Generate Events (Bronze)")
print("=" * 80)
from data_gen.generate_events import main as gen_main
process_date = "2026-01-12"
gen_main(process_date)
print(f"✓ Generated and uploaded events for {process_date} to bronze bucket")

# Run Spark medallion job (silver + gold)
print("\n" + "=" * 80)
print("STEP 2: Transform & Aggregate (Silver & Gold)")
print("=" * 80)
from spark_jobs.medallion_job import main as medallion_main
medallion_main(process_date)
print(f"✓ Transformed {process_date} data to silver and aggregated to gold")

# Load aggregates to Postgres
print("\n" + "=" * 80)
print("STEP 3: Load Aggregates to Postgres")
print("=" * 80)
from pyspark.sql import SparkSession
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

year = process_date[:4]
month = process_date[5:7]
day = process_date[8:10]

spark = (
    SparkSession.builder.appName("LoadToPostgres")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate()
)

gold_path = f"s3a://gold/daily_metrics/year={year}/month={month}/day={day}/"
gold_df = spark.read.parquet(gold_path)
print(f"✓ Read gold layer from: {gold_path}")

pdf = gold_df.toPandas()
spark.stop()

pdf["metric_date"] = pd.to_datetime(pdf["event_date"]).dt.date
pdf = pdf.drop(columns=["event_date"])

hook = PostgresHook(postgres_conn_id="warehouse_postgres")
engine = hook.get_sqlalchemy_engine()

with engine.begin() as conn:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS daily_metrics ("
        "metric_date date,"
        "daily_active_users int,"
        "total_revenue numeric,"
        "top_product_id int,"
        "top_product_revenue numeric,"
        "created_at timestamp default now()"
        ");"
    )
    conn.execute(
        "DELETE FROM daily_metrics WHERE metric_date = %s",
        (process_date,),
    )
    if not pdf.empty:
        pdf.to_sql(
            "daily_metrics",
            con=conn,
            if_exists="append",
            index=False,
        )
        print(f"✓ Loaded {len(pdf)} row(s) into daily_metrics table")
    else:
        print("⚠ Warning: Gold dataframe was empty")

print("\n" + "=" * 80)
print("PIPELINE COMPLETE")
print("=" * 80)
