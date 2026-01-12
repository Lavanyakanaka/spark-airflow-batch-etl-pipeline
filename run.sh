#!/bin/bash
# Quick ETL Pipeline Runner - Single command execution

set -e

PROCESS_DATE="${1:-$(date +%Y-%m-%d)}"

echo "=================================================="
echo "ETL Pipeline - Processing: $PROCESS_DATE"
echo "=================================================="

echo -e "\n[1/4] Generating events to Bronze layer..."
docker-compose exec -T airflow-init python3 -c \
  "from data_gen.generate_events import main; main('$PROCESS_DATE'); print('✓ Bronze loaded')"

echo -e "\n[2/4] Transforming to Silver & Aggregating to Gold..."
docker-compose exec -T airflow-init python3 -c \
  "from spark_jobs.medallion_job import main; main('$PROCESS_DATE'); print('✓ Silver & Gold ready')"

echo -e "\n[3/4] Loading aggregates to Postgres..."
docker-compose exec -T airflow-init python3 << PYTHON_SCRIPT
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from pyspark.sql import SparkSession

year = '$PROCESS_DATE'[:4]
month = '$PROCESS_DATE'[5:7]
day = '$PROCESS_DATE'[8:10]

spark = (SparkSession.builder.appName('LoadToPostgres')
    .config('spark.hadoop.fs.s3a.endpoint', 'http://minio:9000')
    .config('spark.hadoop.fs.s3a.access.key', 'minioadmin')
    .config('spark.hadoop.fs.s3a.secret.key', 'minioadmin')
    .config('spark.hadoop.fs.s3a.path.style.access', 'true')
    .getOrCreate())

gold_path = f's3a://gold/daily_metrics/year={year}/month={month}/day={day}/'
gold_df = spark.read.parquet(gold_path)
pdf = gold_df.toPandas()
spark.stop()

pdf['metric_date'] = pd.to_datetime(pdf['event_date']).dt.date
pdf = pdf.drop(columns=['event_date'])

hook = PostgresHook(postgres_conn_id='warehouse_postgres')
engine = hook.get_sqlalchemy_engine()

with engine.begin() as conn:
    conn.execute('CREATE TABLE IF NOT EXISTS daily_metrics (metric_date date, daily_active_users int, total_revenue numeric, top_product_id int, top_product_revenue numeric, created_at timestamp default now());')
    conn.execute('DELETE FROM daily_metrics WHERE metric_date = %s', ('$PROCESS_DATE',))
    if not pdf.empty:
        pdf.to_sql('daily_metrics', con=conn, if_exists='append', index=False)
        print(f'✓ Loaded {len(pdf)} row(s)')
PYTHON_SCRIPT

echo -e "\n[4/4] Verifying data in Postgres..."
docker-compose exec -T warehouse-postgres psql -U warehouse -d warehouse -c \
  "SELECT metric_date, daily_active_users, total_revenue, top_product_id FROM daily_metrics WHERE metric_date = '$PROCESS_DATE' LIMIT 3;"

echo -e "\n=================================================="
echo "✓ Pipeline Complete!"
echo "=================================================="
echo -e "\nNext steps:"
echo "  • Dashboard: http://localhost:8501"
echo "  • Airflow: http://localhost:8082 (admin/admin)"
echo "  • MinIO: http://localhost:9001 (minioadmin/minioadmin)"
