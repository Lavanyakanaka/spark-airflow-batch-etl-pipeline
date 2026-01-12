#!/usr/bin/env python3
"""
Automated ETL Pipeline Runner
Executes the complete Medallion architecture pipeline for a given date.
"""
import subprocess
import sys
import time
from datetime import datetime

def run_command(cmd, description, show_output=True):
    """Execute shell command and report status."""
    print(f"\n{'='*80}")
    print(f"▶ {description}")
    print(f"{'='*80}")
    
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=not show_output,
            text=True,
            timeout=300
        )
        if result.returncode == 0:
            print(f"✓ {description} completed successfully")
            if result.stdout and not show_output:
                print(result.stdout[:500])  # Print first 500 chars if captured
            return True
        else:
            print(f"✗ {description} failed with exit code {result.returncode}")
            if result.stderr:
                print(f"Error: {result.stderr[:500]}")
            return False
    except subprocess.TimeoutExpired:
        print(f"✗ {description} timed out after 5 minutes")
        return False
    except Exception as e:
        print(f"✗ {description} failed: {str(e)}")
        return False

def main():
    if len(sys.argv) > 1:
        process_date = sys.argv[1]
    else:
        process_date = datetime.now().strftime("%Y-%m-%d")
    
    print(f"\n{'='*80}")
    print(f"ETL Pipeline Runner - Processing date: {process_date}")
    print(f"{'='*80}")
    
    steps = [
        (
            f"docker-compose exec -T airflow-init python3 -c \"from data_gen.generate_events import main; main('{process_date}')\"",
            "1. Generate & Upload Events to Bronze Layer",
            True
        ),
        (
            f"docker-compose exec -T airflow-init python3 -c \"from spark_jobs.medallion_job import main; main('{process_date}')\"",
            "2. Transform to Silver & Aggregate to Gold",
            True
        ),
        (
            f"docker-compose exec -T airflow-init bash -c \"python3 -c \\\"from airflow.providers.postgres.hooks.postgres import PostgresHook; import pandas as pd; from pyspark.sql import SparkSession; spark = SparkSession.builder.appName('LoadToPostgres').config('spark.hadoop.fs.s3a.endpoint', 'http://minio:9000').config('spark.hadoop.fs.s3a.access.key', 'minioadmin').config('spark.hadoop.fs.s3a.secret.key', 'minioadmin').config('spark.hadoop.fs.s3a.path.style.access', 'true').getOrCreate(); year='{process_date[:4]}'; month='{process_date[5:7]}'; day='{process_date[8:10]}'; gold_path=f's3a://gold/daily_metrics/year={{year}}/month={{month}}/day={{day}}/'; gold_df=spark.read.parquet(gold_path); pdf=gold_df.toPandas(); spark.stop(); pdf['metric_date']=pd.to_datetime(pdf['event_date']).dt.date; pdf=pdf.drop(columns=['event_date']); hook=PostgresHook(postgres_conn_id='warehouse_postgres'); engine=hook.get_sqlalchemy_engine(); conn=engine.begin(); conn.execute('CREATE TABLE IF NOT EXISTS daily_metrics (metric_date date, daily_active_users int, total_revenue numeric, top_product_id int, top_product_revenue numeric, created_at timestamp default now());'); conn.execute('DELETE FROM daily_metrics WHERE metric_date = %s', ('{process_date}',)); pdf.to_sql('daily_metrics', con=conn, if_exists='append', index=False); conn.close(); print(f'Loaded {{len(pdf)}} rows')\\\"\"",
            "3. Load Aggregates to Postgres Data Mart",
            True
        ),
        (
            f"docker-compose exec -T warehouse-postgres psql -U warehouse -d warehouse -c \"SELECT COUNT(*) as row_count FROM daily_metrics WHERE metric_date = '{process_date}';\"",
            "4. Verify Data Loaded to Postgres",
            True
        ),
    ]
    
    success_count = 0
    for cmd, desc, show in steps:
        if run_command(cmd, desc, show):
            success_count += 1
        else:
            print(f"⚠ Pipeline step failed. Stopping execution.")
            break
    
    print(f"\n{'='*80}")
    print(f"Pipeline Summary: {success_count}/{len(steps)} steps completed")
    print(f"{'='*80}")
    
    if success_count == len(steps):
        print("✓ Full pipeline executed successfully!")
        print(f"\nNext: View results at http://localhost:8501 (Streamlit Dashboard)")
        return 0
    else:
        print(f"✗ Pipeline incomplete. Check errors above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
