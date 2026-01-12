#!/usr/bin/env python3
"""
Comprehensive pipeline verification and execution script.
Checks all services, runs the pipeline, and validates outputs.
"""
import subprocess
import sys
import time
from datetime import datetime

def run_cmd(cmd, description=""):
    """Run shell command and return success status."""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=60)
        if result.returncode == 0:
            return True, result.stdout
        else:
            return False, result.stderr
    except Exception as e:
        return False, str(e)

def verify_services():
    """Check if all required services are running."""
    print("\n" + "="*80)
    print("VERIFICATION: Checking Services")
    print("="*80)
    
    services = [
        ("airflow-postgres", "Airflow Metadata DB"),
        ("minio", "MinIO Object Storage"),
        ("warehouse-postgres", "Warehouse DB"),
        ("airflow-webserver", "Airflow Web UI"),
        ("airflow-scheduler", "Airflow Scheduler"),
        ("streamlit", "Streamlit Dashboard"),
    ]
    
    all_running = True
    for service_name, description in services:
        success, output = run_cmd(f'docker ps --filter "name={service_name}" --format "{{{{.Status}}}}"')
        if success and "Up" in output:
            print(f"✓ {description:.<40} Running")
        else:
            print(f"✗ {description:.<40} NOT RUNNING")
            all_running = False
    
    return all_running

def run_pipeline(process_date):
    """Execute the full ETL pipeline."""
    print("\n" + "="*80)
    print(f"PIPELINE: Processing date {process_date}")
    print("="*80)
    
    steps = [
        {
            "name": "Generate Events (Bronze)",
            "cmd": f'docker-compose exec -T airflow-init python3 -c "from data_gen.generate_events import main; main(\'{process_date}\')"'
        },
        {
            "name": "Transform & Aggregate (Silver → Gold)",
            "cmd": f'docker-compose exec -T airflow-init python3 -c "from spark_jobs.medallion_job import main; main(\'{process_date}\')"'
        },
        {
            "name": "Load to Postgres",
            "cmd": f'''docker-compose exec -T airflow-init python3 << 'EOFPY'
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from pyspark.sql import SparkSession

year = '{process_date}'[:4]
month = '{process_date}'[5:7]
day = '{process_date}'[8:10]

spark = (SparkSession.builder.appName('LoadToPostgres')
    .config('spark.hadoop.fs.s3a.endpoint', 'http://minio:9000')
    .config('spark.hadoop.fs.s3a.access.key', 'minioadmin')
    .config('spark.hadoop.fs.s3a.secret.key', 'minioadmin')
    .config('spark.hadoop.fs.s3a.path.style.access', 'true')
    .getOrCreate())

gold_path = f's3a://gold/daily_metrics/year={{year}}/month={{month}}/day={{day}}/'
gold_df = spark.read.parquet(gold_path)
pdf = gold_df.toPandas()
spark.stop()

pdf['metric_date'] = pd.to_datetime(pdf['event_date']).dt.date
pdf = pdf.drop(columns=['event_date'])

hook = PostgresHook(postgres_conn_id='warehouse_postgres')
engine = hook.get_sqlalchemy_engine()

with engine.begin() as conn:
    conn.execute('CREATE TABLE IF NOT EXISTS daily_metrics (metric_date date, daily_active_users int, total_revenue numeric, top_product_id int, top_product_revenue numeric, created_at timestamp default now());')
    conn.execute('DELETE FROM daily_metrics WHERE metric_date = %s', ('{process_date}',))
    if not pdf.empty:
        pdf.to_sql('daily_metrics', con=conn, if_exists='append', index=False)
        print(f'Loaded {{len(pdf)}} row(s)')
EOFPY'''
        },
        {
            "name": "Verify Postgres Data",
            "cmd": f'''docker-compose exec -T warehouse-postgres psql -U warehouse -d warehouse -c "SELECT metric_date, daily_active_users, ROUND(total_revenue::numeric,2) as revenue, top_product_id FROM daily_metrics WHERE metric_date = '{process_date}';"'''
        }
    ]
    
    success_count = 0
    for i, step in enumerate(steps, 1):
        print(f"\n[{i}/{len(steps)}] {step['name']}...")
        success, output = run_cmd(step['cmd'])
        
        if success:
            print(f"✓ {step['name']} completed")
            if output.strip():
                # Print first 500 chars of output
                print(f"   Output: {output.strip()[:500]}")
            success_count += 1
        else:
            print(f"✗ {step['name']} FAILED")
            print(f"   Error: {output.strip()[:300]}")
    
    return success_count == len(steps)

def verify_outputs(process_date):
    """Verify that data was loaded correctly."""
    print("\n" + "="*80)
    print("VALIDATION: Checking Outputs")
    print("="*80)
    
    # Check Bronze
    success, output = run_cmd(f'docker-compose exec minio mc ls local/bronze/raw/events/date={process_date}/ --recursive')
    if success:
        print(f"✓ Bronze layer has files for {process_date}")
    else:
        print(f"✗ Bronze layer empty for {process_date}")
    
    # Check Silver
    year = process_date[:4]
    month = process_date[5:7]
    day = process_date[8:10]
    success, output = run_cmd(f'docker-compose exec minio mc ls local/silver/events/year={year}/month={month}/day={day}/ --recursive')
    if success:
        print(f"✓ Silver layer has files for {process_date}")
    else:
        print(f"✗ Silver layer empty for {process_date}")
    
    # Check Gold
    success, output = run_cmd(f'docker-compose exec minio mc ls local/gold/daily_metrics/year={year}/month={month}/day={day}/ --recursive')
    if success:
        print(f"✓ Gold layer has files for {process_date}")
    else:
        print(f"✗ Gold layer empty for {process_date}")
    
    # Check Postgres
    success, output = run_cmd(f'docker-compose exec -T warehouse-postgres psql -U warehouse -d warehouse -c "SELECT COUNT(*) FROM daily_metrics WHERE metric_date = \'{process_date}\';"')
    if success and "1" in output:
        print(f"✓ Data loaded to Postgres warehouse")
    else:
        print(f"✗ Postgres data missing")

def main():
    process_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    
    print(f"\n{'='*80}")
    print(f"ETL Pipeline - Verification & Execution")
    print(f"Date: {process_date}")
    print(f"{'='*80}")
    
    # Step 1: Verify services
    if not verify_services():
        print("\n⚠ WARNING: Some services are not running!")
        print("   Run: docker-compose up -d")
        return 1
    
    # Step 2: Run pipeline
    print("\nStarting pipeline execution...")
    time.sleep(2)
    
    if not run_pipeline(process_date):
        print("\n✗ Pipeline execution failed")
        return 1
    
    # Step 3: Verify outputs
    verify_outputs(process_date)
    
    print(f"\n{'='*80}")
    print("✓ Pipeline Verification Complete!")
    print(f"{'='*80}")
    print("\nAccess Results:")
    print("  • Dashboard:     http://localhost:8501")
    print("  • Airflow UI:    http://localhost:8082")
    print("  • MinIO Console: http://localhost:9001")
    print("  • Postgres:      localhost:5433 (user: warehouse)")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
