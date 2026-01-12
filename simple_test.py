#!/usr/bin/env python3
"""
Simple step-by-step pipeline test
"""
print("Step 1: Testing imports...")
try:
    from data_gen.generate_events import main as gen_main
    print("  ✓ data_gen import OK")
except Exception as e:
    print(f"  ✗ data_gen import failed: {e}")
    exit(1)

try:
    from spark_jobs.medallion_job import main as medallion_main
    print("  ✓ spark_jobs import OK")
except Exception as e:
    print(f"  ✗ spark_jobs import failed: {e}")
    exit(1)

print("\nStep 2: Testing event generation...")
try:
    gen_main("2026-01-12")
    print("  ✓ Events generated OK")
except Exception as e:
    print(f"  ✗ Event generation failed: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

print("\nStep 3: Testing Spark transformation...")
try:
    medallion_main("2026-01-12")
    print("  ✓ Spark job completed OK")
except Exception as e:
    print(f"  ✗ Spark job failed: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

print("\nStep 4: Testing Postgres loading...")
try:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    import pandas as pd
    from pyspark.sql import SparkSession
    
    year = "2026"
    month = "01"
    day = "12"
    
    spark = (SparkSession.builder.appName('LoadTest')
        .config('spark.hadoop.fs.s3a.endpoint', 'http://minio:9000')
        .config('spark.hadoop.fs.s3a.access.key', 'minioadmin')
        .config('spark.hadoop.fs.s3a.secret.key', 'minioadmin')
        .config('spark.hadoop.fs.s3a.path.style.access', 'true')
        .getOrCreate())
    
    gold_path = f's3a://gold/daily_metrics/year={year}/month={month}/day={day}/'
    gold_df = spark.read.parquet(gold_path)
    pdf = gold_df.toPandas()
    spark.stop()
    
    print(f"  ✓ Read {len(pdf)} rows from gold layer")
    
    pdf['metric_date'] = pd.to_datetime(pdf['event_date']).dt.date
    pdf = pdf.drop(columns=['event_date'])
    
    hook = PostgresHook(postgres_conn_id='warehouse_postgres')
    engine = hook.get_sqlalchemy_engine()
    
    with engine.begin() as conn:
        conn.execute('CREATE TABLE IF NOT EXISTS daily_metrics (metric_date date, daily_active_users int, total_revenue numeric, top_product_id int, top_product_revenue numeric, created_at timestamp default now());')
        conn.execute('DELETE FROM daily_metrics WHERE metric_date = %s', ('2026-01-12',))
        if not pdf.empty:
            pdf.to_sql('daily_metrics', con=conn, if_exists='append', index=False)
            print(f"  ✓ Loaded {len(pdf)} row(s) to Postgres")
        else:
            print(f"  ✗ Gold dataframe was empty")
            exit(1)
    
except Exception as e:
    print(f"  ✗ Postgres load failed: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

print("\n" + "="*60)
print("✓ PIPELINE TEST PASSED")
print("="*60)
