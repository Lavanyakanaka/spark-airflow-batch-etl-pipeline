#!/usr/bin/env python3
"""
Diagnostic script to identify pipeline errors
"""
import subprocess
import sys

def test_import(module_path, desc):
    """Test if a module can be imported."""
    try:
        exec(f"import {module_path}")
        print(f"✓ {desc}")
        return True
    except Exception as e:
        print(f"✗ {desc}: {str(e)[:100]}")
        return False

def test_connection(test_code, desc):
    """Test a connection."""
    try:
        exec(test_code)
        print(f"✓ {desc}")
        return True
    except Exception as e:
        print(f"✗ {desc}: {str(e)[:100]}")
        return False

print("\n" + "="*60)
print("DIAGNOSTIC TEST")
print("="*60)

# Test 1: Imports
print("\n[1] Testing Imports")
test_import("data_gen.generate_events", "Data generation module")
test_import("spark_jobs.medallion_job", "Spark job module")
test_import("minio", "MinIO client")
test_import("pyspark.sql", "PySpark")
test_import("pandas", "Pandas")

# Test 2: Airflow connections
print("\n[2] Testing Airflow Connections")
test_code = """
from airflow.hooks.base import BaseHook
conn = BaseHook.get_connection("minio_conn")
print(f"  MinIO: {conn.host}:{conn.port}")
"""
try:
    exec(test_code)
    print("✓ MinIO connection defined")
except Exception as e:
    print(f"✗ MinIO connection: {str(e)[:100]}")

test_code = """
from airflow.hooks.base import BaseHook
conn = BaseHook.get_connection("warehouse_postgres")
print(f"  Postgres: {conn.host}:{conn.port}")
"""
try:
    exec(test_code)
    print("✓ Warehouse connection defined")
except Exception as e:
    print(f"✗ Warehouse connection: {str(e)[:100]}")

# Test 3: MinIO connectivity
print("\n[3] Testing MinIO")
test_code = """
from minio import Minio
client = Minio("minio:9000", access_key="minioadmin", secret_key="minioadmin", secure=False)
buckets = client.list_buckets()
print(f"  Found {len(list(buckets))} buckets")
"""
try:
    exec(test_code)
    print("✓ MinIO accessible")
except Exception as e:
    print(f"✗ MinIO connection: {str(e)[:100]}")

# Test 4: Postgres connectivity
print("\n[4] Testing Postgres")
test_code = """
from airflow.providers.postgres.hooks.postgres import PostgresHook
hook = PostgresHook(postgres_conn_id="warehouse_postgres")
engine = hook.get_sqlalchemy_engine()
result = engine.execute("SELECT 1")
print(f"  Query result: OK")
"""
try:
    exec(test_code)
    print("✓ Postgres accessible")
except Exception as e:
    print(f"✗ Postgres connection: {str(e)[:100]}")

print("\n" + "="*60)
print("END DIAGNOSTIC")
print("="*60)
