# Spark + Airflow Batch ETL Pipeline - Medallion Architecture

A production-ready, containerized batch ETL pipeline demonstrating the Medallion (Bronze → Silver → Gold) data lake architecture with Spark, Airflow, MinIO (S3-compatible), and Postgres.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    Data Generation                               │
│              (1M+ synthetic user events/day)                     │
└────────────────────────┬────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER (MinIO)                          │
│          Raw CSV files (date-partitioned in S3)                  │
└────────────────────────┬────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│    Spark Job: Transformation & Cleaning                          │
│    ✓ Schema validation  ✓ Type casting  ✓ Null handling          │
└──────────┬──────────────────────────────────────┬────────────────┘
           ↓                                      ↓
    ┌────────────────┐             ┌──────────────────────┐
    │ SILVER LAYER   │             │   GOLD LAYER         │
    │ (MinIO)        │             │   (MinIO)            │
    │ Clean Data     │             │ Aggregated Business  │
    │ Parquet        │             │ Metrics (Parquet)    │
    │ Partitioned    │             │ • Daily Active Users │
    └────────────────┘             │ • Total Revenue      │
                                   │ • Top Products       │
                                   └──────────┬───────────┘
                                              ↓
                                   ┌──────────────────────┐
                                   │  Postgres Warehouse  │
                                   │   Data Mart          │
                                   │  (daily_metrics)     │
                                   └──────────┬───────────┘
                                              ↓
                                   ┌──────────────────────┐
                                   │  Streamlit Dashboard │
                                   │   Visualization      │
                                   └──────────────────────┘
```

## Services

| Service | Port | Purpose |
|---------|------|---------|
| Airflow Webserver | 8082 | DAG orchestration & monitoring |
| Airflow Scheduler | (internal) | Workflow scheduling |
| MinIO S3 API | 9000 | Object storage (Bronze/Silver/Gold) |
| MinIO Console | 9001 | S3 browser UI |
| Postgres (Airflow) | 5434 | Airflow metadata DB |
| Postgres (Warehouse) | 5433 | Data warehouse / analytics DB |
| Streamlit | 8501 | Analytics dashboard |

## Quick Start

### 1. Prerequisites

- Docker & Docker Compose installed
- ~5GB free disk space (for MinIO data + logs)

### 2. Start Stack

```bash
cd spark-airflow-batch-etl-pipeline
docker-compose up -d
```

Services will be ready in ~30 seconds. Check status:

```bash
docker-compose ps
```

### 3. Run Pipeline (Option A: Automated Script)

Run the full pipeline for a specific date:

```bash
# For today
docker-compose exec -T airflow-init python3 run_pipeline.py

# For a specific date
docker-compose exec -T airflow-init python3 run_pipeline.py 2026-01-12
```

This script automatically:
1. ✓ Generates 1M events → Bronze (MinIO)
2. ✓ Transforms & cleans → Silver (Parquet)
3. ✓ Aggregates → Gold (Parquet) 
4. ✓ Loads aggregates → Postgres Data Mart
5. ✓ Verifies row counts

### 4. Run Pipeline (Option B: Manual Steps)

Generate events:
```bash
docker-compose exec -T airflow-init python3 -c \
  "from data_gen.generate_events import main; main('2026-01-12')"
```

Transform & aggregate (Spark job):
```bash
docker-compose exec -T airflow-init python3 -c \
  "from spark_jobs.medallion_job import main; main('2026-01-12')"
```

Verify data in Postgres:
```bash
docker-compose exec -T warehouse-postgres psql -U warehouse -d warehouse -c \
  "SELECT * FROM daily_metrics WHERE metric_date = '2026-01-12' LIMIT 5;"
```

### 5. View Results

**Streamlit Dashboard** (real-time metrics):
```
http://localhost:8501
```

**Airflow Web UI** (DAG orchestration):
```
http://localhost:8082
Login: admin / admin
```

**MinIO Console** (object storage browser):
```
http://localhost:9001
Login: minioadmin / minioadmin
```

**Postgres Data Mart** (SQL queries):
```bash
docker-compose exec warehouse-postgres psql -U warehouse -d warehouse

warehouse=> SELECT * FROM daily_metrics;
warehouse=> \q
```

## Key Files

| File | Purpose |
|------|---------|
| `docker-compose.yml` | Full infrastructure as code |
| `airflow/dags/etl_medallion_dag.py` | Airflow DAG (daily orchestration) |
| `data_gen/generate_events.py` | Synthetic event generator (1M events/day) |
| `spark_jobs/medallion_job.py` | PySpark ETL job (Bronze → Silver → Gold) |
| `streamlit_app/app.py` | Analytics dashboard |
| `run_pipeline.py` | Automated end-to-end pipeline runner |

## Data Flow

### Bronze Layer (Raw)
- CSV files with schema:
  ```
  event_id, user_id, event_type, product_id, event_timestamp, amount
  ```
- Partitioned: `s3://bronze/raw/events/date=YYYY-MM-DD/`
- Data quality: Not validated, exactly as received

### Silver Layer (Cleaned)
- Parquet format
- Partitioned: `s3://silver/events/year=YYYY/month=MM/day=DD/`
- Columns added:
  - `event_timestamp_ts` (timestamp)
  - `event_date` (date)
  - `day_of_week` (E)
- Data quality: Schema validated, nulls handled, types enforced

### Gold Layer (Aggregated)
- Parquet format (optimized for analytics)
- Partitioned: `s3://gold/daily_metrics/year=YYYY/month=MM/day=DD/`
- Metrics by date:
  - `daily_active_users` (distinct count)
  - `total_revenue` (sum)
  - `top_product_id` (highest revenue)
  - `top_product_revenue` (highest amount)

### Postgres Data Mart
- Table: `daily_metrics`
- Used for dashboard & BI tools
- Idempotent: rows re-created on backfill/rerun

## Idempotency & Rerunning

The pipeline is fully idempotent:

```bash
# Run for a date that already processed — data will be replaced
docker-compose exec -T airflow-init python3 run_pipeline.py 2026-01-12

# Gold layer is overwritten (mode="overwrite")
# Postgres rows deleted before re-insert
# No duplicates will accumulate
```

## Data Quality Checks

The Airflow DAG includes data quality validation tasks:

1. **Bronze Check**: Verifies CSV files exist for the date
2. **Silver/Gold Check**: Row counts > 0 after transformation
3. **Postgres Load Check**: Counts match expected aggregates

To manually validate:

```bash
# Check bronze bucket
docker-compose exec minio mc ls local/bronze/raw/events/

# List silver partitions
docker-compose exec minio mc ls local/silver/events/

# Verify gold data
docker-compose exec minio mc ls local/gold/daily_metrics/

# Query Postgres
docker-compose exec warehouse-postgres psql -U warehouse -d warehouse \
  -c "SELECT COUNT(*), SUM(daily_active_users) FROM daily_metrics;"
```

## Scheduling

The Airflow DAG (`etl_medallion_dag`) is configured to:
- Run daily at 00:00 UTC (configurable)
- Process yesterday's data (via `{{ ds }}` Airflow macro)
- Support backfills for past dates without duplication

To trigger manually:

```bash
docker-compose exec -T airflow-scheduler airflow dags trigger etl_medallion_dag -e 2026-01-12
```

Or via the web UI: **Admin** → **DAGs** → **etl_medallion_dag** → **Trigger DAG**

## Troubleshooting

### Container Issues

**Check logs:**
```bash
docker-compose logs airflow-scheduler --tail=100
docker-compose logs spark_jobs --tail=100
docker-compose logs warehouse-postgres --tail=100
```

**Restart services:**
```bash
docker-compose restart
```

### Data Not Appearing

1. Verify Bronze layer uploaded:
   ```bash
   docker-compose exec minio mc ls local/bronze/raw/events/date=2026-01-12/
   ```

2. Check Spark job logs (look for errors):
   ```bash
   docker-compose exec -T airflow-init python3 -c "from spark_jobs.medallion_job import main; main('2026-01-12')"
   ```

3. Validate Postgres connection:
   ```bash
   docker-compose exec warehouse-postgres psql -U warehouse -d warehouse -c "SELECT 1;"
   ```

### Out of Memory

If Spark fails with memory errors, increase MinIO/Spark heap in `docker-compose.yml` (currently 1GB default).

## Performance Notes

- **1M events** processes in ~2-3 minutes on modern hardware
- **Spark local mode** (single-threaded) — production clusters run multi-node workers
- **MinIO disk** grows ~200MB-500MB per day (depending on event volume)
- **Postgres** queries sub-second for daily aggregates

## Production Deployment

For production use:

1. **Use managed services**: AWS S3, RDS, managed Spark cluster
2. **Enable logging**: CloudWatch, ELK stack, Prometheus
3. **Add monitoring**: Airflow sensors, data quality frameworks (Great Expectations)
4. **Scale**: Multi-node Spark, Kubernetes orchestration
5. **Security**: IAM roles, encryption at rest/transit, secrets management
6. **Testing**: Unit tests, integration tests, data validation tests

## License

Educational/demonstration project. Modify freely for learning purposes.

## Support

For issues or improvements:
1. Check logs: `docker-compose logs <service>`
2. Verify all containers running: `docker-compose ps`
3. Rebuild fresh: `docker-compose down && docker-compose up -d`
