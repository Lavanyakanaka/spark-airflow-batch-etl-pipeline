# QUICK START GUIDE

## 5-Minute Setup & Test

### Step 1: Start All Services
```bash
cd spark-airflow-batch-etl-pipeline
docker-compose up -d
```

Wait 10-15 seconds for services to initialize.

### Step 2: Verify Services Running
```bash
docker-compose ps
```

You should see 6-8 containers with "Up" status.

### Step 3: Run Pipeline (Choose one option)

**Option A: Automated Verification Script (Recommended)**
```bash
docker-compose exec -T airflow-init python3 verify_pipeline.py 2026-01-12
```

**Option B: Python Script**
```bash
docker-compose exec -T airflow-init python3 run_pipeline.py 2026-01-12
```

**Option C: Bash Script (Unix/Mac)**
```bash
bash run.sh 2026-01-12
```

**Option D: Batch Script (Windows)**
```cmd
run.bat 2026-01-12
```

**Option E: Manual Steps**
```bash
# Generate events
docker-compose exec -T airflow-init python3 -c \
  "from data_gen.generate_events import main; main('2026-01-12')"

# Transform data
docker-compose exec -T airflow-init python3 -c \
  "from spark_jobs.medallion_job import main; main('2026-01-12')"

# Query results
docker-compose exec -T warehouse-postgres psql -U warehouse -d warehouse -c \
  "SELECT * FROM daily_metrics WHERE metric_date = '2026-01-12' LIMIT 3;"
```

### Step 4: View Results

**Streamlit Dashboard** (Live metrics & charts)
```
http://localhost:8501
```

**Airflow Web UI** (DAG orchestration)
```
http://localhost:8082
Username: admin
Password: admin
```

**MinIO Console** (Object storage browser)
```
http://localhost:9001
Username: minioadmin
Password: minioadmin
```

**Direct Postgres Query**
```bash
docker-compose exec -T warehouse-postgres psql -U warehouse -d warehouse

warehouse=> SELECT * FROM daily_metrics;
```

---

## Troubleshooting

### Issue: "Connection refused"
**Solution**: Wait 15 seconds after `docker-compose up -d`, then retry.

### Issue: "No such file or directory: data_gen"
**Solution**: Ensure you're in the project root directory:
```bash
cd spark-airflow-batch-etl-pipeline
```

### Issue: "Table 'daily_metrics' does not exist"
**Solution**: Run the pipeline once to create the table:
```bash
docker-compose exec -T airflow-init python3 run_pipeline.py
```

### Issue: Out of memory or "Spark failed"
**Solution**: Reduce event volume in `data_gen/generate_events.py` from 1M to 100K:
```python
def generate_events_for_date(process_date: str, n_rows: int = 100_000) -> pd.DataFrame:
```

### Check Logs
```bash
# Airflow scheduler
docker-compose logs airflow-scheduler --tail=50

# Spark job execution (appears in airflow-init logs)
docker-compose logs airflow-init --tail=50

# MinIO
docker-compose logs minio --tail=30

# Postgres warehouse
docker-compose logs warehouse-postgres --tail=30
```

---

## What Gets Created

After running the pipeline for 2026-01-12, you'll have:

**MinIO (Object Storage)**
- `s3://bronze/raw/events/date=2026-01-12/events_2026-01-12.csv` (1M rows)
- `s3://silver/events/year=2026/month=01/day=12/` (Parquet, cleaned)
- `s3://gold/daily_metrics/year=2026/month=01/day=12/` (Parquet, aggregated)

**Postgres (Data Warehouse)**
- Table: `daily_metrics` with row:
  ```
  metric_date   | daily_active_users | total_revenue | top_product_id | top_product_revenue
  2026-01-12    |      ~67,000       |  ~$15,000,000 |    2,345        |    ~$25,000
  ```

**Dashboard**
- Line chart of daily active users (historical)
- Bar chart of revenue (historical)
- 3 KPI cards (DAU, Revenue, Top Product)

---

## Re-running for Different Dates

```bash
# Run for a specific past date
docker-compose exec -T airflow-init python3 run_pipeline.py 2026-01-10

# Run for today
docker-compose exec -T airflow-init python3 run_pipeline.py $(date +%Y-%m-%d)

# Run for multiple dates (creates time series)
for date in 2026-01-{10..15}; do
  docker-compose exec -T airflow-init python3 run_pipeline.py $date
  sleep 5
done
```

Then view the dashboard at `http://localhost:8501` to see multi-day trends.

---

## Next Steps

1. **View Architecture Diagram**: See `README.md`
2. **Explore Airflow DAG**: Open http://localhost:8082 → DAGs → etl_medallion_dag
3. **Trigger DAG Manually**: Airflow UI → Click DAG → "Trigger DAG"
4. **Query Data Mart**: Connect your BI tool (Tableau, Looker, etc.) to `warehouse-postgres:5433`
5. **Scale to Production**: Modify docker-compose.yml to use AWS S3, RDS, managed Spark cluster

---

## File Reference

| File | Purpose |
|------|---------|
| `docker-compose.yml` | Infrastructure (Airflow, MinIO, Postgres, Streamlit) |
| `airflow/dags/etl_medallion_dag.py` | Airflow DAG for daily scheduling |
| `data_gen/generate_events.py` | Synthetic event generator (1M events) |
| `spark_jobs/medallion_job.py` | PySpark ETL transformation job |
| `streamlit_app/app.py` | Analytics dashboard |
| `run_pipeline.py` | Python orchestrator script |
| `run.sh` / `run.bat` | Shell/batch shortcuts |
| `verify_pipeline.py` | Comprehensive verification & testing |
| `README.md` | Full documentation |

---

## Support

For detailed info on architecture, data flow, and configuration:
→ See `README.md`

For code examples and best practices:
→ See inline comments in `spark_jobs/medallion_job.py`, `data_gen/generate_events.py`
