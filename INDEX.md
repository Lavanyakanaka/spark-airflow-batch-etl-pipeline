# Spark + Airflow Batch ETL Pipeline - Medallion Architecture
## 📋 Project Index & Getting Started

---

## 🚀 Quick Start (2 minutes)

```bash
# 1. Start all services
docker-compose up -d

# 2. Run the pipeline
docker-compose exec -T airflow-init python3 run_pipeline.py 2026-01-12

# 3. View results
# Dashboard:   http://localhost:8501
# Airflow:     http://localhost:8082 (admin/admin)
# MinIO:       http://localhost:9001 (minioadmin/minioadmin)
```

---

## 📚 Documentation (Read in Order)

| Document | Purpose | Read Time |
|----------|---------|-----------|
| **QUICKSTART.md** | 5-minute setup guide | 5 min |
| **README.md** | Full architecture & best practices | 20 min |
| **DELIVERY_SUMMARY.md** | What was delivered & features | 10 min |

---

## 📁 Project Structure

```
spark-airflow-batch-etl-pipeline/
├── docker-compose.yml              # Infrastructure as Code (all services)
├── 
├── airflow/                        # Airflow configuration
│   ├── dags/
│   │   └── etl_medallion_dag.py   # Daily orchestration DAG
│   ├── airflow.cfg                # Airflow config
│   ├── webserver_config.py        # Web UI config
│   └── requirements.txt           # Python dependencies
│
├── data_gen/                       # Data generation module
│   ├── generate_events.py         # 1M events generator
│   └── __init__.py
│
├── spark_jobs/                     # Spark ETL jobs
│   ├── medallion_job.py           # Bronze→Silver→Gold transformation
│   └── __init__.py
│
├── streamlit_app/                  # Analytics dashboard
│   └── app.py                     # KPI visualization
│
├── QUICKSTART.md                   # ⭐ START HERE - 5 min setup
├── README.md                       # Full documentation
├── DELIVERY_SUMMARY.md             # What was built
│
├── run_pipeline.py                 # Orchestrator script (Python)
├── run.sh                          # Quick runner (Bash)
├── run.bat                         # Quick runner (Batch/Windows)
├── verify_pipeline.py              # Verification & testing script
├── test_pipeline.py                # Integration test suite
│
└── logs/                           # Airflow logs (created at runtime)
```

---

## 🎯 What This Project Delivers

### ✅ Complete ETL Pipeline
- **Automated daily batch processing** of 1M+ events
- **Multi-stage transformation** (raw → cleaned → aggregated)
- **Production-ready idempotency** (safe to rerun without duplicates)
- **Data quality validation** at each layer

### ✅ Medallion Architecture
- **Bronze**: Raw CSV files in MinIO (S3-compatible)
- **Silver**: Cleaned Parquet files (validated schema)
- **Gold**: Aggregated business metrics (optimized for analytics)

### ✅ Orchestration & Scheduling
- **Airflow DAG** for daily automated runs
- **Manual triggers** via CLI or web UI
- **Backfill support** for historical data processing

### ✅ Analytics Ready
- **Postgres data warehouse** for BI tools
- **Streamlit dashboard** with live KPIs
- **Real-time metrics** visualization

---

## 🏗️ Architecture Diagram

```
Raw Events (1M)
      ↓
[BRONZE LAYER]  ← Raw CSV in MinIO
      ↓
[Spark Job]     ← Transformation & Aggregation
      ↓         ↓
   SILVER    GOLD
 [Cleaned]  [Metrics]
 Parquet    Parquet
      ↓         ↓
[POSTGRES DATA MART] ← daily_metrics table
      ↓
[STREAMLIT DASHBOARD] ← KPI visualization
```

---

## 📊 Services & Ports

| Service | URL | Purpose |
|---------|-----|---------|
| **Airflow Webserver** | http://localhost:8082 | DAG orchestration |
| **Streamlit Dashboard** | http://localhost:8501 | Analytics & KPIs |
| **MinIO Console** | http://localhost:9001 | Object storage browser |
| **Postgres (Warehouse)** | localhost:5433 | Data warehouse |

**Credentials:**
- Airflow: `admin` / `admin`
- MinIO: `minioadmin` / `minioadmin`
- Postgres: `warehouse` / `warehouse`

---

## 🔧 Running the Pipeline

### Option 1: Python Script (Recommended)
```bash
docker-compose exec -T airflow-init python3 run_pipeline.py 2026-01-12
```

### Option 2: Bash Script (Unix/Mac)
```bash
bash run.sh 2026-01-12
```

### Option 3: Batch Script (Windows)
```cmd
run.bat 2026-01-12
```

### Option 4: Airflow Web UI
- Navigate to http://localhost:8082
- Click DAG: `etl_medallion_dag`
- Click "Trigger DAG"

### Option 5: Manual Steps
```bash
# Generate events
docker-compose exec -T airflow-init python3 -c \
  "from data_gen.generate_events import main; main('2026-01-12')"

# Transform data
docker-compose exec -T airflow-init python3 -c \
  "from spark_jobs.medallion_job import main; main('2026-01-12')"

# Query results
docker-compose exec -T warehouse-postgres psql -U warehouse -d warehouse \
  -c "SELECT * FROM daily_metrics WHERE metric_date = '2026-01-12';"
```

---

## 📈 Pipeline Metrics

**Event Volume**: 1M events/day (configurable)

**Performance**:
- Event generation: ~10s
- Spark transformation: ~2-3min
- Data load: ~1s
- **Total**: ~3-4min end-to-end

**Storage**:
- Bronze layer: ~200MB/day
- Silver layer: ~150MB/day
- Gold layer: <1MB/day

---

## ✨ Key Features

✅ **Fully Containerized** – Single `docker-compose up` deploys everything  
✅ **Idempotent** – Safe to rerun same date without duplicates  
✅ **Production-Ready** – Error handling, logging, data validation  
✅ **Scalable** – Easily adapt to 100M+ events  
✅ **Well-Documented** – README, QUICKSTART, code comments  
✅ **Cross-Platform** – Windows, Mac, Linux  
✅ **Observable** – Airflow UI, dashboard, logs  
✅ **Testable** – Integration test suite included  

---

## 🧪 Verification

Run the verification script to test all components:

```bash
docker-compose exec -T airflow-init python3 verify_pipeline.py 2026-01-12
```

This will:
1. ✓ Check all services running
2. ✓ Verify MinIO buckets exist
3. ✓ Test Postgres connections
4. ✓ Generate sample events
5. ✓ Run Spark transformation
6. ✓ Load data to warehouse
7. ✓ Verify outputs

---

## 🐛 Troubleshooting

### Services not starting?
```bash
docker-compose restart
docker-compose logs --tail=50
```

### Out of memory?
Reduce event volume in `data_gen/generate_events.py`:
```python
def generate_events_for_date(..., n_rows: int = 100_000):  # 100K instead of 1M
```

### Dashboard won't load?
```bash
docker-compose logs streamlit
docker-compose restart streamlit
```

### Airflow DAG not visible?
```bash
docker-compose restart airflow-scheduler
docker-compose logs airflow-scheduler --tail=100
```

See **README.md** Troubleshooting section for more details.

---

## 📚 Files Reference

### Core Infrastructure
- `docker-compose.yml` – All services (9 containers)
- `airflow/requirements.txt` – Python dependencies

### Pipeline Code
- `data_gen/generate_events.py` – 1M synthetic events
- `spark_jobs/medallion_job.py` – ETL transformation
- `airflow/dags/etl_medallion_dag.py` – Orchestration DAG
- `streamlit_app/app.py` – Dashboard

### Automation
- `run_pipeline.py` – Python orchestrator
- `run.sh` – Bash shortcut
- `run.bat` – Windows batch
- `verify_pipeline.py` – Verification suite

### Documentation
- `README.md` – Full documentation
- `QUICKSTART.md` – Quick setup guide
- `DELIVERY_SUMMARY.md` – Project summary

---

## 🎓 Learning Outcomes

This project demonstrates:

1. **Data Engineering Fundamentals**
   - Event generation & ingestion
   - Data lake design (Medallion architecture)
   - ETL pipeline development

2. **Big Data Technologies**
   - Apache Airflow (orchestration)
   - Apache Spark (distributed processing)
   - MinIO (data lakes)

3. **Data Warehousing**
   - Schema design
   - Fact/dimension tables
   - Data marts

4. **DevOps & Infrastructure**
   - Docker containerization
   - Docker Compose orchestration
   - Infrastructure as Code (IaC)

5. **Analytics & BI**
   - KPI definition
   - Dashboard development (Streamlit)
   - Data visualization

---

## 🚀 Next Steps

1. **Start the stack**
   ```bash
   docker-compose up -d
   ```

2. **Read QUICKSTART.md** for 5-minute setup

3. **Run the pipeline**
   ```bash
   docker-compose exec -T airflow-init python3 run_pipeline.py
   ```

4. **View the dashboard**
   ```
   http://localhost:8501
   ```

5. **Explore Airflow UI**
   ```
   http://localhost:8082
   ```

---

## 📞 Support

For help:
- **Quick questions** → See QUICKSTART.md
- **Architecture details** → See README.md
- **Code questions** → See inline comments in source files
- **Issues** → Check README.md Troubleshooting section

---

**✅ Project Status: COMPLETE & READY TO USE**

Start with `QUICKSTART.md` → Read `README.md` → Run `run_pipeline.py` → View Dashboard!
