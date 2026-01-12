# Batch ETL Pipeline with Spark, Airflow, and MinIO

## Project Overview

This project implements a complete batch ETL pipeline using Apache Spark, Apache Airflow, and MinIO. The pipeline processes synthetic user event data through a three-tier Medallion architecture.

## Architecture

### System Components

- **PostgreSQL**: Data warehouse
- **MinIO**: S3-compatible object storage
  - Bronze Layer: Raw event data
  - Silver Layer: Cleaned data (Parquet)
  - Gold Layer: Aggregated data (Parquet)
- **Apache Airflow**: Workflow orchestration
- **Apache Spark**: Data processing
- **Streamlit**: Interactive dashboard

## Quick Start

### Prerequisites

- Docker & Docker Compose (v1.29.0+)
- 8GB RAM minimum
- 20GB disk space

### Setup

```bash
git clone https://github.com/Lavanyakanaka/spark-airflow-batch-etl-pipeline.git
cd spark-airflow-batch-etl-pipeline
docker-compose up -d
```

### Access Services

- **Airflow**: http://localhost:8080 (admin/admin)
- **MinIO**: http://localhost:9001 (minioadmin/minioadmin)
- **Streamlit**: http://localhost:8501
- **Spark Master**: http://localhost:8888

## Project Structure

```
.
├── docker-compose.yml
├── dags/
│   └── etl_pipeline.py
├── scripts/
│   ├── data_generator.py
│   └── spark_job.py
├── streamlit_app.py
└── requirements.txt
```

## Running the Pipeline

1. Open Airflow UI at http://localhost:8080
2. Locate "etl_pipeline" DAG
3. Click "Trigger DAG"
4. Monitor task execution in Airflow UI

## Monitoring

### MinIO Console
http://localhost:9001 - View data in bronze, silver, gold buckets

### Streamlit Dashboard
http://localhost:8501 - View analytics and metrics

## Technologies

- Python 3.9+
- Apache Airflow 2.7.0
- Apache Spark 3.3.0
- MinIO
- PostgreSQL 14
- Streamlit
- Docker Compose

## Author

Kella Lavanya Kanaka
