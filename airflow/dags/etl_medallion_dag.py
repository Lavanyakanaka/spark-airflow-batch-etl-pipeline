from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook

from minio import Minio
import pandas as pd


def generate_events_task(process_date, **context):
    from data_gen.generate_events import main as gen_main
    gen_main(process_date)


def bronze_quality_check_task(process_date, **context):
    from minio import Minio
    conn = BaseHook.get_connection("minio_conn")
    host_port = f"{conn.host}:{conn.port}"
    client = Minio(host_port, access_key=conn.login, secret_key=conn.password, secure=False)
    bucket = "bronze"
    prefix = f"raw/events/date={process_date}/"
    objects = list(client.list_objects(bucket, prefix=prefix, recursive=True))
    if not objects:
        raise ValueError(f"No bronze files found for {process_date}")


def silver_gold_quality_check_task(process_date, **context):
    from pyspark.sql import SparkSession

    conn = BaseHook.get_connection("minio_conn")
    host_port = f"{conn.host}:{conn.port}"
    access_key = conn.login
    secret_key = conn.password

    year = process_date[:4]
    month = process_date[5:7]
    day = process_date[8:10]

    spark = (
        SparkSession.builder.appName("DQCheck")
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{host_port}")
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .getOrCreate()
    )

    silver_path = f"s3a://silver/events/year={year}/month={month}/day={day}/"
    gold_path = f"s3a://gold/daily_metrics/year={year}/month={month}/day={day}/"

    silver_df = spark.read.parquet(silver_path)
    gold_df = spark.read.parquet(gold_path)

    silver_count = silver_df.count()
    gold_count = gold_df.count()

    if silver_count == 0 or gold_count == 0:
        raise ValueError("Silver or Gold layer is empty")

    spark.stop()


def load_to_postgres_task(process_date, **context):
    from pyspark.sql import SparkSession

    conn = BaseHook.get_connection("minio_conn")
    host_port = f"{conn.host}:{conn.port}"
    access_key = conn.login
    secret_key = conn.password

    year = process_date[:4]
    month = process_date[5:7]
    day = process_date[8:10]

    spark = (
        SparkSession.builder.appName("LoadToPostgres")
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{host_port}")
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .getOrCreate()
    )

    gold_path = f"s3a://gold/daily_metrics/year={year}/month={month}/day={day}/"
    gold_df = spark.read.parquet(gold_path)

    pdf = gold_df.toPandas()
    spark.stop()

    pdf["metric_date"] = pd.to_datetime(pdf["event_date"]).dt.date
    pdf = pdf.drop(columns=["event_date"])

    hook = PostgresHook(postgres_conn_id="warehouse_postgres")
    engine = hook.get_sqlalchemy_engine()

    with engine.begin() as conn2:
        conn2.execute(
            "CREATE TABLE IF NOT EXISTS daily_metrics ("
            "metric_date date,"
            "daily_active_users int,"
            "total_revenue numeric,"
            "top_product_id int,"
            "top_product_revenue numeric,"
            "created_at timestamp default now()"
            ");"
        )
        conn2.execute(
            "DELETE FROM daily_metrics WHERE metric_date = %s",
            (process_date,),
        )
        if not pdf.empty:
            pdf.to_sql(
                "daily_metrics",
                con=conn2,
                if_exists="append",
                index=False,
            )


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="etl_medallion_dag",
    default_args=default_args,
    description="End-to-end Medallion ETL",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    generate_events = PythonOperator(
        task_id="generate_events",
        python_callable=generate_events_task,
        op_kwargs={"process_date": "{{ ds }}"},
    )

    bronze_quality_check = PythonOperator(
        task_id="bronze_quality_check",
        python_callable=bronze_quality_check_task,
        op_kwargs={"process_date": "{{ ds }}"},
    )

    def run_medallion(process_date, **context):
        from spark_jobs.medallion_job import main as medallion_main
        medallion_main(process_date)

    spark_medallion = PythonOperator(
        task_id="spark_medallion",
        python_callable=run_medallion,
        op_kwargs={"process_date": "{{ ds }}"},
    )

    silver_gold_quality_check = PythonOperator(
        task_id="silver_gold_quality_check",
        python_callable=silver_gold_quality_check_task,
        op_kwargs={"process_date": "{{ ds }}"},
    )

    load_to_postgres = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres_task,
        op_kwargs={"process_date": "{{ ds }}"},
    )

    generate_events >> bronze_quality_check >> spark_medallion >> silver_gold_quality_check >> load_to_postgres
