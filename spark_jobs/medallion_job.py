import argparse


def get_spark():
    try:
        from pyspark.sql import SparkSession
    except Exception as e:
        raise RuntimeError("Missing dependency 'pyspark'. Install with 'pip install pyspark'") from e

    spark = (
        SparkSession.builder.appName("MedallionJob")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    return spark


def read_bronze(spark, process_date):
    try:
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
    except Exception as e:
        raise RuntimeError("Missing dependency 'pyspark'. Install with 'pip install pyspark'") from e

    path = f"s3a://bronze/raw/events/date={process_date}/"
    schema = StructType(
        [
            StructField("event_id", StringType(), True),
            StructField("user_id", IntegerType(), True),
            StructField("event_type", StringType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("event_timestamp", StringType(), True),
            StructField("amount", DoubleType(), True),
        ]
    )
    df = (
        spark.read.option("header", True)
        .schema(schema)
        .csv(path)
    )
    return df


def transform_to_silver(df):
    try:
        from pyspark.sql import functions as F
    except Exception as e:
        raise RuntimeError("Missing dependency 'pyspark'. Install with 'pip install pyspark'") from e

    df_clean = (
        df.withColumn(
            "event_timestamp_ts",
            F.to_timestamp("event_timestamp"),
        )
        .dropna(subset=["event_id", "user_id", "product_id", "event_timestamp_ts"])
        .withColumn("event_date", F.to_date("event_timestamp_ts"))
        .withColumn("day_of_week", F.date_format("event_date", "E"))
        .drop("event_timestamp")
        .withColumnRenamed("event_timestamp_ts", "event_timestamp")
    )
    return df_clean


def write_silver(df, process_date):
    year = process_date[:4]
    month = process_date[5:7]
    day = process_date[8:10]

    output_path = f"s3a://silver/events/year={year}/month={month}/day={day}/"
    (
        df.write.mode("overwrite")
        .parquet(output_path)
    )


def read_silver_for_date(spark, process_date):
    year = process_date[:4]
    month = process_date[5:7]
    day = process_date[8:10]
    path = f"s3a://silver/events/year={year}/month={month}/day={day}/"
    return spark.read.parquet(path)


def aggregate_to_gold(df_silver, process_date):
    try:
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window
    except Exception as e:
        raise RuntimeError("Missing dependency 'pyspark'. Install with 'pip install pyspark'") from e

    # daily active users and total revenue
    agg = (
        df_silver.groupBy("event_date")
        .agg(
            F.countDistinct("user_id").alias("daily_active_users"),
            F.sum("amount").alias("total_revenue"),
        )
    )

    # top product by revenue
    product_rev = (
        df_silver.groupBy("event_date", "product_id")
        .agg(F.sum("amount").alias("product_revenue"))
    )

    rownum = (
        F.row_number()
        .over(
            Window.partitionBy("event_date").orderBy(F.col("product_revenue").desc())
        )
    )

    ranked = product_rev.withColumn("rn", rownum).filter(F.col("rn") == 1).drop("rn")

    result = (
        agg.join(ranked, on="event_date", how="left")
        .select(
            "event_date",
            "daily_active_users",
            "total_revenue",
            F.col("product_id").alias("top_product_id"),
            F.col("product_revenue").alias("top_product_revenue"),
        )
    )
    return result


def write_gold(df_gold, process_date):
    year = process_date[:4]
    month = process_date[5:7]
    day = process_date[8:10]
    output_path = f"s3a://gold/daily_metrics/year={year}/month={month}/day={day}/"
    df_gold.write.mode("overwrite").parquet(output_path)


def main(process_date: str):
    spark = get_spark()

    bronze_df = read_bronze(spark, process_date)
    silver_df = transform_to_silver(bronze_df)
    write_silver(silver_df, process_date)

    silver_df_for_date = read_silver_for_date(spark, process_date)
    gold_df = aggregate_to_gold(silver_df_for_date, process_date)
    write_gold(gold_df, process_date)

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--process-date", required=True)
    args = parser.parse_args()
    main(args.process_date)
