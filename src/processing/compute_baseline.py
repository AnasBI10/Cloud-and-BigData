"""
Baseline Berechnung. Berechnung von Erwartungswert und Streuung, je link_id x Wochentag x Stunde aus Silver Data
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, dayofweek, hour, stddev

SILVER_TABLE_PATH = os.environ.get("SILVER_TABLE_PATH", "s3a://bronze/traffic_speeds_valid")
BASELINE_TABLE_PATH = os.environ.get("BASELINE_TABLE_PATH", "s3a://gold/baseline_profile")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "admin")
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "")

# Zellen mit zu wenig stichproben werden ausgeschlossen
MIN_SAMPLES_PER_CELL = 5


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("congestion-watch-compute-baseline")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    silver = spark.read.format("delta").load(SILVER_TABLE_PATH)

    baseline = (
        silver
        # event_time ist bereits durch silver def gefiltert, daher keine weitere konvertierung 
        .withColumn("weekday", dayofweek("event_time"))
        .withColumn("hour_of_day", hour("event_time"))
        .groupBy("link_id", "weekday", "hour_of_day")
        .agg(
            avg("speed_mph").alias("baseline_speed"),
            stddev("speed_mph").alias("baseline_stddev"),
            count("*").alias("sample_count"),
        )
        .where(f"sample_count >= {MIN_SAMPLES_PER_CELL}")
    )

    (
        baseline.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(BASELINE_TABLE_PATH)
    )

    total_cells = baseline.count()
    distinct_links = baseline.select("link_id").distinct().count()
    print(f"Baseline geschrieben: {total_cells} Zellen ueber {distinct_links} Segmente nach {BASELINE_TABLE_PATH}")


if __name__ == "__main__":
    main()
