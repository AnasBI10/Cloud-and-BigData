import os

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.avro.functions import from_avro, to_avro
from pyspark.sql.functions import (
    avg, broadcast, coalesce, col, count, current_timestamp, expr, lit, struct, when, window,
)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC_IN = os.environ.get("KAFKA_TOPIC_IN", "traffic.speeds.raw")
KAFKA_TOPIC_DLQ = os.environ.get("KAFKA_TOPIC_DLQ", "traffic.speeds.dlq")
SCHEMA_PATH = os.environ.get("SCHEMA_PATH", "/opt/spark-app/schemas/traffic_speed_event.avsc")
SEED_PATH = os.environ.get("SEED_PATH", "/opt/spark-app/data/dot_links_seed.json")
CHECKPOINT_BASE = os.environ.get("CHECKPOINT_DIR", "/opt/spark-app/checkpoints")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "admin")
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "")

BRONZE_TABLE_PATH = os.environ.get("BRONZE_TABLE_PATH", "s3a://bronze/traffic_speeds_raw")
SILVER_TABLE_PATH = os.environ.get("SILVER_TABLE_PATH", "s3a://bronze/traffic_speeds_valid")
GOLD_TABLE_PATH = os.environ.get("GOLD_TABLE_PATH", "s3a://gold/congestion_scores")

WINDOW_DURATION = "5 minutes"
WINDOW_SLIDE = "1 minute"
WATERMARK_DELAY = "2 minutes"
CONFLUENT_WIRE_HEADER_BYTES = 5


def read_avro_schema(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def load_seed(spark: SparkSession) -> DataFrame:
    return (
        spark.read.option("multiline", "true").json(SEED_PATH)
        .select(
            col("link_id"),
            col("borough").alias("seed_borough"),
            col("link_name").alias("seed_link_name"),
        )
    )


def enrich_with_seed(events: DataFrame, seed: DataFrame) -> DataFrame:
    return (
        events
        .join(broadcast(seed), on="link_id", how="left")
        .withColumn("borough", coalesce(col("seed_borough"), col("borough")))
        .withColumn("link_name", coalesce(col("seed_link_name"), col("link_name")))
        .drop("seed_borough", "seed_link_name")
    )


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("congestion-watch-bronze-silver-gold")
        .config("spark.sql.shuffle.partitions", "12")
        .config("spark.jars.ivy", "/opt/spark-app/ivy-cache")
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


def append_delta(df: DataFrame, path: str) -> None:
    (
        df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(path)
    )


def upsert_gold(batch_df: DataFrame, path: str) -> None:
    if not DeltaTable.isDeltaTable(batch_df.sparkSession, path):
        (
            batch_df.write
            .format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
            .save(path)
        )
        return

    target = DeltaTable.forPath(batch_df.sparkSession, path)
    (
        target.alias("target")
        .merge(
            batch_df.alias("source"),
            "target.link_id = source.link_id AND target.window_start = source.window_start",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    batch_df.persist()

    bronze = batch_df.drop("is_late_marker")
    append_delta(bronze, BRONZE_TABLE_PATH)

    silver = bronze.where(col("status") == 0)
    append_delta(silver, SILVER_TABLE_PATH)

    gold_input = batch_df.where(col("status") == 0).where(~col("is_late_marker"))
    gold_agg = (
        gold_input
        .groupBy("window_start", "window_end", "link_id", "borough")
        .agg(
            avg("speed_mph").alias("avg_speed_mph"),
            count("*").alias("sample_count"),
        )
        .withColumn(
            "congestion_score",
            when(col("avg_speed_mph") >= 35, lit(0))
            .when(col("avg_speed_mph") <= 15, lit(100))
            .otherwise(((lit(35) - col("avg_speed_mph")) / lit(20) * lit(100)).cast("int")),
        )
        .withColumn("updated_at", current_timestamp())
    )
    upsert_gold(gold_agg, GOLD_TABLE_PATH)

    batch_df.unpersist()


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    avro_schema_json = read_avro_schema(SCHEMA_PATH)

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC_IN)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    decoded = (
        raw.select(
            col("key").cast("string").alias("kafka_key"),
            col("value").substr(CONFLUENT_WIRE_HEADER_BYTES + 1, 1000000).alias("avro_payload"),
        )
        .select("kafka_key", from_avro(col("avro_payload"), avro_schema_json).alias("event"))
        .select("kafka_key", "event.*")
        .withColumnRenamed("data_as_of", "event_time")
    )

    seed = load_seed(spark)
    enriched = enrich_with_seed(decoded, seed)

    late_cutoff = expr(f"current_timestamp() - interval {WATERMARK_DELAY}")
    tagged = enriched.withColumn("is_late", col("event_time") < late_cutoff)



    window_col = window(col("event_time"), WINDOW_DURATION, WINDOW_SLIDE)
    windowed = (
        tagged
        .withWatermark("event_time", WATERMARK_DELAY)
        .withColumn("window", window_col)
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop("window")
        .withColumnRenamed("is_late", "is_late_marker")
    )

    query = (
        windowed.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/bronze-silver-gold")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )

    dlq_payload = struct(
        col("link_id"),
        col("event_time").alias("data_as_of"),
        col("event_key"),
        col("status"),
        col("speed_mph"),
        col("travel_time_s"),
        col("borough"),
        col("link_name"),
        col("link_points"),
        col("ingested_at"),
        col("source"),
    )

    late_records = tagged.where(col("is_late"))
    dlq_query = (
        late_records
        .select(col("kafka_key"), to_avro(dlq_payload).alias("value"))
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", KAFKA_TOPIC_DLQ)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/late-data-dlq")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
