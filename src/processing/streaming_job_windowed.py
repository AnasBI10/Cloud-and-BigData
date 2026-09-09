import os

from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro, to_avro
from pyspark.sql.functions import (
    avg, col, count, expr, lit, struct, when, window,
)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC_IN = os.environ.get("KAFKA_TOPIC_IN", "traffic.speeds.raw")
KAFKA_TOPIC_DLQ = os.environ.get("KAFKA_TOPIC_DLQ", "traffic.speeds.dlq")
SCHEMA_PATH = os.environ.get("SCHEMA_PATH", "/opt/spark-app/schemas/traffic_speed_event.avsc")
CHECKPOINT_BASE = os.environ.get("CHECKPOINT_DIR", "/opt/spark-app/checkpoints")

WINDOW_DURATION = "5 minutes"
WINDOW_SLIDE = "1 minute"
WATERMARK_DELAY = "2 minutes"
CONFLUENT_WIRE_HEADER_BYTES = 5


def read_avro_schema(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("congestion-watch-windowed-aggregation")
        .config("spark.sql.shuffle.partitions", "12")
        .config("spark.jars.ivy", "/opt/spark-app/ivy-cache")
        .getOrCreate()
    )
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
            col("value").substr(
                CONFLUENT_WIRE_HEADER_BYTES + 1, 1000000
            ).alias("avro_payload"),
        )
        .select("kafka_key", from_avro(col("avro_payload"), avro_schema_json).alias("event"))
        .select("kafka_key", "event.*")
        .withColumnRenamed("data_as_of", "event_time")
    )

    valid = decoded.where(col("status") == 0)

    late_cutoff = expr(f"current_timestamp() - interval {WATERMARK_DELAY}")
    tagged = valid.withColumn("is_late", col("event_time") < late_cutoff)

    late_records = tagged.where(col("is_late"))

    aggregated = (
        tagged
        .where(~col("is_late"))
        .withWatermark("event_time", WATERMARK_DELAY)
        .groupBy(
            window(col("event_time"), WINDOW_DURATION, WINDOW_SLIDE),
            col("link_id"),
            col("borough"),
        )
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
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "link_id",
            "borough",
            "avg_speed_mph",
            "sample_count",
            "congestion_score",
        )
    )

    console_query = (
        aggregated.writeStream
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/windowed-console")
        .outputMode("update")
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

    dlq_query = (
        late_records
        .select(
            col("kafka_key"),
            to_avro(dlq_payload).alias("value"),
        )
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
