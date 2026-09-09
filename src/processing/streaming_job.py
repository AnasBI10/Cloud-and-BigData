import os

from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "traffic.speeds.raw")
SCHEMA_PATH = os.environ.get("SCHEMA_PATH", "/opt/spark-app/schemas/traffic_speed_event.avsc")
CHECKPOINT_DIR = os.environ.get("CHECKPOINT_DIR", "/opt/spark-app/checkpoints/console-sink")

CONFLUENT_WIRE_HEADER_BYTES = 5


def read_avro_schema(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("congestion-watch-processing")
        .config("spark.sql.shuffle.partitions", "12")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    avro_schema_json = read_avro_schema(SCHEMA_PATH)

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    payload_without_header = raw.select(
        col("key").cast("string").alias("kafka_key"),
        col("timestamp").alias("kafka_timestamp"),
        col("value").substr(
            CONFLUENT_WIRE_HEADER_BYTES + 1, 1000000
        ).alias("avro_payload"),
    )

    decoded = payload_without_header.select(
        col("kafka_key"),
        col("kafka_timestamp"),
        from_avro(col("avro_payload"), avro_schema_json).alias("event"),
    ).select("kafka_key", "kafka_timestamp", "event.*")

    query = (
        decoded.writeStream
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", CHECKPOINT_DIR)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
