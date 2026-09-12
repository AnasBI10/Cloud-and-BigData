import os

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.avro.functions import from_avro, to_avro
from pyspark.sql.functions import (
    avg, broadcast, coalesce, col, count, current_timestamp, date_format, dayofweek, expr,
    first, hour, lit, struct, when, window,
)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC_IN = os.environ.get("KAFKA_TOPIC_IN", "traffic.speeds.raw")
KAFKA_TOPIC_DLQ = os.environ.get("KAFKA_TOPIC_DLQ", "traffic.speeds.dlq")
KAFKA_TOPIC_WEATHER = os.environ.get("KAFKA_TOPIC_WEATHER", "weather.observations.raw")
SCHEMA_PATH = os.environ.get("SCHEMA_PATH", "/opt/spark-app/schemas/traffic_speed_event.avsc")
WEATHER_SCHEMA_PATH = os.environ.get(
    "WEATHER_SCHEMA_PATH", "/opt/spark-app/schemas/weather_observation_event.avsc"
)
SEED_PATH = os.environ.get("SEED_PATH", "/opt/spark-app/data/dot_links_seed.json")
BASELINE_TABLE_PATH = os.environ.get("BASELINE_TABLE_PATH", "s3a://gold/baseline_profile")
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
# War 65 Minuten, hat bei Left-Outer-Join-Semantik den Heap gesprengt (OOM,
# 12.09.). Traffic-Zeilen hängen bis zum Ablauf im Join-State. 20 Minuten reichen bei einem 5-Minuten-Poll-Intervall komfortabel
WEATHER_WATERMARK_DELAY = "20 minutes"
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


def read_weather_stream(spark: SparkSession, weather_schema_json: str) -> DataFrame:
    """Liest weather.observations.raw und dekodiert das Avro-Payload (SCRUM-84).

    borough/event_key/ingested_at/source kollidieren namentlich mit dem
    Traffic-Schema, deshalb hier mit weather_-Praefix umbenannt - sonst
    wirft der spaetere Join einen AMBIGUOUS_REFERENCE-Fehler. Watermark auf
    observed_at (fachliche Beobachtungszeit), nicht auf ingested_at.
    """
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC_WEATHER)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    decoded = (
        raw.select(
            col("value").substr(CONFLUENT_WIRE_HEADER_BYTES + 1, 1000000).alias("avro_payload"),
        )
        .select(from_avro(col("avro_payload"), weather_schema_json).alias("w"))
        .select("w.*")
        .withColumnRenamed("borough", "weather_borough")
        .withColumnRenamed("event_key", "weather_event_key")
        .withColumnRenamed("ingested_at", "weather_ingested_at")
        .withColumnRenamed("source", "weather_source")
        .withColumnRenamed("latitude", "weather_latitude")
        .withColumnRenamed("longitude", "weather_longitude")
    )

    return decoded.withWatermark("observed_at", WEATHER_WATERMARK_DELAY)


def load_baseline(spark: SparkSession) -> DataFrame:
    """Wird eimalig beim Start geladen und per Broadcast bereitgestellt, nicht jedesmal neu berechnet"""
    return (
        spark.read.format("delta").load(BASELINE_TABLE_PATH)
        .select("link_id", "weekday", "hour_of_day", "baseline_speed", "baseline_stddev")
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
        .config(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
        )
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
            .partitionBy("window_date")
            .save(path)
        )
        return

    # SCRUM-84b: autoMerge aktiviert Schema-Evolution fuer MERGE-Operationen.
    # Ohne dieses Flag ignoriert Delta neue Spalten aus source (hier: die
    # Wetter-Felder) beim Merge in eine bereits bestehende Zieltabelle -
    # anders als beim initialen .write mit mergeSchema=true, das nur beim
    # allerersten Anlegen der Tabelle greift.
    batch_df.sparkSession.conf.set(
        "spark.databricks.delta.schema.autoMerge.enabled", "true"
    )
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


def build_process_batch(baseline: DataFrame):
    """erzeugt process_batch als funktion mit enthaltenem zustand, damit die einmal geladene baseline in jedem batch verfügbar ist"""

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
                avg("speed_mph").alias("speed_avg"),
                count("*").alias("sample_count"),
                first("link_name", ignorenulls=True).alias("link_name"),
                first("link_points", ignorenulls=True).alias("link_points"),
                # SCRUM-84b: Wetter-Aggregate aus dem Stream-Stream-Join.
                # Spaltennamen exakt wie im Gold-Contract (docs/gold-contract.md)
                # dokumentiert - ohne weather_-Praefix, damit der bestehende
                # DeltaReader (SCRUM-79) sie ohne Anpassung liest.
                # avg() ignoriert NULLs automatisch (kein Match gefunden).
                avg("temperature_c").alias("temperature_c"),
                avg("precipitation_mm").alias("precipitation_mm"),
                avg("wind_speed_kmh").alias("wind_speed_kmh"),
                first("weather_condition", ignorenulls=True).alias("weather_condition"),
            )
            .withColumn("window_date", date_format(col("window_start"), "yyyy-MM-dd"))
            # gleiche berechnung wie in compute_baseline.py, auch keine tz konvertierung
            .withColumn("weekday", dayofweek(col("window_start")))
            .withColumn("hour_of_day", hour(col("window_start")))
        )

        joined = (
            gold_agg
            .join(broadcast(baseline), on=["link_id", "weekday", "hour_of_day"], how="left")
            .drop("weekday", "hour_of_day")
            # has_baseline erfordert stddev > 0
            .withColumn(
                "has_baseline",
                col("baseline_speed").isNotNull()
                & col("baseline_stddev").isNotNull()
                & (col("baseline_stddev") > 0),
            )
            .withColumn(
                "congestion_score",
                when(
                    col("has_baseline"),
                    (col("baseline_speed") - col("speed_avg")) / col("baseline_stddev"),
                ),
                # kein .otherwise(): bleibt NULL ohne Baseline, wie im Gold-Contract steht
            )
            # Umbenannt von late_event_detected (SCRUM-86) auf den Contract-Namen
            # is_late_arrival. 
            .withColumn("is_late_arrival", lit(False))
            .withColumn("updated_at", current_timestamp())
        )
        upsert_gold(joined, GOLD_TABLE_PATH)

        batch_df.unpersist()

    return process_batch


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

    baseline = load_baseline(spark)

    weather_schema_json = read_avro_schema(WEATHER_SCHEMA_PATH)
    weather = read_weather_stream(spark, weather_schema_json)

    # is_late wird VOR dem Wetter-Join berechnet, tagged_traffic haengt
    # deshalb ausschliesslich am Traffic-Stream (SCRUM-84 Nachtrag). Grund:
    # die DLQ-Query weiter unten braucht denselben Source-Count wie ihr
    # bestehender Checkpoint (late-data-dlq kennt nur 1 Source aus der Zeit
    # vor dem Wetter-Join). Wuerde late_records aus der bereits gejointen
    # Query abgeleitet, haette auch die DLQ-Query 2 Sources und der
    # Checkpoint waere inkompatibel - derselbe AssertionError wie eben bei
    # bronze-silver-gold, nur fuer eine Query, die inhaltlich gar kein
    # Wetter braucht.
    late_cutoff = expr(f"current_timestamp() - interval {WATERMARK_DELAY}")
    tagged_traffic = enriched.withColumn("is_late", col("event_time") < late_cutoff)

    # Watermark muss vor dem Stream-Stream-Join auf beiden Seiten gesetzt
    # sein, sonst haelt Spark den Join-State unbegrenzt im Speicher.
    enriched_watermarked = tagged_traffic.withWatermark("event_time", WATERMARK_DELAY)

    with_weather = (
        enriched_watermarked.alias("t")
        .join(
            weather.alias("w"),
            expr(
                "t.borough = w.weather_borough AND "
                "t.event_time >= w.observed_at AND "
                "t.event_time < w.observed_at + interval 1 hour"
            ),
            "left",
        )
        .drop("weather_borough")
    )

    window_col = window(col("event_time"), WINDOW_DURATION, WINDOW_SLIDE)
    windowed = (
        with_weather
        .withColumn("window", window_col)
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop("window")
        .withColumnRenamed("is_late", "is_late_marker")
    )

    query = (
        windowed.writeStream
        .foreachBatch(build_process_batch(baseline))
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

    late_records = tagged_traffic.where(col("is_late"))
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
