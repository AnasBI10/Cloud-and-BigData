"""Einmaliger Bootstrap: laedt historische DOT-Messungen von Socrata direkt
in die Silver-Schicht (Kappa-Ausnahme, siehe DATA_SOURCES.md)."""

import json
import os
import time
import urllib.request
from datetime import datetime
from urllib.parse import urlencode

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

SOCRATA_ENDPOINT = "https://data.cityofnewyork.us/resource/i4gi-tjb9.json"
APP_TOKEN = os.environ.get("SOCRATA_APP_TOKEN")
FROM_DATE = os.environ.get("BACKFILL_FROM", "2026-07-21T00:00:00")
PAGE_SIZE = int(os.environ.get("BACKFILL_PAGE_SIZE", "50000"))

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "admin")
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "")
SILVER_TABLE_PATH = os.environ.get("SILVER_TABLE_PATH", "s3a://bronze/traffic_speeds_valid")

SCHEMA = StructType([
    StructField("link_id", StringType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("speed_mph", DoubleType(), True),
    StructField("travel_time_s", IntegerType(), True),
    StructField("status", IntegerType(), True),
    StructField("borough", StringType(), True),
    StructField("link_name", StringType(), True),
    StructField("source", StringType(), True),
])


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("congestion-watch-backfill-silver")
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


def parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def fetch_page(cursor: str) -> list[dict]:
    # $offset-Pagination wird bei SODA2 ab grosser Tiefe instabil (HTTP 500) --
    # deshalb Cursor auf data_as_of statt Offset, kein Tiefenlimit.
    params = {
        "$where": f"data_as_of >= '{cursor}' AND status='0'",
        "$order": "data_as_of",
        "$limit": PAGE_SIZE,
    }
    url = f"{SOCRATA_ENDPOINT}?{urlencode(params)}"
    request = urllib.request.Request(url)
    if APP_TOKEN:
        request.add_header("X-App-Token", APP_TOKEN)

    # Socrata liefert vereinzelt transiente 500er (siehe DATA_SOURCES.md,
    # Cache-Verhalten), mit Backoff erneut versuchen statt abzubrechen.
    last_error = None
    for attempt in range(5):
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                return json.loads(response.read())
        except urllib.error.HTTPError as exc:
            last_error = exc
            wait = 5 * (attempt + 1)
            print(f"Socrata-Fehler {exc.code} bei Cursor {cursor}, Versuch {attempt + 1}/5, warte {wait}s")
            time.sleep(wait)
    raise last_error


def to_row(record: dict) -> tuple:
    speed = record.get("speed")
    travel = record.get("travel_time")
    return (
        record.get("link_id"),
        parse_ts(record.get("data_as_of")),
        float(speed) if speed not in (None, "") else None,
        int(float(travel)) if travel not in (None, "") else None,
        0,  # serverseitig bereits auf status='0' gefiltert
        record.get("borough"),
        record.get("link_name"),
        "REPLAY",
    )


def main() -> None:
    if not APP_TOKEN:
        print("WARNUNG: kein SOCRATA_APP_TOKEN gesetzt -- anonymes Rate-Limit greift.")

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    cursor = FROM_DATE
    total = 0
    while True:
        page = fetch_page(cursor)
        if not page:
            break
        rows = [to_row(r) for r in page]
        df = spark.createDataFrame(rows, schema=SCHEMA)
        (
            df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(SILVER_TABLE_PATH)
        )
        total += len(rows)
        print(f"Backfill: {len(rows)} Zeilen geschrieben (Cursor {cursor}), gesamt {total}")
        next_cursor = page[-1]["data_as_of"]
        if next_cursor == cursor or len(page) < PAGE_SIZE:
            break
        cursor = next_cursor

    print(f"Backfill abgeschlossen: {total} Zeilen seit {FROM_DATE} nach {SILVER_TABLE_PATH}")


if __name__ == "__main__":
    main()
