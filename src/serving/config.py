"""Konfiguration der Serving-API (SCRUM-79)."""

from __future__ import annotations

import logging
import os
import pathlib
from dataclasses import dataclass

log = logging.getLogger("serving.config")


@dataclass(frozen=True)
class Settings:
    gold_reader: str
    delta_uri: str
    baseline_uri: str
    cache_ttl_s: int
    baseline_ttl_s: int
    tumbling_only: bool

    s3_endpoint: str
    s3_access_key: str | None
    s3_secret_key: str | None
    s3_region: str
    s3_allow_http: bool

    seed_path: pathlib.Path

    ingest_mode: str
    watermark_delay_s: int
    max_scenario_minutes: int
    max_events_per_minute: int

    default_limit: int
    max_limit: int
    cors_origins: list[str]

    @staticmethod
    def from_env() -> "Settings":
        return Settings(
            gold_reader=os.getenv("GOLD_READER", "fixture").lower(),
            delta_uri=os.getenv("DELTA_URI", "s3://gold/congestion_scores"),
            baseline_uri=os.getenv("BASELINE_URI", "s3://gold/baseline_profile"),
            cache_ttl_s=int(os.getenv("CACHE_TTL_S", "20")),
            baseline_ttl_s=int(os.getenv("BASELINE_TTL_S", "900")),
            tumbling_only=os.getenv("TUMBLING_ONLY", "true").lower() == "true",
            s3_endpoint=os.getenv("S3_ENDPOINT", "http://minio:9000"),
            # Fallback auf die Cluster-Secret-Namen MINIO_ROOT_USER/PASSWORD.
            s3_access_key=os.getenv("S3_ACCESS_KEY") or os.getenv("MINIO_ROOT_USER") or None,
            s3_secret_key=os.getenv("S3_SECRET_KEY") or os.getenv("MINIO_ROOT_PASSWORD") or None,
            s3_region=os.getenv("S3_REGION", "us-east-1"),
            s3_allow_http=os.getenv("S3_ALLOW_HTTP", "true").lower() == "true",
            seed_path=pathlib.Path(
                os.getenv("SEED_PATH", "/app/data/dot_links_seed.json")
            ),
            ingest_mode=os.getenv("INGEST_MODE", "kafka").lower(),
            # Muss zu WATERMARK_DELAY in streaming_job_bsg.py passen.
            watermark_delay_s=int(os.getenv("WATERMARK_DELAY_S", "120")),
            max_scenario_minutes=int(os.getenv("MAX_SCENARIO_MINUTES", "60")),
            max_events_per_minute=int(os.getenv("MAX_EVENTS_PER_MINUTE", "60")),
            default_limit=int(os.getenv("DEFAULT_LIMIT", "10")),
            max_limit=int(os.getenv("MAX_LIMIT", "200")),
            cors_origins=[
                o.strip() for o in os.getenv("CORS_ORIGINS", "*").split(",") if o.strip()
            ],
        )

    def storage_options(self) -> dict[str, str]:
        opts: dict[str, str] = {
            "AWS_ENDPOINT_URL": self.s3_endpoint,
            "AWS_REGION": self.s3_region,
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        }
        if self.s3_allow_http:
            opts["AWS_ALLOW_HTTP"] = "true"
        if self.s3_access_key and self.s3_secret_key:
            opts["AWS_ACCESS_KEY_ID"] = self.s3_access_key
            opts["AWS_SECRET_ACCESS_KEY"] = self.s3_secret_key
        return opts


def setup_logging() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
