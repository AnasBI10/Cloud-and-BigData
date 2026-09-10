"""Konfiguration der Serving-API (SCRUM-79).

Gleiche Linie wie src/ingestion/common.py: alles kommt aus der Umgebung,
nichts ist im Image verdrahtet. Der Betrieb kommt aus ConfigMap und Secret
(SCRUM-81), damit dasselbe Artefakt lokal, auf kind und auf dem k3s laeuft.
"""

from __future__ import annotations

import logging
import os
import pathlib
from dataclasses import dataclass

log = logging.getLogger("serving.config")


@dataclass(frozen=True)
class Settings:
    # --- Gold-Layer -------------------------------------------------------
    # "fixture" = Entwicklungsmodus ohne Delta-Tabelle, "delta" = Abgabestand.
    # Der Wechsel ist eine Env-Variable, keine Code-Aenderung: die API haengt
    # am Vertrag aus docs/gold-contract.md, nicht an seiner Herkunft.
    gold_reader: str
    delta_uri: str
    baseline_uri: str
    cache_ttl_s: int
    baseline_ttl_s: int
    tumbling_only: bool

    # --- MinIO / S3 -------------------------------------------------------
    s3_endpoint: str
    s3_access_key: str | None
    s3_secret_key: str | None
    s3_region: str
    s3_allow_http: bool

    # --- Stammdaten -------------------------------------------------------
    seed_path: pathlib.Path

    # --- API --------------------------------------------------------------
    default_limit: int
    max_limit: int
    cors_origins: list[str]

    @staticmethod
    def from_env() -> "Settings":
        return Settings(
            gold_reader=os.getenv("GOLD_READER", "fixture").lower(),
            # Pfad wie ihn der Sink aus SCRUM-86 tatsaechlich schreibt
            # (streaming_job_bsg.py, GOLD_TABLE_PATH). Der frueher hier
            # eingetragene Bucket "congestion-watch" existiert nicht — Helm
            # legt nur "bronze" und "gold" an (values.yaml, minio.buckets).
            delta_uri=os.getenv("DELTA_URI", "s3://gold/congestion_scores"),
            # Zweite Tabelle, geschrieben von src/processing/compute_baseline.py.
            # Der Sink schreibt keine Baseline in die Gold-Tabelle, die API
            # verbindet beide (siehe readers.BaselineIndex).
            baseline_uri=os.getenv("BASELINE_URI", "s3://gold/baseline_profile"),
            # Das Dashboard pollt im Sekundentakt, die Gold-Tabelle bekommt
            # aber nur alle 5 Minuten ein neues Fenster. Ohne Cache liest jede
            # Anfrage das Delta-Log neu — das kostet nur Zeit und liefert
            # dasselbe Ergebnis.
            cache_ttl_s=int(os.getenv("CACHE_TTL_S", "20")),
            # Die Baseline ist ein Batch-Ergebnis ueber Wochen von Historie und
            # aendert sich nur, wenn compute_baseline.py neu laeuft. Sie
            # haeufiger zu lesen als der CronJob sie schreibt, bringt nichts.
            baseline_ttl_s=int(os.getenv("BASELINE_TTL_S", "900")),
            # Der Sink schreibt gleitende Fenster (5 Minuten, 1 Minute Slide),
            # also fuenf einander ueberlappende Zeilen je Segment und
            # Fuenfminutenblock. Fuer den Chart in SCRUM-90 ist das kein
            # Mehrwert, sondern vierfach dieselbe Messung. Auf "false"
            # stellen, wer die gleitende Sicht wirklich sehen will.
            tumbling_only=os.getenv("TUMBLING_ONLY", "true").lower() == "true",
            s3_endpoint=os.getenv("S3_ENDPOINT", "http://minio:9000"),
            # Fallback auf die MinIO-Variablen: das Secret "minio-credentials"
            # heisst im Cluster MINIO_ROOT_USER/MINIO_ROOT_PASSWORD und wird
            # per envFrom in mehrere Pods gereicht. Ohne diesen Fallback
            # startet die API dort ohne Zugangsdaten und laeuft im
            # Delta-Modus in ein 403.
            s3_access_key=os.getenv("S3_ACCESS_KEY") or os.getenv("MINIO_ROOT_USER") or None,
            s3_secret_key=os.getenv("S3_SECRET_KEY") or os.getenv("MINIO_ROOT_PASSWORD") or None,
            s3_region=os.getenv("S3_REGION", "us-east-1"),
            # MinIO laeuft im Cluster ohne TLS. delta-rs verweigert HTTP sonst.
            s3_allow_http=os.getenv("S3_ALLOW_HTTP", "true").lower() == "true",
            seed_path=pathlib.Path(
                os.getenv("SEED_PATH", "/app/data/dot_links_seed.json")
            ),
            default_limit=int(os.getenv("DEFAULT_LIMIT", "10")),
            # Deckel gegen versehentliche Vollabfragen ueber die Query-Parameter.
            max_limit=int(os.getenv("MAX_LIMIT", "200")),
            cors_origins=[
                o.strip() for o in os.getenv("CORS_ORIGINS", "*").split(",") if o.strip()
            ],
        )

    def storage_options(self) -> dict[str, str]:
        """Verbindungsparameter fuer delta-rs gegen MinIO."""
        opts: dict[str, str] = {
            "AWS_ENDPOINT_URL": self.s3_endpoint,
            "AWS_REGION": self.s3_region,
            # MinIO ist kein echtes S3: ohne diesen Schalter erwartet delta-rs
            # DynamoDB fuer das Commit-Locking und bricht beim Lesen ab.
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
