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
    cache_ttl_s: int

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
            delta_uri=os.getenv(
                "DELTA_URI", "s3://congestion-watch/gold/segment_windows"
            ),
            # Das Dashboard pollt im Sekundentakt, die Gold-Tabelle bekommt
            # aber nur alle 5 Minuten ein neues Fenster. Ohne Cache liest jede
            # Anfrage das Delta-Log neu — das kostet nur Zeit und liefert
            # dasselbe Ergebnis.
            cache_ttl_s=int(os.getenv("CACHE_TTL_S", "20")),
            s3_endpoint=os.getenv("S3_ENDPOINT", "http://minio:9000"),
            s3_access_key=os.getenv("S3_ACCESS_KEY") or None,
            s3_secret_key=os.getenv("S3_SECRET_KEY") or None,
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
