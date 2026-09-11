"""Wetter-Poller fuer den Enrichment-Join (SCRUM-84).

Holt stuendliche Beobachtungen von Open-Meteo je NYC-Borough und schreibt
sie nach weather.observations.raw. Bewusst ein eigener Modus statt eines
zweiten Images: teilt sich Serialisierung, Registry-Client und
Kafka-Konfiguration mit den Verkehrs-Producern (common.py).

Poll-Intervall 5 Minuten x 5 Boroughs = 1.440 Aufrufe/Tag, deutlich unter
dem 10.000er-Limit von Open-Meteo (siehe DATA_SOURCES.md). Bewusst nah an
der DOT-Meldefrequenz (~7,7 Min) gewaehlt - eine feinere Wetteraufloesung
braeuchte der Join nicht, da Open-Meteo ohnehin nur stuendlich aufloest.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

from common import EventPublisher, Settings

log = logging.getLogger("ingestion.weather")

WEATHER_SCHEMA_PATH = Path(
    os.getenv("WEATHER_SCHEMA_PATH", "/app/schemas/weather_observation_event.avsc")
)

WEATHER_TOPIC = os.getenv("WEATHER_TOPIC", "weather.observations.raw")
POLL_INTERVAL_S = int(os.getenv("WEATHER_POLL_INTERVAL_S", "300"))

API_URL = "https://api.open-meteo.com/v1/forecast"

# Repraesentative Koordinaten je Borough. Open-Meteo rundet auf den
# naechstgelegenen Gitterpunkt (1-11 km Aufloesung), bei Manhattan kann die
# Abweichung deshalb mehrere Kilometer betragen - dokumentiert in
# DATA_SOURCES.md als bewusst akzeptierte Einschraenkung.
BOROUGHS = {
    "Manhattan": (40.7831, -73.9712),
    "Brooklyn": (40.6782, -73.9442),
    "Queens": (40.7282, -73.7949),
    "Bronx": (40.8448, -73.8648),
    "Staten Island": (40.5795, -74.1502),
}

HOURLY_VARS = "temperature_2m,precipitation,snowfall,wind_speed_10m"


def derive_condition(precip_mm: float | None, snow_cm: float | None) -> str:
    """Textkategorie aus den numerischen Werten ableiten.

    Open-Meteo liefert keinen Klartext-Zustand in dieser Abfrage. Die
    Ableitung liegt bewusst hier im Producer und nicht im Consumer, damit
    alle Downstream-Verbraucher dieselbe Schwellenlogik sehen statt jeder
    eine eigene zu implementieren.
    """
    if snow_cm and snow_cm > 0:
        return "snow"
    if precip_mm is None or precip_mm <= 0:
        return "clear"
    if precip_mm < 0.5:
        return "drizzle"
    return "rain"


def fetch_borough(borough: str, lat: float, lon: float) -> dict | None:
    """Aktuellste verfuegbare Stunde fuer ein Borough holen."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": HOURLY_VARS,
        "forecast_days": 1,
        "timezone": "UTC",
    }
    try:
        resp = requests.get(API_URL, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        log.warning("Open-Meteo-Abruf fuer %s fehlgeschlagen: %s", borough, exc)
        return None

    hourly = data.get("hourly") or {}
    times = hourly.get("time") or []
    if not times:
        log.warning("Keine Stundenwerte fuer %s erhalten", borough)
        return None

    # Aktuellste Stunde, die nicht in der Zukunft liegt.
    now = datetime.now(timezone.utc)
    idx = 0
    for i, t in enumerate(times):
        ts = datetime.fromisoformat(t).replace(tzinfo=timezone.utc)
        if ts <= now:
            idx = i
        else:
            break

    observed_at = datetime.fromisoformat(times[idx]).replace(tzinfo=timezone.utc)

    def val(key: str):
        series = hourly.get(key) or []
        return series[idx] if idx < len(series) else None

    precip = val("precipitation")
    snow = val("snowfall")

    return {
        "borough": borough,
        "observed_at": observed_at,
        "event_key": f"{borough}|{observed_at.isoformat().replace('+00:00', 'Z')}",
        "temperature_c": val("temperature_2m"),
        "precipitation_mm": precip,
        "wind_speed_kmh": val("wind_speed_10m"),
        "weather_condition": derive_condition(precip, snow),
        # Tatsaechlich gelieferter Gitterpunkt, nicht der angefragte Wert.
        "latitude": float(data.get("latitude", lat)),
        "longitude": float(data.get("longitude", lon)),
        "ingested_at": datetime.now(timezone.utc),
        "source": "OPEN_METEO",
    }


def run(settings: Settings) -> None:
    # Topic kommt aus der ConfigMap (TOPIC=weather.observations.raw),
    # nicht hier hartkodiert - konsistent zu den anderen beiden Modi.

    import common

    original_schema_path = common.SCHEMA_PATH
    common.SCHEMA_PATH = WEATHER_SCHEMA_PATH
    try:
        publisher = EventPublisher(settings, key_field="borough")
    finally:
        common.SCHEMA_PATH = original_schema_path

    log.info(
        "Wetter-Poller gestartet: %d Boroughs, Intervall %d s, Topic %s",
        len(BOROUGHS),
        POLL_INTERVAL_S,
        WEATHER_TOPIC,
    )

    while True:
        published = 0
        for borough, (lat, lon) in BOROUGHS.items():
            event = fetch_borough(borough, lat, lon)
            if event is None:
                continue
            publisher.publish(event)
            published += 1

        publisher.flush()
        log.info("Zwischenstand: %d/%d Boroughs zugestellt", published, len(BOROUGHS))
        time.sleep(POLL_INTERVAL_S)
