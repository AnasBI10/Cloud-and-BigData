"""Antwortmodelle der Serving-API (SCRUM-79).

Diese Datei ist die ausfuehrbare Fassung von docs/gold-contract.md. Wenn der
Delta-Sink (SCRUM-86) ein Feld anders benennt, faellt es hier auf und nicht
erst im Dashboard.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class SegmentWindow(BaseModel):
    """Ein aggregiertes Zeitfenster eines Strassensegments."""

    link_id: str
    window_start: datetime
    window_end: datetime
    speed_avg: float | None = Field(None, description="mph, nur aus status = 0")
    sample_count: int = 0

    baseline_speed: float | None = None
    baseline_stddev: float | None = None
    congestion_score: float | None = Field(
        None,
        description=(
            "(baseline_speed - speed_avg) / baseline_stddev. "
            "Positiv = langsamer als erwartet."
        ),
    )
    has_baseline: bool = Field(
        False,
        description=(
            "false bei Segmenten ohne ausreichende Historie. Diese sind nicht "
            "unauffaellig, sondern unbewertbar."
        ),
    )

    borough: str | None = None
    link_name: str | None = None
    link_points: str | None = None

    weather_condition: str | None = None
    temperature_c: float | None = None
    precipitation_mm: float | None = None

    is_late_arrival: bool = False


class AnomalyResponse(BaseModel):
    """Antwortumschlag fuer die Top-N-Anomalien.

    Der Umschlag traegt bewusst die Datenqualitaets-Kennzahlen mit: ein
    Dashboard, das nur `items` zeigt, wuerde die Segmente ohne Baseline
    stillschweigend als unauffaellig darstellen.
    """

    generated_at: datetime
    reader: str = Field(description="fixture | delta")
    latest_window: datetime | None = None
    total_segments: int
    segments_with_baseline: int
    segments_without_baseline: int
    items: list[SegmentWindow]


class TimeseriesResponse(BaseModel):
    link_id: str
    link_name: str | None = None
    borough: str | None = None
    has_baseline: bool
    hours: int
    points: list[SegmentWindow]


class Segment(BaseModel):
    """Kartengrundlage: alle bekannten Segmente, auch die ohne aktuelle Messung."""

    link_id: str
    link_name: str | None = None
    borough: str | None = None
    link_points: str | None = None
    has_baseline: bool = False
    last_seen: datetime | None = None
    last_speed: float | None = None
    last_score: float | None = None


class SegmentsResponse(BaseModel):
    generated_at: datetime
    count: int
    items: list[Segment]


class Health(BaseModel):
    status: str
    reader: str
    detail: str | None = None
    latest_window: datetime | None = None
