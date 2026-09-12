"""Antwortmodelle der Serving-API (SCRUM-79)"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

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
            "Positiv = langsamer als erwartet. Kommt fertig aus dem Streaming-Job."
        ),
    )
    has_baseline: bool = Field(
        False,
        description="false bei Segmenten ohne ausreichende Historie — unbewertbar, nicht unauffaellig.",
    )

    borough: str | None = None
    link_name: str | None = None
    link_points: str | None = None

    weather_condition: str | None = None
    temperature_c: float | None = None
    precipitation_mm: float | None = None

    is_late_arrival: bool = False


class BaselineCell(BaseModel):
    baseline_speed: float
    baseline_stddev: float


class BaselineResponse(BaseModel):
    generated_at: datetime
    at: datetime = Field(description="Zeitpunkt, fuer den die Zelle gilt")
    count: int
    items: dict[str, BaselineCell]


class AnomalyResponse(BaseModel):
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
    ingest: str | None = Field(None, description="kafka | dryrun")


class EventRequest(BaseModel):
    """Ein von Hand erzeugtes Messereignis, entspricht dem Avro-Schema aus SCRUM-74."""

    link_id: str = Field(description="Segment aus data/dot_links_seed.json")
    status: Literal[0, -101] = Field(
        0,
        description=(
            "0 = gueltige Messung, -101 = Sentinel. -101 wird in Silver "
            "verworfen und erreicht die Gold-Schicht nie."
        ),
    )
    speed_mph: float | None = Field(None, ge=0, le=120)
    travel_time_s: int | None = Field(None, ge=0)
    data_as_of: datetime | None = Field(None, description="Messzeitpunkt. Ohne Angabe: jetzt.")
    allow_late: bool = Field(
        False,
        description=(
            "Erlaubt einen Zeitstempel aelter als die Watermark. Das Event landet "
            "dann in der DLQ statt in der Aggregation (SCRUM-85)."
        ),
    )


class EventAck(BaseModel):
    published: bool
    ingest: str = Field(description="kafka | dryrun")
    topic: str
    link_id: str
    event_key: str
    data_as_of: datetime
    status: int
    speed_mph: float | None
    late: bool = Field(False, description="true = geht in die DLQ, nicht in die Aggregation")
    note: str


class ScenarioRequest(BaseModel):
    link_id: str
    scenario: Literal["congestion", "recovery", "sensor_outage"]
    duration_minutes: int = Field(10, ge=1)
    events_per_minute: int = Field(6, ge=1)
    reference_speed_mph: float | None = Field(
        None,
        ge=1,
        le=120,
        description="Ausgangsgeschwindigkeit. Ohne Angabe die Baseline des Segments, sonst ein Ersatzwert.",
    )


class ScenarioStatus(BaseModel):
    scenario_id: str
    scenario: str
    link_id: str
    state: str = Field(description="running | done | failed | cancelled")
    detail: str | None = None
    planned_events: int
    published_events: int
    duration_minutes: int
    events_per_minute: int
    reference_speed: float
    started_at: datetime
    finished_at: datetime | None = None
    expected_effect_at: datetime
    ingest: str | None = None


class ScenarioOption(BaseModel):
    name: str
    beschreibung: str


class ScenarioListResponse(BaseModel):
    available: list[ScenarioOption]
    runs: list[ScenarioStatus]
