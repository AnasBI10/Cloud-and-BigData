"""Antwortmodelle der Serving-API (SCRUM-79).

Diese Datei ist die ausfuehrbare Fassung von docs/gold-contract.md. Wenn der
Delta-Sink (SCRUM-86) ein Feld anders benennt, faellt es hier auf und nicht
erst im Dashboard.
"""

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
            "Positiv = langsamer als erwartet. Wird von der API aus der "
            "Baseline-Tabelle berechnet, weil der Gold-Sink keine Baseline "
            "mitschreibt (siehe docs/gold-contract.md)."
        ),
    )
    speed_index: float | None = Field(
        None,
        description=(
            "Der Score, den der Spark-Job selbst in die Gold-Tabelle schreibt: "
            "0-100 allein aus der Absolutgeschwindigkeit, ohne Baseline. "
            "Unveraendert durchgereicht, damit nichts stillschweigend "
            "umgedeutet wird. Fuer die Anomalie-Rangliste zaehlt "
            "congestion_score, nicht dieser Wert."
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
    ingest: str | None = Field(
        None, description="kafka | dryrun — Modus der Einspeisung (SCRUM-89)"
    )


# ---------------------------------------------------------------------------
# Einspeisung (SCRUM-89)
# ---------------------------------------------------------------------------


class EventRequest(BaseModel):
    """Ein von Hand erzeugtes Messereignis.

    Entspricht genau dem Avro-Schema aus SCRUM-74, mit denselben Feldern, die
    auch der DOT-Feed liefert. Was hier hineingeht, ist von einem echten Event
    im Topic nicht zu unterscheiden — ausser durch den Zeitstempel.
    """

    link_id: str = Field(description="Segment aus data/dot_links_seed.json")
    status: Literal[0, -101] = Field(
        0,
        description=(
            "0 = gueltige Messung, -101 = Sentinel des Feeds. Bewusst waehlbar, "
            "um den Statusfilter des Spark-Jobs vorzufuehren: -101 wird in "
            "Silver verworfen und erreicht die Gold-Schicht nie."
        ),
    )
    speed_mph: float | None = Field(None, ge=0, le=120)
    travel_time_s: int | None = Field(None, ge=0)
    data_as_of: datetime | None = Field(
        None, description="Messzeitpunkt. Ohne Angabe: jetzt."
    )
    allow_late: bool = Field(
        False,
        description=(
            "Erlaubt einen Zeitstempel aelter als die Watermark. Das Event "
            "landet dann nicht in der Aggregation, sondern in der DLQ — "
            "genau der Weg, den SCRUM-85 fuer verspaetete Daten vorsieht. "
            "Ohne dieses Flag wird ein solcher Zeitstempel abgelehnt, damit "
            "niemand vergeblich auf eine Aenderung im Dashboard wartet."
        ),
    )


class EventAck(BaseModel):
    """Quittung. Traegt das tatsaechlich gesendete Event mit, damit sichtbar
    bleibt, was die API ergaenzt oder korrigiert hat (etwa das
    Sentinel-Triplett bei status=-101)."""

    published: bool
    ingest: str = Field(description="kafka | dryrun")
    topic: str
    link_id: str
    event_key: str
    data_as_of: datetime
    status: int
    speed_mph: float | None
    late: bool = Field(
        False, description="true = geht in die DLQ, nicht in die Aggregation"
    )
    note: str


class ScenarioRequest(BaseModel):
    """Eine Folge von Events ueber mehrere Minuten.

    Ein einzelnes Event veraendert einen Fenstermittelwert kaum sichtbar. Fuer
    eine Vorfuehrung braucht es eine Entwicklung ueber die Zeit.
    """

    link_id: str
    scenario: Literal["congestion", "recovery", "sensor_outage"]
    duration_minutes: int = Field(10, ge=1)
    events_per_minute: int = Field(6, ge=1)
    reference_speed_mph: float | None = Field(
        None,
        ge=1,
        le=120,
        description=(
            "Ausgangsgeschwindigkeit. Ohne Angabe nimmt die API die Baseline "
            "des Segments fuer diese Stunde, sonst einen Ersatzwert."
        ),
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
    expected_effect_at: datetime = Field(
        description=(
            "Frueheste Zeit, zu der die Aenderung im Dashboard stehen kann: "
            "Fensterversatz plus Trigger-Intervall des Spark-Jobs."
        )
    )
    ingest: str | None = None


class ScenarioOption(BaseModel):
    name: str
    beschreibung: str


class ScenarioListResponse(BaseModel):
    """Katalog und die Laeufe DIESES Pods.

    Bei mehreren Repliken beantwortet ein anderer Pod diese Anfrage
    moeglicherweise, ohne den Lauf zu kennen. Die UI verlaesst sich deshalb
    auf die Antwort des POST und nutzt diese Liste nur zur Kontrolle.
    """

    available: list[ScenarioOption]
    runs: list[ScenarioStatus]
