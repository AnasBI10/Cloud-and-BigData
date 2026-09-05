# Gold-Layer-Vertrag (SCRUM-79 ↔ SCRUM-82/83/84/86)

Schnittstelle zwischen Processing (VT) und Serving (BT). Der Delta-Sink aus
SCRUM-86 schreibt genau dieses Schema, die Serving-API liest genau dieses
Schema. Aenderungen nur abgestimmt — die API bricht sonst still.

Stand: 04.09.2026. Abstimmung noch offen.

## Tabelle

Pfad: `s3a://congestion-watch/gold/segment_windows`

| Spalte | Typ | Bedeutung |
|---|---|---|
| `link_id` | string | Segment-ID aus dem DOT-Feed |
| `window_start` | timestamp (UTC) | Beginn des Aggregationsfensters |
| `window_end` | timestamp (UTC) | Ende des Aggregationsfensters |
| `window_date` | string `YYYY-MM-DD` | **Partitionsspalte**, abgeleitet aus `window_start` |
| `speed_avg` | double | Mittlere Geschwindigkeit im Fenster (mph), nur aus `status = 0` |
| `sample_count` | int | Anzahl gueltiger Messungen im Fenster |
| `baseline_speed` | double, nullable | Erwartungswert aus der Baseline |
| `baseline_stddev` | double, nullable | Streuung der Baseline-Zelle |
| `congestion_score` | double, nullable | Standardisierte Abweichung, siehe unten |
| `has_baseline` | boolean | `false` fuer Segmente ohne ausreichende Historie |
| `borough` | string, nullable | Join-Schluessel Wetter |
| `link_name` | string, nullable | Klartext fuer die Anzeige |
| `link_points` | string, nullable | Polylinie fuer die Karte |
| `weather_condition` | string, nullable | Wetterlage aus dem Enrichment-Join |
| `temperature_c` | double, nullable | Temperatur zum Fensterzeitpunkt |
| `precipitation_mm` | double, nullable | Niederschlag zum Fensterzeitpunkt |
| `is_late_arrival` | boolean | `true`, wenn das Fenster durch verspaetete Events korrigiert wurde |

## Festlegungen, die abgestimmt sein muessen

**Fenstergroesse: 5 Minuten, tumbling.** Bei einer Meldefrequenz von ~7,7 Minuten
je Sensor enthaelt nicht jedes Fenster fuer jedes Segment einen Wert. Fenster
ohne gueltige Messung werden **nicht** geschrieben — die API interpoliert nicht,
sie zeigt Luecken als Luecken.

**Vorzeichen des Score: positiv = langsamer als erwartet = auffaellig.**

    congestion_score = (baseline_speed - speed_avg) / baseline_stddev

Ein Wert von +2.0 heisst: zwei Standardabweichungen langsamer als fuer diesen
`link_id` × Wochentag × Stunde ueblich. Negative Werte (schneller als erwartet)
sind gueltig und werden geschrieben, aber im Dashboard nicht als Anomalie
gelistet.

**`has_baseline = false` ist kein Fehler, sondern ein Befund.** 31 der 125
Segmente haben keine Historie im Exportzeitraum (README, Abschnitt 2, Veracity).
Fuer diese bleiben `baseline_speed`, `baseline_stddev` und `congestion_score`
`null`. Die API zaehlt sie separat aus und weist sie im Dashboard als
*unbewertbar* aus, nicht als *unauffaellig*. Das ist der Unterschied zwischen
"kein Stau" und "wissen wir nicht".

**Baseline-Raster:** `link_id` × Wochentag × Stunde ergibt 168 Zellen je Segment.
Bei 940k Zeilen / 94 Sensoren und ~49 % Sentinel-Anteil bleiben rechnerisch nur
**~30 Messungen je Zelle**. Fuer Mittelwert + Standardabweichung ist das duenn.
Zwei Auswege, einer davon muss in Kapitel 5 begruendet stehen:

1. Groeberes Raster: Werktag/Wochenende × 2-Stunden-Buckets = 24 Zellen,
   ~210 Messungen je Zelle.
2. Robuste Statistik: Median + IQR statt Mittelwert + Sigma, `congestion_score`
   dann als robuster z-Score `(median - speed_avg) / (IQR/1.349)`.

Die API rechnet nicht — sie liest `congestion_score`, wie er geschrieben wurde.
Welche Variante gewaehlt wird, aendert am Vertrag nichts.

**Zeitzone: alles UTC.** Umrechnung auf `America/New_York` passiert erst in der
UI. Der Baseline-Schluessel (Wochentag, Stunde) bezieht sich dagegen auf lokale
NYC-Zeit — sonst verschiebt sich der Feierabendverkehr je nach Sommerzeit um
eine Stunde. Diese Umrechnung gehoert in den Spark-Job, nicht in die API.

## Was die API zusaetzlich braucht

`GET /api/segments` liefert die Kartengrundlage fuer alle 125 Segmente, auch fuer
die ohne aktuelle Messung. Quelle ist `data/dot_links_seed.json`, angereichert um
den letzten bekannten Zustand aus der Gold-Tabelle. Der Spark-Job muss dafuer
nichts zusaetzlich liefern.
