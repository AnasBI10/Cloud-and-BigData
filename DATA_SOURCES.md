# DATA_SOURCES.md

Dokumentation der genutzten und geprüften Datenquellen für NYC Congestion Watch.
Pflicht nach Local Law 11 (§ 23-502 d): Quelle, Version/Abrufdatum und
vorgenommene Änderungen sind bei Weiterveröffentlichung anzugeben.

Stand: 02.09.2026 — Datenbeschaffung für Sprint 0 abgeschlossen.
Offene Anschlusspunkte für spätere Sprints stehen am Ende dieser Datei.

---

## 1. NYC DOT Traffic Speeds NBE (Leitquelle)

| | |
|---|---|
| Datensatz-ID | `i4gi-tjb9` |
| Endpunkt | `https://data.cityofnewyork.us/resource/i4gi-tjb9.json` |
| API-Version | SODA2 (bestätigt am 01.09.2026, kein Bruch durch die laufende SODA3-Migration) |
| Zugriff | App Token (`X-App-Token`-Header), erstellt am 01.09.2026 |
| Lizenz | NYC Open Data / Local Law 11 (2012), § 23-502 (d) — keine Registrierungs-, Lizenz- oder Nutzungsbeschränkung; Pflicht zur Angabe von Quelle, Version, Änderungen bei Weiterveröffentlichung |
| Gewährleistung | Ausdrücklich ausgeschlossen (§ 23-504) |

### Feldsatz

```
id, speed, travel_time, status, data_as_of, link_id, link_points,
encoded_poly_line, encoded_poly_line_lvls, owner, transcom_id,
borough, link_name
```

Typen laut API: `id`, `speed`, `travel_time`, `status` = text;
`data_as_of` = floating_timestamp; alle übrigen Felder = text.

**Verifizierte Schlüsselstruktur:** `link_id` und `transcom_id` sind
identisch und referenzieren dieselbe stabile TRANSCOM-Segment-Kennung
(Beispiel: `4362250`/`4362250`, Cross Island Parkway nordbound bei
Willets Point Blvd, Queens). `id` ist **kein** eindeutiger
Record-Identifier — am Bulk-Export bestätigt: `id` hat exakt so viele
distinkte Werte wie `link_id` und korrespondiert konstant mit genau
einer `link_id` über hunderte Zeitstempel hinweg. **Es gibt kein Feld,
das eine einzelne Messung eindeutig identifiziert** — der einzige
verlässliche Schlüssel ist die Kombination `link_id` + `data_as_of`.
→ **Baseline- und Partitionierungsschlüssel: `link_id`. Avro-Schema
(SCRUM-74) braucht den zusammengesetzten Schlüssel `link_id` +
`data_as_of` für Kafka-Message-Key und Deduplizierung.**

**Weitere Feldeigenschaften:**
- `speed`/`travel_time` sind trotz `text`-Typisierung sauber numerisch
  formatiert (Dezimalpunkt, keine Einheiten im String) — einfache
  Konvertierung im Job.
- `link_points` enthält eine vollständige Polylinie (Lat/Lon-Punktfolge)
  je Segment — Segmentgeometrie direkt nutzbar für die Kartenanzeige in
  SCRUM-90, keine separate Geometriequelle nötig.
- `owner` = `"NYC-DOT-Region 10"` (bislang einziger beobachteter Wert).

### Cache-Verhalten

Response-Header zeigen mitunter `X-SODA2-Data-Out-Of-Date: true` mit
einem `Truth-Last-Modified`-Zeitstempel bis zu ~3 Stunden vor der
tatsächlichen Antwortzeit. **Konsequenz:** Die reale Aktualisierungs-
frequenz des Feeds kann von der Sensor-Meldefrequenz abweichen — bei der
Poll-Intervall-Wahl (SCRUM-81) berücksichtigen, nicht ungeprüft von der
Meldefrequenz ableiten.

### Kennzahlen (Stand 01.09.2026, 24h-Fenster)

| Kennzahl | Wert |
|---|---|
| Aktive Sensoren (`link_id`) | **125** |
| Records/Tag (alle Status) | **23.394** |
| Abgeleitete Meldefrequenz je Sensor | 23.394 ÷ 125 ≈ **187/Tag** ≈ alle 7,7 Min. |
| Bytes/Record (roh, JSON) | **~624** (Stichprobe, 100 Records gemittelt) |
| Abgeleiteter Durchsatz | 23.394 × 624 Byte ≈ **14,6 MB/Tag** (nur DOT-Feed, roh) |
| Kumulierte Records gesamt | 58,8 Mio. (Stand 09.03.2022, öffentliche Metadaten) |
| Datensatz-Beginn | 17.04.2017 |

Hinweis zur Methodik: `count(distinct link_id)` bzw. `count(*)` ohne
Zeitfilter über die komplette Historie läuft in einen Timeout
(Full-Table-Scan über ~59 Mio. Zeilen, keine Fehlermeldung, einfach
keine Antwort). Alle Kennzahlen-Queries daher auf ein 24h-Zeitfenster
eingegrenzt — für Baseline-Dichte ist das ohnehin die richtige
Bezugsgröße (aktuell aktive Sensoren), nicht die historische Gesamtzahl.

### Status-Feld — Sentinel-Wert verifiziert

Verteilung im 24h-Fenster:

| `status` | count | Anteil |
|---|---:|---:|
| `-101` | 12.755 | ~49 % |
| `0` | 13.389 | ~51 % |

`status=-101` geht in ~84 % der Fälle (10.704 von 12.755) exakt mit
`speed=0` und `travel_time=0` einher — ein Sentinel-Triplett für „keine
gültige Messung", keine reale Nullgeschwindigkeit. `status=0` streut über
plausible, realistische Geschwindigkeitswerte und ist der gültige
Datenpunkt.

**Veracity-Argument (belastbar, selbst gemessen):** Rund die Hälfte
aller Rohmeldungen im DOT-Feed ist keine gültige Geschwindigkeitsmessung.
**Harte Konsequenz für die Implementierung:** Der Streaming-Job
(SCRUM-77/-82) muss `status = 0` als Filterbedingung vor jeder
Aggregation anwenden — sonst verzerren Nullwerte in ~50 % der Records
Baseline und Congestion-Score massiv.

### Baseline-Dichte

```
Zellen je Segment = 168 (7 Wochentage × 24 Stunden)
Records/Jahr (hochgerechnet aus 24h-Wert) = 23.394 × 365 ≈ 8.538.810
Messungen je Zelle (roh) = 8.538.810 ÷ 125 link_id ÷ 168 ≈ 407
Messungen je Zelle (nach Sentinel-Filter, ~halbiert) ≈ 200
```

**Entscheidung:** Baseline wird nach dem feinen Raster **Wochentag ×
Stunde** (168 Zellen) berechnet — auch nach Abzug der Sentinel-Records
liegt die Dichte deutlich über der kritischen Schwelle, kein gröberes
Binning nötig. **Vorgabe an SCRUM-83:** Baseline-Schlüssel = `link_id` ×
Wochentag × Stunde, vorberechnet aus der Historie und als Broadcast
geladen (nicht im Streaming-State angelernt).

⚠️ Die Jahreshochrechnung nimmt konstante Sensoraktivität und
Meldefrequenz übers Jahr an (Full-Table-Scan nicht möglich, siehe oben).
Für den Prototyp vertretbar, im Bericht als Annahme zu kennzeichnen,
nicht als gemessenen Wert.

### Bulk-Export für Baseline-Historie

**Vorgehen:** Automatisiertes Pagination-Skript (`fetch_dot_baseline.py`,
unterbrechungssicher über Checkpoint-Chunks) gegen `$where`-Filter auf
`data_as_of` und `status='0'`, 19 Chunks à ≤50.000 Zeilen zusammengeführt.

**Zeitraum bewusst gewählt:** Ein erster Test mit dem Zeitraum
Juni 2026 ergab nur 101 von 125 aktuell aktiven Sensoren — zu weit
zurückliegend. Der gewählte Zeitraum 21.07.–31.08.2026 (letzte ~6
Wochen vor Sprint-Start) deckt 94 von 125 Sensoren ab und liegt damit
näher an der aktuellen Sensorlage.

**Ergebnis:** `dot_baseline_full.parquet`

| Kennzahl | Wert |
|---|---|
| Zeilen | 940.234 |
| Zeitraum | 21.07.2026 00:00 – 31.08.2026 23:58 (~42 Tage) |
| Distinkte `link_id` | 94 |
| Status | durchgängig `0` (Filter griff korrekt) |
| Duplikate (`link_id`+`data_as_of`) | 0 |
| `speed`-Wertebereich | 0,62 – 109,36 (plausibel) |
| `travel_time`-Wertebereich | 20 – 9.193 Sekunden (plausibel) |
| Records/Sensor | Median 11.567, Min 107 (ein Ausreißer), Max 11.953 |

**Geprüfte und noch offene Baseline-Lücke:** 94 Sensoren in der Historie
gegenüber 125 aktuell aktiven. Geprüft und ausgeschlossen: Export
unvollständig (0 Duplikate, volle Zeitspanne abgedeckt, nur 1 von 94
Sensoren beginnt spät im Fenster mit nur 107 Records — eher
unregelmäßiges Melden als Neuzugang). **Wahrscheinlichste Erklärung:**
Die zusätzlichen ~31 Sensoren sind erst nach dem 31.08.2026 aktiv
geworden und liegen außerhalb des exportierten Fensters — mit rein
historischen Bulk-Daten nicht weiter aufklärbar.

**Entscheidung zur Behandlung:** Segmente ohne ausreichende
Baseline-Historie laufen ohne Baseline-Vergleich und werden im
Congestion-Score explizit als „Historie unzureichend" markiert, bis
genug eigene Live-Daten vorliegen. Eine bewusste, im Bericht offen
benannte Scope-Grenze (Abschnitt 12), kein verstecktes Problem.

**Ablage:** aktuell lokal (`dot_baseline_full.parquet`); Umzug nach
MinIO, sobald SCRUM-78 steht — Pfad hier nachtragen.

---

## 2. Open-Meteo (Forecast + Archive)

| | |
|---|---|
| Endpunkt Live | `https://api.open-meteo.com/v1/forecast` |
| Endpunkt Historie | `https://archive-api.open-meteo.com/v1/archive` |
| Zugriff | Kein API-Key, kein Sign-up |
| Lizenz | CC BY 4.0 — Attribution **verpflichtend** (Quelle, Lizenzlink, Änderungshinweis) |
| Nutzungsbedingung | Kostenlos nur für nicht-kommerzielle Nutzung, max. 10.000 Aufrufe/Tag; Bildung und öffentlich finanzierte Forschung ausdrücklich als nicht-kommerziell genannt — Studienarbeit fällt darunter |
| Gewährleistung | Ausdrücklich ausgeschlossen („as is") |

### Kennzahlen

Testabruf über alle 5 Boroughs, identischer Zeitraum wie der
DOT-Bulk-Export (21.07.–31.08.2026), zusammengeführt in
`weather_baseline.parquet`:

| Kennzahl | Wert |
|---|---|
| Zeilen gesamt | 5.040 (5 Boroughs × 1.008 Stunden) |
| Zeitraum je Borough | 21.07.2026 00:00 – 31.08.2026 23:00, lückenlos |
| Variablen | `temperature_2m` (°C), `precipitation`/`rain` (mm), `snowfall` (cm), `wind_speed_10m` (km/h) |
| NaN / Duplikate | keine |
| Temperatur-Wertebereich | 16,6 – 35,9 °C (plausibel, NYC Hochsommer) |
| Niederschlag | bis 16 mm/h, kein Schnee im Zeitraum (erwartungsgemäß) |

`precipitation` und `rain` sind in diesem (schneefreien) Zeitraum
identisch — `precipitation` reicht als alleinige Variable für den
Enrichment-Join in SCRUM-84.

**Einschränkung:** Abgefragte Koordinaten werden vom Wettermodell auf den
nächstgelegenen Gitterpunkt gerundet (Modellauflösung 1–11 km). Bei
schmalen Bezirken wie Manhattan (angefragt −73.9712, geliefert −74.0199,
≈5 km Differenz) kann eine Gitterzelle Wetterdaten eines Nachbarbezirks
liefern. Für den Prototyp akzeptiert, als Einschränkung in Abschnitt 12
zu nennen.

**Poll-Intervall (Entscheidung):** 5 Minuten × 5 Boroughs = 1.440
Aufrufe/Tag — deutlich unter dem 10.000er-Limit, bewusst nah an der
DOT-Meldefrequenz (~7,7 Min.) gewählt, da eine feinere Wetterauflösung
keinen Mehrwert für den Join bringt. Wert geht als Vorgabe an SCRUM-81.

**Attributionstext** (Dashboard-Footer und README 1.3):
> Wetterdaten: [Open-Meteo](https://open-meteo.com/), lizenziert unter
> [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).

---

## 3. Eigener Producer (synthetischer Verkehrsdatenstrom)

| | |
|---|---|
| Rolle | Lastquelle für Skalierungsnachweis (SCRUM-93), Demo-Betrieb ohne Abhängigkeit von externer API-Verfügbarkeit |
| Verwendet | Echte `link_id`-Werte aus Quelle 1 → gilt als abgeleitetes Werk, Angabepflicht nach Local Law 11 gilt entsprechend |
| Lizenz | Keine externe — eigener Code |

---

## 4. Geprüft und bewusst nicht verwendet

| Quelle | Grund |
|---|---|
| TLC Trip Records | Schlüssel (Taxi-Zone statt Straßensegment) und Messgröße (Distanz/Dauer statt Geschwindigkeit) passen nicht zur Baseline-Anforderung aus Abschnitt 1.1 |
| MTA Bus Time | Busse ≠ Straßensegmente; Developer-Key nötig; Logos/Marken nicht mitlizenziert |
| 511NY Events | Ereignismeldungen, keine Geschwindigkeitsmessungen |

---

## Offene Anschlusspunkte für spätere Sprints

- [ ] An SCRUM-74: Avro-Schema mit zusammengesetztem Schlüssel
      `link_id` + `data_as_of` umsetzen
- [ ] An SCRUM-78: Entscheidung *single* vs. *distributed mode* für
      MinIO vorziehen (horizontale Skalierung nur im distributed mode)
- [ ] An SCRUM-81: Poll-Intervalle (Wetter 5 Min.), App-Token als
      Secret, Cache-Staleness-Verhalten berücksichtigen
- [ ] An SCRUM-83: Baseline-Schlüssel und Broadcast-Ladevorgang
      umsetzen; Sonderbehandlung für Segmente ohne Baseline-Historie
- [ ] An SCRUM-90: Open-Meteo-Attribution im Dashboard-Footer umsetzen
- [ ] Baseline-Parquet-Dateien nach MinIO verschieben, sobald SCRUM-78
      steht; Pfad hier nachtragen
- [ ] Cache-Staleness über mehrere Abrufe hinweg beobachten, um reale
      Update-Frequenz statt einmaligen Cache-Timestamp zu ermitteln