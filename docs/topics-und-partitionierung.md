## Speicherkonzept: Kafka-Topics und Partitionierung

| Topic | Partitionen | Replikationsfaktor | Retention | Key |
|---|---|---|---|---|
| `traffic.speeds.raw` | 12 | 3 | 7 Tage | `link_id` |
| `weather.observations.raw` | 5 | 3 | 7 Tage | `borough` |
| `traffic.speeds.dlq` | 3 | 3 | 14 Tage | - |

**Begruendung traffic.speeds.raw (12 Partitionen):** Die Partitionsanzahl legt
die Obergrenze der Consumer-Parallelitaet fest - mehr gleichzeitig lesende
Consumer innerhalb einer Consumer-Group als Partitionen sind wirkungslos.
12 wurde gewaehlt, um fuer den Skalierungsnachweis (horizontales Hochskalieren
des Processing-Jobs) ausreichend Spielraum zu haben, ohne bei nur 125 aktiven
Sensoren unnoetig viele kleine, duenn befuellte Partitionen zu erzeugen.

**Begruendung weather.observations.raw (5 Partitionen):** Der Partitionsschluessel
ist borough - NYC hat exakt 5 Boroughs. Mehr als 5 Partitionen waeren technisch
moeglich, aber mindestens eine Partition bliebe garantiert leer, da der Key-Raum
auf 5 feste Werte begrenzt ist.

**Begruendung traffic.speeds.dlq (3 Partitionen, Dead-Letter-Queue):** Fuer
Records, die nicht gegen das Avro-Schema validieren. Geringes erwartetes
Volumen, daher niedrige Partitionsanzahl; laengere Retention (14 statt 7 Tage)
fuer nachtraegliche Fehleranalyse.

**Replikationsfaktor 3 (alle Topics):** Passend zum 3-Broker-Kafka-Cluster,
mit min.insync.replicas=2 - ein Broker-Ausfall fuehrt nicht zu Datenverlust
oder Schreibstopp.

**Partitionierungsschluessel:** link_id (Verkehrsmessungen) bzw. borough
(Wetter) statt zufaelliger Verteilung - das garantiert, dass alle Events
desselben Strassensegments bzw. Bezirks in derselben Partition und damit in
garantierter Reihenfolge ankommen. Fuer die Windowed Aggregation im
Processing-Job ist das Voraussetzung: ohne Ordnungsgarantie pro Segment
waeren zeitliche Aggregationen pro link_id nicht deterministisch
nachvollziehbar.
## Workload-Typ-Zuordnung
| Komponente | Workload-Typ | Begründung |
4
|---|---|---|
5
| Kafka | StatefulSet | Jeder Broker hält einen eigenen, nicht austauschbaren Datenbestand (Partitionsreplikate) und braucht stabile Identität (Broker-ID im KRaft-Quorum, advertised listener) |
6
| MinIO | StatefulSet | Objektdaten persistent, stabiler DNS-Name als S3-Endpoint für alle Consumer |
7
| Schema-Registry | Deployment | Zustandslos gegenüber Kubernetes — der eigentliche State liegt im Kafka-Topic `_schemas`, jede Replik liest denselben Log |
8
| Producer (synthetic) | Deployment | Zustandslos, beliebig horizontal skalierbar, daher mit HPA gekoppelt |
9
| Producer (live) | StatefulSet | Stabile Identität für zukünftige Erweiterung um Offset-Tracking je Instanz |
10
| Processing (Spark) | Deployment | Checkpoint liegt extern auf PVC, der Pod selbst ist austauschbar |
11
| Serving-API | Deployment | Zustandslos, liest nur aus MinIO/Kafka, mehrere Replicas hinter einem Service |

## Workload-Typ-Zuordnung

| Komponente | Workload-Typ | Begruendung |
|---|---|---|
| Kafka | StatefulSet | Jeder Broker haelt einen eigenen, nicht austauschbaren Datenbestand (Partitionsreplikate) und braucht stabile Identitaet (Broker-ID im KRaft-Quorum, advertised listener) |
| MinIO | StatefulSet | Objektdaten persistent, stabiler DNS-Name als S3-Endpoint fuer alle Consumer |
| Schema-Registry | Deployment | Zustandslos gegenueber Kubernetes - der eigentliche State liegt im Kafka-Topic _schemas, jede Replik liest denselben Log |
| Producer (synthetic) | Deployment | Zustandslos, beliebig horizontal skalierbar, daher mit HPA gekoppelt |
| Producer (live) | StatefulSet | Stabile Identitaet fuer zukuenftige Erweiterung um Offset-Tracking je Instanz |
| Processing (Spark) | Deployment | Checkpoint liegt extern auf PVC, der Pod selbst ist austauschbar |
| Serving-API | Deployment | Zustandslos, liest nur aus MinIO/Kafka, mehrere Replicas hinter einem Service |
## SCRUM-88 Schema-Evolution und Kompaktierung
Schema-Evolution wird durch Delta Lake mit
`mergeSchema=true` unterstützt.
Für die langfristige Pflege der Delta-Tabellen
wurden zwei Kubernetes CronJobs eingerichtet:
- delta-optimize
- delta-vacuum
Damit können kleine Dateien periodisch
kompaktiert und veraltete Versionen bereinigt
werden.



Speicherformat und Partitionierung im Data Lake (SCRUM-87)
Dateiformat

Alle Tabellen (Bronze, Silver, Gold, Baseline) werden als Delta Lake in MinIO geschrieben, physisch Parquet-Dateien plus Transaktionslog. Die Grundsatzentscheidung Delta vs. reines Parquet ist in "docs/technische-abweichungen" begründet. Für diese Pipeline zusätzlich relevant:

- Der Streaming-Sink committet alle 30 s. Durch ACID-Transaktionen liest die Serving-API (delta-rs) immer nur vollständig committete Stände, nie einen halb geschriebenen Micro-Batch.
- Die Gold-Tabelle wird per "MERGE" auf "(link_id, window_start)" aktualisiert. Das ist mit reinem Parquet nicht möglich.
- Parquet ist spaltenorientiert: Die Leser projizieren nur die benötigten Spalten ("SINK_COLUMNS" in "readers.py"), es werden nur diese Spalten gelesen.

Kompression: Snappy, der Spark/Delta-Standard (nicht explizit konfiguriert). Snappy ist sehr CPU-schonend, was bei einem Commit alle 30 s wichtiger ist als die maximale Kompressionsrate. zstd würde besser komprimieren, bei dem geringen absoluten Datenvolumen ist der Unterschied aber klein. Falls der Speicher auf dem MinIO-PVC knapp wird, ist zstd die naheliegende Alternative.

Partitionierung

| Tabelle  |             Pfad                    |        Partitionsspalte      |
| Bronze   | "s3a://bronze/traffic_speeds_raw"   |            keine             |
| Silver   | "s3a://bronze/traffic_speeds_valid" |            keine             |
| Gold     | "s3a://gold/congestion_scores"      | "window_date" ("yyyy-MM-dd") |
| Baseline | "s3a://gold/baseline_profile"       |            keine             |

Grundsatz: Partitionierung lohnt sich erst bei großen Tabellen (Databricks empfiehlt sie erst ab ca. 1 TB), bei kleinen Tabellen erzeugt sie vor allem zusätzliche kleine Dateien. Delta speichert außerdem Min/Max-Statistiken pro Datei, sodass Filter auch ohne Partitionen Dateien überspringen können (Data Skipping). Wir partitionieren daher nur dort, wo das Lesemuster es klar rechtfertigt.

Gold nach "window_date" (bewusste Ausnahme): Die Partitionsspalte folgt dem Lesezugriff. Die Serving-API fragt immer ein Zeitfenster ab ("window_start >= since") und filtert zusätzlich auf "window_date >= since". Dadurch liest delta-rs nur die Verzeichnisse der betroffenen Tage (Partition Pruning). Das gilt auch für die Zeitreihe eines einzelnen Segments, da diese ebenfalls zeitlich eingeschränkt wird. Gold ist die Tabelle, die das Dashboard bei jeder Aktualisierung liest und die mit der Laufzeit unbegrenzt wächst. Zusätzlich lassen sich alte Daten tageweise als ganze Partition entfernen.

Tagesgranularität ergibt ein Verzeichnis pro Tag mit bis zu ~180.000 Zeilen (125 Segmente × 1.440 Fensterstarts pro Tag bei 5-Minuten-Fenstern mit 1-Minuten-Schritt). Stundenpartitionen würden 24-mal mehr Verzeichnisse mit entsprechend weniger Daten erzeugen. "window_date" ist ein String im ISO-Format: Die lexikografische Sortierung entspricht der chronologischen, der Vergleich im Reader ist daher korrekt.

Warum nicht "link_id": Jeder 30-s-Micro-Batch enthält Daten vieler Segmente. Eine Partitionierung nach "link_id" würde jeden Commit auf bis zu 125 Verzeichnisse verteilen und die Zahl kleiner Dateien vervielfachen. Der Hauptzugriff ist zeitbasiert, nicht segmentbasiert.

Bronze, Silver und Baseline unpartitioniert:
- Silver wird nur von "compute_baseline.py" gelesen. Der Job aggregiert die gesamte Historie je "(link_id, Wochentag, Stunde)" ohne Filter, Partition Pruning hätte bei diesem Zugriffsmuster keinen Effekt.
- Bronze ist ein reines Append-Archiv der Rohdaten ohne Leser im Normalbetrieb.
- Die Baseline umfasst höchstens 125 × 7 × 24 = 21.000 Zeilen und wird komplett gelesen und überschrieben.
- Silver liegt als eigenes Präfix im Bucket "bronze". Die Schichten werden über den Pfad getrennt, nicht über die Partitionierung.

Bekannte Einschränkungen

- Die Partitionierung wird nur beim Anlegen der Gold-Tabelle gesetzt ("streaming_job_bsg.py", Zweig "not isDeltaTable"). Eine vorher angelegte Tabelle bleibt unpartitioniert und muss neu erstellt werden.
- Die "MERGE"-Bedingung enthält "window_date" nicht. Partition Pruning wirkt daher nur beim Lesen, nicht beim Schreiben.
- "compute_baseline.py" liest Silver vollständig, die Laufzeit wächst mit der Historie. Würde der Job auf ein festes Zeitfenster (z. B. die letzten Wochen) begrenzt, wäre eine Datumspartitionierung von Silver sinnvoll.
- Der 30-s-Trigger erzeugt viele kleine Dateien. Kompaktierung (OPTIMIZE/VACUUM) ist nicht Teil dieses Tickets.
