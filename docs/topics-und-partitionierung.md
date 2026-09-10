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
