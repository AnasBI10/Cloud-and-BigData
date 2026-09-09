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
