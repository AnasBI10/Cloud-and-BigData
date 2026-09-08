# Topics, Partitionierung und Key-Strategie

Begründungsteil zu SCRUM-76. Geht später in Kapitel 4 der README ein.

## Topic-Übersicht

| Topic | Key | Partitionen | RF / minISR | Retention |
|---|---|---|---|---|
| `traffic.speeds.raw` | `link_id` | 12 | 3 / 2 | 7 Tage |
| `weather.observations.raw` | `borough` | 5 | 3 / 2 | 7 Tage |
| `traffic.speeds.dlq` | `link_id` | 3 | 3 / 2 | 14 Tage |

## Warum `link_id` als Key

Kafka garantiert Reihenfolge nur innerhalb einer Partition. Der Key bestimmt die
Partition, also bestimmt der Key, was in Reihenfolge bleibt.

Die gesamte Processing-Logik ist pro Segment definiert: das Zeitfenster in
SCRUM-82 aggregiert je Segment, die Zustandsverwaltung in SCRUM-83 hält je
Segment einen Baseline-Vergleich, der Congestion-Score ist ein Wert je Segment
und Fenster. Es gibt keine Berechnung, die zwei Segmente in einer festen
Reihenfolge zueinander braucht.

`link_id` als Key heißt deshalb: alles, was Reihenfolge braucht, bekommt sie —
und nichts darüber hinaus wird unnötig serialisiert.

Nicht gewählt:

- **Kein Key (round robin).** Messungen eines Segments landen dann verstreut über
  alle Partitionen. Der Zustandsoperator müsste über Partitionsgrenzen hinweg
  zusammenführen, und eine verspätet eintreffende Messung könnte eine frühere
  überholen.
- **`borough` als Key.** Nur 5 verschiedene Werte, damit maximal 5 nutzbare
  Partitionen — und Manhattan hätte ein Vielfaches der Last von Staten Island.
  Der Skalierungsnachweis in SCRUM-93 wäre damit bei 5 Consumern am Ende.
- **`link_id` + `data_as_of` als Key.** Wäre nahezu eindeutig und würde perfekt
  gleichverteilen, zerstört aber genau die Eigenschaft, für die wir den Key
  brauchen: zwei Messungen desselben Segments lägen in verschiedenen Partitionen.
  Die Kombination ist unser Dedup-Schlüssel im Lake (Feld `event_key`), nicht
  unser Partitionsschlüssel.

## Warum 12 Partitionen

Die Partitionszahl ist die harte Obergrenze der Consumer-Parallelität: eine
Partition wird immer von höchstens einem Consumer einer Gruppe gelesen. Mehr
Spark-Executors als Partitionen bringen nichts, die überzähligen laufen leer.

Die Zahl ist damit eine Aussage über die geplante Skalierung, nicht über die
aktuelle Datenmenge — und Partitionen lassen sich nachträglich nur erhöhen, nie
verringern.

- Aktuell 125 aktive Sensoren, ~23.400 Records/Tag, rund 14,6 MB/Tag roh. Das
  packt eine einzelne Partition ohne Schwierigkeiten. Der Durchsatz ist hier
  nicht das Argument.
- 12 Partitionen erlauben, den Streaming-Job von 1 auf 12 Instanzen zu skalieren
  und den Effekt in SCRUM-93 auch zu zeigen. Bei 3 Partitionen wäre der
  Nachweis nach zwei Verdopplungen zu Ende.
- 125 Keys auf 12 Partitionen ergeben im Schnitt rund 10 Segmente je Partition.
  Genug, dass sich einzelne stark meldende Segmente statistisch ausgleichen; die
  Verteilung ist damit nicht perfekt, aber unkritisch.
- Bei 3 Brokern ist 12 durch 3 teilbar, die Leader verteilen sich also gleichmäßig.

Der Lastgenerator aus SCRUM-75 nutzt echte `link_id`-Werte. Die Partitions-
verteilung unter Last entspricht deshalb der des Live-Feeds und nicht der eines
synthetischen Gleichverteilungs-Ideals.

## Warum 5 Partitionen beim Wetter

Wetter wird je Borough abgefragt, es gibt genau 5. Mehr Partitionen als Keys
wären garantiert leer und würden nur den Cluster-State aufblähen. Der Strom ist
mit 5-Minuten-Takt ohnehin klein; im Join (SCRUM-84) ist er die statische Seite.

## Warum eine DLQ

Records, die gegen das Avro-Schema fallen, dürfen weder den Stream anhalten noch
still verschwinden. Sie landen in `traffic.speeds.dlq` mit längerer Retention,
damit sich ein Schema-Bruch im Nachhinein nachvollziehen lässt — das ist auch
das ehrliche Gegenstück zum Schema-Evolution-Argument in Kapitel 2.

## Replikationsfaktor 3, minISR 2

Ein Broker darf ausfallen, ohne dass Schreiben blockiert. Bei RF=3/minISR=2
bleiben nach einem Ausfall zwei In-Sync-Replikate, der Producer bekommt weiter
`acks=all` bestätigt. Zusätzlich Voraussetzung für die transaktionale
Exactly-once-Semantik aus SCRUM-95, die einen replizierten
`transaction_state`-Log braucht.
