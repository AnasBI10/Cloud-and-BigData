# Git-Workflow und Definition of Done

**NYC Congestion Watch** — Cloud und BigData Projekt  
Dokumentation des Entwicklungsprozesses nach agilen Prinzipien mit expliziter Anbindung an Datencharakteristik.

Stand: 02.09.2026 | Gültig ab Sprint 1

---

## 1. Git-Branching-Modell

Wir nutzen ein **Feature-Branch-Modell** mit expliziten Branch-Naming-Konventionen:

### 1.1 Branch-Struktur

```
main
├── (production-ready, tagged releases)
└─── dev
     ├── (integration branch für Sprint)
     ├── feature/SCRUM-XX-kurzbeschreibung
     ├── fix/SCRUM-XX-bugfix
     └── chore/infrastruktur-aufgabe
```

### 1.2 Branch-Naming-Konvention

**Format:** `<typ>/<SCRUM-XX>-<kurzbeschreibung>`

| Typ | Zweck | Beispiel |
|-----|-------|---------|
| `feature/` | Neue Funktionalität | `feature/SCRUM-77-spark-streaming` |
| `fix/` | Bugfix oder Datenprobleme | `fix/SCRUM-99-sentinel-filter` |
| `chore/` | Infra, Docs, keine Features | `chore/k8s-rbac-setup` |
| `data/` | Datenquellen-Änderungen | `data/SCRUM-81-weather-poller` |
| `test/` | Test-Erweiterungen | `test/SCRUM-88-veracity-validation` |

**Regel:** Branch-Name beschreibt **was** gemacht wird, nicht **wer** es macht.

### 1.3 Branch-Schutz für `main` und `dev`

**Beide Branches sind geschützt:**

- [ ] Erfordert Pull Request vor Merge
- [ ] Erfordert mindestens 1 Approval (Reviewer von außerhalb des Authors)
- [ ] Erfordert Status Checks erfolgreich (CI/CD — siehe Abschnitt 3)
- [ ] Erfordert aktuellen Branch Status (Rebase/Merge gegen Main erforderlich)
- [ ] Dismiss stale pull request approvals wenn neue Commits gepusht werden: **enabled**
- [ ] Löscht Head Branch nach Merge: **enabled**

---

## 2. Pull Request Workflow

### 2.1 PR erstellen

**Vor dem Erstellen:**
1. Branch von `dev` (nicht von `main`!) auschecken
2. Lokal gegen Entwickler-Dependencies testen
3. `git rebase dev` (nicht merge!) um Konflikte früh zu erkennen

**PR-Titel:** Sollte exakt dem Ticket-Titel entsprechen
```
[SCRUM-77] Spark Structured Streaming Job für Statusfilter
```

**PR-Beschreibung:** Muss folgende Punkte abdecken (Template in `.github/PULL_REQUEST_TEMPLATE.md`):

```markdown
## Linked Issue
Closes #<issue-number> (GitHub Issue, nicht SCRUM-Ticket)

## Was ändert sich?
- [kurze Beschreibung der Funktionalität]

## Wie wurde getestet?
- [ ] Unit Tests geschrieben / erweitert
- [ ] Lokal gegen Docker-Compose getestet
- [ ] Datenfluss geprüft (Input → Output Vergleich)

## Datencharakteristik beeinträchtigt?
- [ ] **Volume:** Ändert sich die Datenmenge? (Bytes/Record, Records/Tag)
- [ ] **Velocity:** Ändert sich die Verarbeitungslatenz? (Poll-Intervall, Cache-Staleness)
- [ ] **Variety:** Werden neue Formate oder Ströme hinzugefügt?
- [ ] **Veracity:** Ändert sich der Datenfilter oder die Validierung?
- [ ] **Value:** Ändert sich der Congestion-Score oder die Entscheidungslogik?

Falls ja → `DATA_SOURCES.md` und `README.md` Abschnitt 2 aktualisieren!

## Lizenz und Attribution
- [ ] Neue Abhängigkeiten: Lizenz geprüft (siehe `LICENSES.md`)
- [ ] Externe Datenquellen: Attribution in `DATA_SOURCES.md` hinzugefügt
- [ ] NYC Open Data oder CC BY 4.0: Angabe korrekt?

## Breaking Changes?
- [ ] Schema-Änderungen in Avro dokumentiert (SCRUM-74)
- [ ] Kafka-Topic-Struktur unverändert (oder Migration geplant)
```

### 2.2 PR Review

**Reviewers:** Mindestens 1 Person, die **nicht** der Author ist.

**Review-Kriterien:**
1. **Code Quality:** Keine Duplicates, Fehlerbehandlung, Logging
2. **Test Coverage:** Neue Features haben Tests (Unit + Integration wenn nötig)
3. **Datenfluss:** Input/Output plausibel, Filterung korrekt (besonders `status=0`!)
4. **Dokumentation:** README/DATA_SOURCES.md aktualisiert?
5. **Lizenz:** Alle Quellen attributiert?

**Approval:** "Approve" markiert, dass Reviewer die Qualität bestätigt.  
**Request Changes:** "Request Changes" bedeutet: Nicht mergeabel bis Author beantwortet.

### 2.3 Merge-Strategie

**Nur squash-merge erlaubt** (GitHub-Setting):

```bash
# GitHub UI macht das automatisch, aber für lokal:
git merge --squash feature/SCRUM-77-spark-streaming
git commit -m "[SCRUM-77] Spark Structured Streaming Job für Statusfilter"
```

**Grund:** Hält die `dev`-History sauber, eine Zeile pro Feature = eine Zeile pro SCRUM-Ticket.

**Nach Merge:**
- GitHub löscht automatisch den Feature-Branch (Branch-Schutz-Setting)
- Lokal: `git branch -D feature/SCRUM-77-spark-streaming`

---

## 3. Continuous Integration (GitHub Actions)

Jeder Push auf `dev` oder PR-Kandidat triggert automatische Checks.

### 3.1 CI-Pipeline (geplant für Sprint 1)

**Status:** Workflows noch nicht implementiert (SCRUM-77 blockiert diese)  
**Zielzustand nach Sprint 1:**

| Stage | Trigger | Dauer | Fehler → Merge blockiert? |
|-------|---------|-------|---------------------------|
| **Lint** | Push/PR | ~30s | ✅ Ja |
| **Unit Tests** | Push/PR | ~1m | ✅ Ja |
| **Build Docker** | Push/PR | ~2m | ✅ Ja |
| **Integration Test** | Push/PR | ~3m | ✅ Ja |
| **Data Validation** | Push/PR | ~1m | ✅ Ja |

**Gesamtdauer:** ~7 Minuten, akzeptabel für lokalere Entwicklung

### 3.2 Data Validation Check (V5-Mapping)

Dieser Check prüft die Datencharakteristik und blockiert wenn kritisch:

```yaml
# .github/workflows/data-validation.yml (zu implementieren)
name: Data Validation

on: [push, pull_request]

jobs:
  data-check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      # VOLUME: Baseline-Datensatz prüfen
      - name: Check baseline size
        run: |
          if [[ ! -f data/dot_baseline_full.parquet ]]; then
            echo "⚠️ Baseline-Datensatz fehlt (dot_baseline_full.parquet)"
            exit 1
          fi
          size=$(stat -c%s data/dot_baseline_full.parquet)
          echo "Baseline size: $size bytes (~940k Zeilen erwartet)"
      
      # VERACITY: Sentinel-Filter prüfen
      - name: Validate status filter
        run: python3 scripts/validate_status_filter.py
        # Prüft: Nur status=0 in Aggregation
      
      # VELOCITY: Cache-Staleness dokumentiert?
      - name: Check cache documentation
        run: grep -q "Cache-Staleness" DATA_SOURCES.md || exit 1
      
      # VARIETY: Schema-Kompatibilität
      - name: Validate Avro schema
        run: python3 scripts/validate_avro_schema.py
        # Prüft zusammengesetzter Key link_id + data_as_of
      
      # VALUE: Congestion-Score Logik korrekt?
      - name: Validate score calculation
        run: python3 scripts/validate_score_logic.py
```

---

## 4. Definition of Done (DoD)

Ein SCRUM-Ticket ist **nur dann "fertig"**, wenn alle diese Kriterien erfüllt sind.

### 4.1 Functional Completeness

| Kriterium | Check | V5-Link |
|-----------|-------|---------|
| ✅ Code implementiert | Feature läuft lokal | *alle* |
| ✅ Unit Tests geschrieben | Test:Code ≥ 50 % Coverage | *alle* |
| ✅ Integration getestet | Gegen Docker-Compose, vollständiger Datenfluss | VARIETY |
| ✅ Fehlerbehandlung | Exception-Cases abgedeckt | VERACITY |
| ✅ Logging hinzugefügt | Kritische Punkte geloggt (Start, Fehler, Durchsatz) | VELOCITY |

### 4.2 Data-aware Criteria (★ Spezifisch für dieses Projekt)

| Kriterium | Aktion | Begründung |
|-----------|--------|-----------|
| **VOLUME** | Baseline/Testdaten sind committed oder extern referenziert | Reproduzierbarkeit, ~940k Zeilen tracken |
| **VELOCITY** | Latenz-Messungen dokumentiert (wenn messbar) | Skalierungsargument für Abschnitt 2 |
| **VARIETY** | Schema-Changes → `DATA_SOURCES.md` Feldsatz updaten | Nachvollziehbarkeit von Datenformat-Änderungen |
| **VERACITY** | Filterregel/Validierung → explizit in Code kommentiert | 49 % Sentinel-Wert darf nicht vergessen werden |
| **VALUE** | Congestion-Score-Logik → mit Beispielwerten dokumentiert | Geschäftslogik in Testfall oder README |

### 4.3 Documentation

| Artefakt | Aktualisiert? | Wo? |
|----------|---------------|-----|
| Code-Kommentare | ✅ Ja (warum, nicht was) | `src/` |
| Unit Test | ✅ Ja (happy path + error cases) | `tests/` |
| README Abschnitt 2 | ✅ Falls V's betroffen | `README.md` |
| DATA_SOURCES.md | ✅ Falls Datenquellen/Schema betroffen | `DATA_SOURCES.md` |
| LICENSES.md | ✅ Nur falls neue Dependencies | `LICENSES.md` |
| Jira/SCRUM-Ticket | ✅ PR-Link hinzugefügt, Subtasks closed | Jira |

### 4.4 Quality Gates

| Check | Tool | Akzeptanzkriterium |
|-------|------|-------------------|
| **Linting** | `pylint` (Python) / `eslint` (JS) | Score ≥ 8.0 |
| **Test Coverage** | `pytest --cov` | ≥ 50 % neue Code-Zeilen |
| **Type Checking** | `mypy` | Keine untyped `Any` |
| **Security** | `bandit` | Keine kritischen Findings |
| **Schema Validation** | Custom (Avro/Parquet) | Kompatibilität mit Registry ✓ |

### 4.5 Review Approval

- [ ] **Mindestens 1 Approval** von Reviewer (nicht der Author)
- [ ] **Kein "Request Changes"** pending
- [ ] **Alle Conversations resolved** (Author beantwortet / Reviewer akzeptiert)
- [ ] **Keine GitHub Conflicts** (rebase erforderlich)

### 4.6 Production Readiness (für `main` Branch)

Zusätzliche Anforderungen für Releases von `dev` → `main`:

- [ ] Release-Notes geschrieben (`CHANGELOG.md`)
- [ ] Version erhöht (Semantic Versioning: `v0.1.0` → `v0.2.0`)
- [ ] Git Tag erstellt: `git tag -a v0.2.0 -m "Release 0.2.0"`
- [ ] Keine `[WIP]` PRs mergeged
- [ ] Alle Tests grün (keine flaky Tests!)
- [ ] Deployment-Manual aktualisiert (k8s-manifeste, env-Vars)

---

## 5. V5-Mapping: Wie DoD die Datencharakteristik sichert

Dieser Abschnitt verknüpft jedes DoD-Kriterium mit den V's:

### Volume-Sicherung

| DoD-Check | Was wird prüft | Warum |
|-----------|----------------|-------|
| Testdaten committed | 940k Zeilen Baseline reproduzierbar | Jeder Entwickler kann tests laufen lassen |
| Baseline-Pfad in CODE | `dot_baseline_full.parquet` existent | Keine "fehlende Dateien"-Fehler in CI |

**Aktion:** `.gitignore` erlaubt `data/*.parquet` falls < 100 MB, sonst Git LFS.

### Velocity-Sicherung

| DoD-Check | Was wird prüft | Warum |
|-----------|----------------|-------|
| Latenz-Messungen documented | Poll-Intervall, Cache-Staleness | Kein Regressionsrisk (z.B. Poll zu aggressiv) |
| Timing-Assertions in Tests | z.B. `assert latency < 5_000_ms` | Warnt wenn Änderung Velocity bricht |

**Beispiel:**
```python
def test_poll_interval_respected():
    start = time.time()
    messages = poller.fetch()  # sollte alle ~5 min. sein
    elapsed = time.time() - start
    assert elapsed < 5 * 60, f"Poll zu langsam: {elapsed}s"
```

### Variety-Sicherung

| DoD-Check | Was wird prüft | Warum |
|-----------|----------------|-------|
| Schema-Validierung in CI | Avro, Parquet, JSON kompatibel | Keine Inkompatibilität zwischen Layers |
| Feldsatz in DATA_SOURCES.md aktuell | link_id, status, data_as_of vorhanden | Datenfluss-Klarheit |

**Aktion:** Schema-Version in Avro erhöhen, Kompatibilität in Registry prüfen.

### Veracity-Sicherung

| DoD-Check | Was wird prüft | Warum |
|-----------|----------------|-------|
| Statusfilter im Code dokumentiert | `status=0` explizit als Filter | 49 % Sentinel-Werte dürfen nicht durchrutschen |
| Validierungs-Tests | z.B. `status=-101` wird gefiltert | Automatisch prüfen, nicht vergessen |

**Beispiel:**
```python
@pytest.mark.parametrize("status,expected_filtered", [
    (0, False),       # gültige Messung
    (-101, True),     # Sentinel → filtered
    (1, True),        # unbekannter Status → filtered
])
def test_status_filter(status, expected_filtered):
    record = {"status": status, "speed": 10}
    assert should_filter_record(record) == expected_filtered
```

### Value-Sicherung

| DoD-Check | Was wird prüft | Warum |
|-----------|----------------|-------|
| Congestion-Score Testfälle | z.B. Score bei Baseline=50, gemessen=30 | Richtige Ranking-Logik |
| Entscheidungslogik documented | "Top-5-Anomalien" vs. "Alle" | Nicht aus Versehen zu viele/wenige Meldungen |

**Aktion:** Jeder Score-Update hat Testfall mit Beispielzahlen.

---

## 6. Release Process

### 6.1 Von `dev` zu `main`

**Trigger:** Am Ende eines Sprints oder wenn kritischer Fix nötig.

```bash
# 1. Lokal: sicherstellen, dass dev aktuell ist
git checkout dev
git pull origin dev

# 2. Changelog schreiben
# (Beispiel in CHANGELOG.md)
# - [SCRUM-77] Spark Streaming für Statusfilter
# - [SCRUM-81] Wetter-Poller integriert
# - [FIX-99] Sentinel-Filter Bug

# 3. Version erhöhen
# pyproject.toml / setup.py / VERSION file
# v0.1.0 → v0.2.0

# 4. Tag erstellen (lokal)
git tag -a v0.2.0 -m "Release 0.2.0: Kappa-Architektur Sprint 1"

# 5. Push mit Tag
git push origin dev
git push origin v0.2.0

# 6. GitHub: Release erstellen (UI)
# - Tag wählen
- Changelog-Text einfügen
# - "Publish Release"
```

### 6.2 Release Notes Template

```markdown
## v0.2.0 — Kappa-Architektur Sprint 1

### Was ist neu?
- Spark Structured Streaming für Live-Datenfluss
- Wetter-Enrichment (Open-Meteo) integriert
- Baseline-Vergleich und Congestion-Score aktiv

### Data Changes
- **Volume:** Baseline jetzt 940k Zeilen (21.07–31.08.2026)
- **Veracity:** Status-Filter (`status=0`) angewendet, 49 % Sentinel-Werte gefiltert
- **Velocity:** Poll-Intervall 5 Min. (Wetter), ~7,7 Min. (Verkehr)

### Breaking Changes
- Keine

### Migrations erforderlich
- Keine (Kappa-Architektur, kein Batch-Layer)

### Lizenz
- Weiterhin CC BY 4.0 (Open-Meteo), Local Law 11 (NYC Open Data)
```

---

## 7. Troubleshooting

### Merge-Konflikt aufgelöst?

```bash
git pull origin dev  # holt aktuelle Version
git rebase dev       # rebase statt merge, um linear zu bleiben
# Konflikte beheben...
git add .
git rebase --continue
git push --force-with-lease  # force ist nötig nach rebase
```

### CI-Status Check fehlgeschlagen?

1. **Logs prüfen** (GitHub Actions → Workflow Logs)
2. **Lokal reproduzieren:** `python -m pytest tests/`
3. **Data Check:** `python scripts/validate_status_filter.py`
4. **Status-Werte prüfen:** `grep -E 'status.*-101' test_data.json | wc -l`

### Datencharakteristik-Check in CI fehlgeschlagen?

→ **Wahrscheinlich:** DATA_SOURCES.md nicht aktualisiert.

**Fix:**
```bash
# Prüfen, was sich geändert hat
git diff DATA_SOURCES.md

# Falls Feldsatz oder Kennzahlen: updaten!
# Volume-Änderung? → Tabelle oben in Abschnitt 2
# Veracity? → Status-Verteilung aktualisieren
# etc.
```

---

## 8. Checkliste für Ticket-Starters (Devs, die neue SCRUM-Tickets anfangen)

Vor dem ersten `git push`:

- [ ] `git checkout -b feature/SCRUM-XX-kurzbeschreibung dev` (von `dev`, nicht `main`)
- [ ] Code geschrieben + Unit Tests
- [ ] Lokal: `pytest tests/ --cov=src/`
- [ ] Lokal: `docker-compose up -d` (gegen vollständiges System testen)
- [ ] README/DATA_SOURCES.md aktualisiert falls Daten betroffen
- [ ] Git log sauber: `git log --oneline | head -5` zeigt verständliche Messages
- [ ] `git push origin feature/SCRUM-XX-kurzbeschreibung`
- [ ] PR auf GitHub erstellen, Template ausfüllen
- [ ] Link zu GitHub Issue + SCRUM-Ticket hinzufügen

---

## 9. Offene Punkte

- [ ] `.github/workflows/` Ordner erstellen (CI/CD Pipelines)
- [ ] `PULL_REQUEST_TEMPLATE.md` implementieren (GitHub Template)
- [ ] `LICENSES.md` erstellen (Abhängigkeiten dokumentieren)
- [ ] `CHANGELOG.md` initialisieren (v0.0.0 Placeholder)
- [ ] Branch-Schutz auf `main` und `dev` aktivieren (GitHub Settings)
- [ ] Git Hooks lokal: `.git/hooks/pre-push` (prüft vor Push)

---

**Fragen?** Siehe README Abschnitt 1–3 oder frag einen Reviewer.  
**Gültig ab:** Sprint 1 (01.09.2026)
