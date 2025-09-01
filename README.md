# Weather Job (Visual Crossing → SQLite)

Automatiserat Python-jobb som hämtar **timvis** väderdata från Visual Crossing för en plats (t.ex. Kungsbacka) och **upserter** till en SQL-tabell (SQLite).  
Robust felhantering, loggning, **schemaläggning i Windows Task Scheduler**, export till **CSV/JSON**, samt automatiska tester.

---

## Funktioner
- Hämtar timdata via Visual Crossing API.
- Skriver till **SQLite** (`weather.db`), ingen server krävs.
- **UPSERT** på `(location, timestamp_local)` → idempotent körning.
- Felhantering: retries för 429/5xx/timeout, tydliga loggar.
- Exportscript:
  - `export_aggregate.py` – **dagliga aggregat** (min/medel/max temp, nederbörd m.m.).
- Tester (`tests.py`).
- Körscript för Task Scheduler: `run_weather_job.cmd`.

---

## Mappstruktur

```text
weather-job/
├─ .env                  (lokalt; ignoreras av Git)
├─ .env.example          (mall utan hemligheter)
├─ .gitignore
├─ README.md
├─ requirements.txt
├─ main.py               (huvudjobb: hourly → SQLite)
├─ export_aggregate.py   (dagliga aggregat → CSV/JSON)
├─ run_weather_job.cmd   (körs av Task Scheduler)
├─ tests.py              (automatiska tester)
├─ logs/                 (lokalt; ignoreras av Git)
└─ exports/              (lokalt; ignoreras av Git)
