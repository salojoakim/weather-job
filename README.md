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
  - `export_weather.py` – rådata till CSV/JSON.
  - `export_aggregate.py` – **dagliga aggregat** (min/medel/max temp, nederbörd m.m.).
- Tester (`tests.py` eller `pytest`).
- Körscript för Task Scheduler: `run_weather_job.cmd`.

---

## Mappstruktur
weather-job/
├─ .env # dina hemligheter (ignoreras av Git)
├─ .env.example # mall (utan hemligheter)
├─ README.md
├─ .gitignore
├─ requirements.txt
├─ main.py # huvudjobb (hourly → SQLite)
├─ export_weather.py # exportera rådata (valfritt)
├─ export_aggregate.py # exportera DAGLIGA aggregat
├─ run_weather_job.cmd # körs av Task Scheduler
├─ tests.py # automatiska tester (eller tests/…)
├─ logs/ # loggar (ej versionskontroll)
└─ exports/ # genererade CSV/JSON (ej versionskontroll)
