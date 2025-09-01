Weather Job (Visual Crossing → SQLite)

Automatiserat Python-jobb som hämtar timvis väderdata från Visual Crossing för en plats (t.ex. Kungsbacka) och upserter till en SQL-tabell (SQLite som standard).
Inkluderar robust felhantering, loggning, schemaläggning i Windows Task Scheduler samt script för CSV/JSON-export och dagliga aggregat.
Automatiska tester (unittest/pytest) ingår.

Innehåll

Funktioner

Mappstruktur

Förkrav

Installation & setup (Windows cmd)

Miljövariabler (.env)

Köra manuellt

Schemaläggning (Task Scheduler)

Exportera data (CSV/JSON)

Tester

Loggar & felsökning

Kodstandard

Säkerhet

Licens

Funktioner

Hämtar väder per timme via Visual Crossing API.

Skriver till SQLite (en fil weather.db, ingen server behövs).

UPSERT (idempotent) på (location, timestamp_local).

Robust felhantering: retries på nätverksfel/429/5xx, tydliga loggar.

Schemaläggning för automatisk körning (Windows Task Scheduler).

Exportscript:

export_weather.py – rådata till CSV/JSON.

export_aggregate.py – dagliga aggregat (min/medel/max temp, nederbörd m.m.) till CSV/JSON.

Automatisk testning (tests.py + valfri pytest).
