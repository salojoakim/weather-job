"""
main.py
========
Schemaläggbart ETL-jobb som hämtar **timvis** väderdata från Visual Crossing och
**upserter** till en SQLite-tabell. Skriptet är robust för nätverksfel och låsta
SQLite-filer, samt loggar i UTF-8 för Windows.

Kör lokalt (Windows cmd):
    (venv) python main.py

Schemalägg i Task Scheduler:
    Program/script: C:\path\weather-job\venv\Scripts\python.exe
    Add arguments : C:\path\weather-job\main.py
    Start in      : C:\path\weather-job

Miljövariabler (.env):
    VC_API_KEY    – Visual Crossing API-nyckel (OBLIGATORISK)
    VC_LOCATION   – t.ex. "Kungsbacka" (default)
    VC_UNIT_GROUP – "metric" eller "us" (default: metric)
    DATABASE_URL  – t.ex. "sqlite:///weather.db" (default)

Exit-koder:
    0 – OK
    1 – Körningsfel (t.ex. nätverk/JSON/UPSERT)
    2 – Initieringsfel (t.ex. felaktig DB_URL eller saknad tabell)
"""

from __future__ import annotations

import os
import sys
import time
import random
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List

import requests
from requests.exceptions import HTTPError, Timeout, ConnectionError
from dotenv import load_dotenv

from sqlalchemy import create_engine, Column, String, Float, DateTime, func, text
from sqlalchemy.orm import DeclarativeBase, Session
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Engine

# ──────────────────────────────────────────────────────────────────────────────
# Init / Loggning
# ──────────────────────────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

# Fil-logg i UTF-8 (Windows visar å/ä/ö korrekt i t.ex. Notepad)
logging.basicConfig(
    filename=LOG_DIR / "weather_job.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    encoding="utf-8",
)
# Konsol-logg (bra vid manuell körning). För Task Scheduler hamnar allt i task.log.
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger().addHandler(console)

# Läs .env explicit från projektmappen (inte från aktuell arbetskatalog)
load_dotenv(BASE_DIR / ".env")

API_KEY: str = (os.getenv("VC_API_KEY") or "").strip()
LOCATION: str = (os.getenv("VC_LOCATION") or "Kungsbacka").strip()
UNIT_GROUP: str = (os.getenv("VC_UNIT_GROUP") or "metric").strip()  # "metric" eller "us"
DB_URL: str = (os.getenv("DATABASE_URL") or "sqlite:///weather.db").strip()

if not API_KEY or API_KEY.upper().startswith(("DIN_", "YOUR_")):
    # Gör felet explicit och lätt att förstå i schemalagd körning.
    raise RuntimeError(
        "Ogiltig VC_API_KEY. Lägg in din Visual Crossing-nyckel i .env (VC_API_KEY=...)."
    )

safe_key = API_KEY[:4] + "..." + API_KEY[-4:] if len(API_KEY) > 8 else "(kort)"
logging.info(
    "Startar jobb – plats: %s, enheter: %s, VC_API_KEY: %s", LOCATION, UNIT_GROUP, safe_key
)

# ──────────────────────────────────────────────────────────────────────────────
# DB-modell (SQLAlchemy ORM)
# ──────────────────────────────────────────────────────────────────────────────

class Base(DeclarativeBase):
    """Bas-klass för SQLAlchemy modeller."""
    pass


class WeatherHourly(Base):
    """
    Timvis väderdata.

    Primärnyckel:
        (location, timestamp_local)

    Vi sparar lokal tidsstämpel (enligt Visual Crossing), inte UTC,
    eftersom användare ofta vill titta per lokal dag/timme.
    """
    __tablename__ = "weather_hourly"

    # Primärnyckel (för idempotent UPSERT)
    location = Column(String(64), primary_key=True)
    timestamp_local = Column(DateTime, primary_key=True)
    timezone_name = Column(String(64))

    # Vanliga väderfält
    temp = Column(Float)
    feelslike = Column(Float)
    humidity = Column(Float)
    precip = Column(Float)
    precipprob = Column(Float)
    windspeed = Column(Float)
    windgust = Column(Float)
    pressure = Column(Float)
    cloudcover = Column(Float)
    conditions = Column(String(255))
    icon = Column(String(64))

    # Metadata
    source = Column(String(32), default="VisualCrossing")
    fetched_at = Column(DateTime, server_default=func.current_timestamp())

# ──────────────────────────────────────────────────────────────────────────────
# Hjälpfunktioner
# ──────────────────────────────────────────────────────────────────────────────

# HTTP-statuskoder som är rimliga att försöka om (transienta fel)
RETRYABLE_STATUS = {429, 500, 502, 503, 504}


def fetch_with_retries(url: str, params: Dict[str, str], max_attempts: int = 5) -> requests.Response:
    """
    Gör ett HTTP GET-anrop med exponentiell backoff och hanterar vanliga fel.

    - 401 → fail-fast (ingen idé att försöka igen utan rätt nyckel).
    - 429/5xx → retry med exponential backoff (tar hänsyn till "Retry-After" om det finns).
    - Timeout/ConnectionError → retry.

    Args:
        url:   API-endpoint.
        params: Query-parametrar som skickas med requests.get.
        max_attempts: Max antal försök innan vi ger upp.

    Returns:
        requests.Response: Lyckat svar (status 200–299) från servern.

    Raises:
        HTTPError / RuntimeError: När vi ska faila direkt eller efter max försök.
    """
    backoff = 1.0  # startfördröjning i sekunder
    for attempt in range(1, max_attempts + 1):
        try:
            r = requests.get(url, params=params, timeout=30)
            if r.status_code == 401:
                logging.error("Unauthorized (401) – kontrollera VC_API_KEY. Svar: %s", r.text[:500])
                r.raise_for_status()

            if r.status_code in RETRYABLE_STATUS:
                # Respektera Retry-After om servern skickar det
                ra = r.headers.get("Retry-After")
                delay = float(ra) if ra and ra.isdigit() else backoff + random.uniform(0, 0.5)
                logging.warning(
                    "HTTP %s – försöker igen om %.1fs (försök %d/%d)",
                    r.status_code, delay, attempt, max_attempts
                )
                time.sleep(delay)
                backoff = min(backoff * 2, 30.0)
                continue

            r.raise_for_status()  # kastar vid 4xx/5xx
            return r

        except (Timeout, ConnectionError) as e:
            # Nätverksproblem: försök igen med backoff
            delay = backoff + random.uniform(0, 0.5)
            logging.warning(
                "Nätverksfel: %s – retry om %.1fs (försök %d/%d)",
                e, delay, attempt, max_attempts
            )
            time.sleep(delay)
            backoff = min(backoff * 2, 30.0)

        except HTTPError:
            # 4xx (utom 429) hjälps sällan av retry → kasta direkt
            raise

    raise RuntimeError(f"Misslyckades efter {max_attempts} HTTP-försök")


def combine_date_time(date_str: str, time_str: str) -> datetime:
    """
    Visual Crossing ger timmar som 'H:MM:SS' (ibland ensiffrig timme).
    Denna funktion normaliserar till 'HH:MM:SS' och bygger en datetime.

    Exempel:
        combine_date_time("2025-08-27", "0:05:00")
        → datetime(2025, 8, 27, 0, 5, 0)
    """
    hour = time_str.split(":")[0]
    if len(hour) == 1:  # gör '0:05:00' → '00:05:00'
        time_str = "0" + time_str
    return datetime.fromisoformat(f"{date_str} {time_str}")


def fetch_hours(location: str, unit_group: str) -> List[Dict]:
    """
    Hämtar timrader för ett litet fönster ([igår, imorgon]).
    Det räcker för schemalagda körningar och minskar payload.

    Returns:
        Lista av dictar som matchar WeatherHourly-kolumner (utom fetched_at).
    """
    end = datetime.now(timezone.utc) + timedelta(days=1)
    start = datetime.now(timezone.utc) - timedelta(days=1)

    url = (
        "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/"
        f"timeline/{location}/{start.date()}/{end.date()}"
    )
    params = {
        "unitGroup": unit_group,
        "include": "hours,current",
        "key": API_KEY,
        "contentType": "json",
        # Mindre svar → snabbare, men lägg till/ta bort fält efter behov.
        "elements": (
            "datetime,temp,feelslike,humidity,precip,precipprob,"
            "windspeed,windgust,pressure,cloudcover,conditions,icon"
        ),
    }

    logging.info("Hämtar data från Visual Crossing för %s...", location)
    r = fetch_with_retries(url, params)

    try:
        data = r.json()
    except Exception:
        logging.error("Kunde inte tolka JSON. Svar (trunkerat): %s", r.text[:500])
        raise

    tz = data.get("timezone")
    days = data.get("days", [])
    rows: List[Dict] = []

    for d in days:
        d_date = d.get("datetime")  # 'YYYY-MM-DD'
        for h in d.get("hours", []):
            t_local = combine_date_time(d_date, h.get("datetime"))
            rows.append(
                {
                    "location": location,
                    "timestamp_local": t_local,
                    "timezone_name": tz,
                    "temp": h.get("temp"),
                    "feelslike": h.get("feelslike"),
                    "humidity": h.get("humidity"),
                    "precip": h.get("precip"),
                    "precipprob": h.get("precipprob"),
                    "windspeed": h.get("windspeed"),
                    "windgust": h.get("windgust"),
                    "pressure": h.get("pressure"),
                    "cloudcover": h.get("cloudcover"),
                    "conditions": h.get("conditions"),
                    "icon": h.get("icon"),
                    "source": "VisualCrossing",
                }
            )

    logging.info("Hämtade %d timrader.", len(rows))
    return rows


def upsert_sqlite(engine: Engine, rows: List[Dict], max_attempts: int = 5) -> None:
    """
    Idempotent UPSERT i SQLite med backoff om databasen är låst.

    Strategi:
        - INSERT ... ON CONFLICT(location, timestamp_local) DO UPDATE
        - Alla kolumner uppdateras utom primärnycklarna.

    Args:
        engine: SQLAlchemy Engine för mål-databasen.
        rows:   Lista med rader att skriva.
        max_attempts: Antal försök vid låsning.

    Raises:
        SQLAlchemyError: Vid permanenta DB-fel (loggas dessutom).
    """
    if not rows:
        logging.info("Inga rader att spara.")
        return

    backoff = 0.5
    for attempt in range(1, max_attempts + 1):
        try:
            with Session(engine) as session:
                insert_stmt = sqlite_insert(WeatherHourly).values(rows)

                # Bygg kolumn-dict för UPDATE-delen (obs: exkludera PK).
                update_cols = {
                    col.name: insert_stmt.excluded[col.name]
                    for col in WeatherHourly.__table__.columns
                    if col.name not in ("location", "timestamp_local")
                }

                stmt = insert_stmt.on_conflict_do_update(
                    index_elements=[WeatherHourly.location, WeatherHourly.timestamp_local],
                    set_=update_cols,
                )

                session.execute(stmt)
                session.commit()

            logging.info("UPSERT klart (%d rader).", len(rows))
            return

        except OperationalError as e:
            # Vanligt i SQLite om en annan process skriver: "database is locked".
            msg = str(e).lower()
            if "database is locked" in msg or "busy" in msg:
                delay = backoff + random.uniform(0, 0.25)
                logging.warning(
                    "SQLite låst – retry om %.2fs (försök %d/%d)", delay, attempt, max_attempts
                )
                time.sleep(delay)
                backoff = min(backoff * 2, 5.0)
                continue
            logging.exception("DB OperationalError")
            raise

        except SQLAlchemyError:
            logging.exception("SQLAlchemy-fel vid UPSERT")
            raise

# ──────────────────────────────────────────────────────────────────────────────
# Huvudflöde
# ──────────────────────────────────────────────────────────────────────────────

def main() -> int:
    """
    Startar DB-anslutning, hämtar data och skriver till tabellen.

    Returns:
        Exit-kod (0/1/2), se modulens docstring.
    """
    try:
        engine = create_engine(
            DB_URL,
            pool_pre_ping=True,  # testa anslutningen innan varje checkout
            connect_args={"check_same_thread": False, "timeout": 30},  # SQLite-friendly
        )

        # Snabb sanity check (ger tydligare init-fel vid t.ex. felaktig DB_URL).
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        # Skapa tabell första gången.
        Base.metadata.create_all(engine)

    except Exception as e:
        logging.exception("Misslyckades att initiera DB: %s", e)
        return 2

    try:
        rows = fetch_hours(LOCATION, UNIT_GROUP)
        upsert_sqlite(engine, rows)
        return 0

    except Exception as e:
        logging.exception("Körning misslyckades: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
