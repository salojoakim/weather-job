# main.py
# Schemaläggbart jobb som hämtar väderdata från Visual Crossing och uppdaterar en SQLite-tabell.
# Kör i Windows cmd:
#   (venv) python main.py
# Schemalägg i Task Scheduler med:
#   Program/script: C:\path\weather-job\venv\Scripts\python.exe
#   Add arguments:  C:\path\weather-job\main.py
#   Start in:       C:\path\weather-job

import os
import sys
import time
import random
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from requests.exceptions import HTTPError, Timeout, ConnectionError
from dotenv import load_dotenv

from sqlalchemy import create_engine, Column, String, Float, DateTime, func, text
from sqlalchemy.orm import DeclarativeBase, Session
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

# ---------- Init / Loggning ----------
BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    filename=LOG_DIR / "weather_job.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    encoding="utf-8",  # UTF-8 i loggfilen (Windows-vänligt)
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger().addHandler(console)

# Läs .env tydligt från projektmappen
load_dotenv(BASE_DIR / ".env")

API_KEY    = (os.getenv("VC_API_KEY") or "").strip()
LOCATION   = (os.getenv("VC_LOCATION") or "Kungsbacka").strip()
UNIT_GROUP = (os.getenv("VC_UNIT_GROUP") or "metric").strip()  # "metric" eller "us"
DB_URL     = (os.getenv("DATABASE_URL") or "sqlite:///weather.db").strip()

if not API_KEY or API_KEY.upper().startswith(("DIN_", "YOUR_")):
    raise RuntimeError("Ogiltig VC_API_KEY. Lägg in din riktiga Visual Crossing-nyckel i .env (VC_API_KEY=...).")

safe_key = API_KEY[:4] + "..." + API_KEY[-4:] if len(API_KEY) > 8 else "(kort)"
logging.info(f"Startar jobb – plats: {LOCATION}, enheter: {UNIT_GROUP}, VC_API_KEY: {safe_key}")

# ---------- DB Modell ----------
class Base(DeclarativeBase):
    pass

class WeatherHourly(Base):
    __tablename__ = "weather_hourly"
    # Primärnyckel (för idempotent UPSERT)
    location = Column(String(64), primary_key=True)
    timestamp_local = Column(DateTime, primary_key=True)  # Lokal tid (enligt VC:s timezone)
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

    # metadata
    source = Column(String(32), default="VisualCrossing")
    fetched_at = Column(DateTime, server_default=func.current_timestamp())

# ---------- Hjälpfunktioner ----------
RETRYABLE_STATUS = {429, 500, 502, 503, 504}

def fetch_with_retries(url: str, params: dict, max_attempts: int = 5) -> requests.Response:
    """HTTP GET med exponentiell backoff. Fail-fast på 401."""
    backoff = 1.0
    for attempt in range(1, max_attempts + 1):
        try:
            r = requests.get(url, params=params, timeout=30)
            if r.status_code == 401:
                logging.error("Unauthorized (401) – kontrollera VC_API_KEY.")
                logging.error("Svar (trunkerat): %s", r.text[:500])
                r.raise_for_status()
            if r.status_code in RETRYABLE_STATUS:
                ra = r.headers.get("Retry-After")
                if ra and ra.isdigit():
                    delay = float(ra)
                else:
                    delay = backoff + random.uniform(0, 0.5)
                logging.warning("HTTP %s – försöker igen om %.1fs (försök %d/%d)",
                                r.status_code, delay, attempt, max_attempts)
                time.sleep(delay)
                backoff = min(backoff * 2, 30.0)
                continue
            r.raise_for_status()
            return r
        except (Timeout, ConnectionError) as e:
            delay = backoff + random.uniform(0, 0.5)
            logging.warning("Nätverksfel: %s – retry om %.1fs (försök %d/%d)",
                            e, delay, attempt, max_attempts)
            time.sleep(delay)
            backoff = min(backoff * 2, 30.0)
        except HTTPError:
            # 4xx (utom 429) är normalt inte hjälpta av retry
            raise
    raise RuntimeError(f"Misslyckades efter {max_attempts} HTTP-försök")

def combine_date_time(date_str: str, time_str: str) -> datetime:
    """
    Visual Crossing: date_str = 'YYYY-MM-DD', time_str = 'H:MM:SS' eller 'HH:MM:SS'.
    Normalisera till HH.
    """
    hour = time_str.split(":")[0]
    if len(hour) == 1:
        time_str = "0" + time_str
    return datetime.fromisoformat(f"{date_str} {time_str}")

def fetch_hours(location: str, unit_group: str):
    """
    Hämtar timvärden för [igår, imorgon] – lagom fönster för regelbunden körning.
    """
    end = datetime.now(timezone.utc) + timedelta(days=1)
    start = datetime.now(timezone.utc) - timedelta(days=1)

    url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{location}/{start.date()}/{end.date()}"
    params = {
        "unitGroup": unit_group,
        "include": "hours,current",
        "key": API_KEY,
        "contentType": "json",
        # Minska payload (valfritt – ta bort om du vill ha allt)
        "elements": "datetime,temp,feelslike,humidity,precip,precipprob,windspeed,windgust,pressure,cloudcover,conditions,icon"
    }

    logging.info(f"Hämtar data från Visual Crossing för {location}...")
    r = fetch_with_retries(url, params)
    try:
        data = r.json()
    except Exception:
        logging.error("Kunde inte tolka JSON. Svar (trunkerat): %s", r.text[:500])
        raise

    tz = data.get("timezone")
    days = data.get("days", [])
    rows = []

    for d in days:
        d_date = d.get("datetime")  # 'YYYY-MM-DD'
        for h in d.get("hours", []):
            t_local = combine_date_time(d_date, h.get("datetime"))
            rows.append({
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
            })

    logging.info(f"Hämtade {len(rows)} timrader.")
    return rows

def upsert_sqlite(engine, rows, max_attempts: int = 5):
    """
    UPSERT för SQLite med retry på lås (database is locked).
    """
    if not rows:
        logging.info("Inga rader att spara.")
        return

    backoff = 0.5
    for attempt in range(1, max_attempts + 1):
        try:
            with Session(engine) as session:
                insert_stmt = sqlite_insert(WeatherHourly).values(rows)
                update_cols = {
                    col.name: insert_stmt.excluded[col.name]
                    for col in WeatherHourly.__table__.columns
                    if col.name not in ("location", "timestamp_local")
                }
                stmt = insert_stmt.on_conflict_do_update(
                    index_elements=[WeatherHourly.location, WeatherHourly.timestamp_local],
                    set_=update_cols
                )
                session.execute(stmt)
                session.commit()
            logging.info("UPSERT klart (%d rader).", len(rows))
            return
        except OperationalError as e:
            msg = str(e).lower()
            if "database is locked" in msg or "busy" in msg:
                delay = backoff + random.uniform(0, 0.25)
                logging.warning("SQLite låst – retry om %.2fs (försök %d/%d)",
                                delay, attempt, max_attempts)
                time.sleep(delay)
                backoff = min(backoff * 2, 5.0)
                continue
            logging.exception("DB OperationalError")
            raise
        except SQLAlchemyError:
            logging.exception("SQLAlchemy-fel vid UPSERT")
            raise

# ---------- Huvudflöde ----------
def main() -> int:
    try:
        engine = create_engine(
            DB_URL,
            pool_pre_ping=True,
            connect_args={"check_same_thread": False, "timeout": 30},
        )
        # Valfritt snabbtest: öppna connection för att få tidigt fel om DB_URL är fel
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        Base.metadata.create_all(engine)  # Skapa tabell om den saknas
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
