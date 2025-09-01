# export_aggregate.py
"""
Dagliga aggregat från weather_hourly -> CSV/JSON.

Exempel:
  (venv) python export_aggregate.py --days 30 --location Kungsbacka --out exports\daily_30d.csv
  (venv) python export_aggregate.py --from 2025-08-01 --to 2025-08-27 --format json
"""

import os
import csv
import json
import argparse
from pathlib import Path
from datetime import datetime, timedelta

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

DB_URL = os.getenv("DATABASE_URL", "sqlite:///weather.db")
DEFAULT_LOCATION = os.getenv("VC_LOCATION", "Kungsbacka")


def detect_day_expr(dialect_name: str) -> str:
    """Rätt SQL-uttryck för att få datum-del av timestamp beroende på databas."""
    name = (dialect_name or "").lower()
    if name in {"sqlite", "mysql", "postgresql"}:
        return "DATE(timestamp_local)"
    if name.startswith("mssql"):
        return "CONVERT(date, timestamp_local)"
    return "DATE(timestamp_local)"


def parse_date(s: str) -> datetime:
    """Stöd för 'YYYY-MM-DD' eller 'YYYY-MM-DD HH:MM'/'YYYY-MM-DDTHH:MM'."""
    s = s.strip().replace("T", " ")
    if " " not in s:
        s += " 00:00"
    return datetime.fromisoformat(s)


def main():
    p = argparse.ArgumentParser(description="Exportera DAGLIGA aggregat från weather_hourly.")
    rng = p.add_mutually_exclusive_group()
    rng.add_argument("--days", type=int, help="Senaste N dagar (inkl. idag).")
    p.add_argument("--from", dest="date_from", type=str, help="Startdatum, t.ex. 2025-08-01")
    p.add_argument("--to", dest="date_to", type=str, help="Slutdatum, t.ex. 2025-08-27")
    p.add_argument("--location", type=str, default=DEFAULT_LOCATION, help=f"Plats (default: {DEFAULT_LOCATION})")
    p.add_argument("--out", type=str, help="Sökväg/filnamn för export. Relativt projektroten om ej absolut.")
    p.add_argument("--format", choices=["csv", "json"], default="csv", help="Exportformat.")
    args = p.parse_args()

    # Tidsintervall
    if args.date_from or args.date_to:
        if not (args.date_from and args.date_to):
            p.error("Ange både --from och --to, eller använd --days.")
        dt_from = parse_date(args.date_from)
        dt_to = parse_date(args.date_to)
    else:
        days = args.days or 7
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        dt_from = today - timedelta(days=days - 1)
        dt_to = datetime.now()

    # Filnamn / sökväg (robust)
    default_dir = BASE_DIR / "exports"
    default_dir.mkdir(exist_ok=True)

    if args.out:
        out_path = Path(args.out)
        # Relativa vägar tolkas från projektroten
        if not out_path.is_absolute():
            out_path = BASE_DIR / out_path
    else:
        span = f"{dt_from:%Y%m%d}-{dt_to:%Y%m%d}"
        ext = "csv" if args.format == "csv" else "json"
        out_path = default_dir / f"daily_{args.location}_{span}.{ext}"

    # Skapa ev. underkataloger
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # DB
    connect_args = {"check_same_thread": False} if DB_URL.startswith("sqlite") else {}
    engine = create_engine(DB_URL, pool_pre_ping=True, connect_args=connect_args)
    day_expr = detect_day_expr(engine.dialect.name)

    where_loc = "AND location = :loc" if args.location else ""
    sql = f"""
        SELECT
            {day_expr} AS day,
            location,
            MIN(temp)                            AS temp_min,
            AVG(temp)                            AS temp_avg,
            MAX(temp)                            AS temp_max,
            SUM(COALESCE(precip, 0))             AS precip_sum,
            MAX(COALESCE(precipprob, 0))         AS precipprob_max,
            AVG(COALESCE(windspeed, 0))          AS windspeed_avg,
            MAX(COALESCE(windgust, 0))           AS windgust_max,
            AVG(COALESCE(humidity, 0))           AS humidity_avg,
            AVG(COALESCE(pressure, 0))           AS pressure_avg,
            AVG(COALESCE(cloudcover, 0))         AS cloudcover_avg,
            COUNT(*)                              AS hours_count
        FROM weather_hourly
        WHERE timestamp_local >= :dt_from
          AND timestamp_local <= :dt_to
          {where_loc}
        GROUP BY {day_expr}, location
        ORDER BY day, location
    """
    params = {"dt_from": dt_from, "dt_to": dt_to}
    if args.location:
        params["loc"] = args.location

    with engine.connect() as conn:
        rows = list(conn.execute(text(sql), params).mappings())

    if not rows:
        print("Inga rader att exportera för valt intervall/plats.")
        return 0

    # Skriv fil
    if args.format == "csv":
        # utf-8-sig (BOM) så Excel visar å/ä/ö korrekt
        with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
            fieldnames = list(rows[0].keys())
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for r in rows:
                r = dict(r)
                d = r["day"]
                if hasattr(d, "isoformat"):
                    r["day"] = d.isoformat()
                w.writerow(r)
    else:
        with open(out_path, "w", encoding="utf-8") as f:
            def _default(o):
                return o.isoformat() if hasattr(o, "isoformat") else str(o)
            json.dump(rows, f, ensure_ascii=False, indent=2, default=_default)

    print(f"Skrev {len(rows)} rader till: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
