# tests.py
# Enhetstester för main.py utan riktiga nätverksanrop eller extern DB.
# Kör:  (venv) python tests.py -v

import os
import unittest
from unittest.mock import patch
from datetime import timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

# Se till att main.py inte klagar på API-nyckel vid import
os.environ.setdefault("VC_API_KEY", "TESTKEY123456")
os.environ.setdefault("VC_LOCATION", "Kungsbacka")
os.environ.setdefault("VC_UNIT_GROUP", "metric")
# Vi använder inte main.DB_URL i testerna (förutom import), men sätter ändå något:
os.environ.setdefault("DATABASE_URL", "sqlite:///weather_test.db")

# Importera appen
import main as app


class FakeResponse:
    """Minimal requests.Response-lik klass för att mocka HTTP-svar."""
    def __init__(self, status_code=200, json_data=None, text_data="", headers=None):
        self.status_code = status_code
        self._json = json_data
        self.text = text_data
        self.headers = headers or {}

    def raise_for_status(self):
        if 400 <= self.status_code < 600:
            from requests import HTTPError
            raise HTTPError(f"{self.status_code} error")

    def json(self):
        if self._json is None:
            raise ValueError("No JSON set on FakeResponse")
        return self._json


class TestHelpers(unittest.TestCase):
    def test_combine_date_time_normalizes_hour(self):
        dt = app.combine_date_time("2025-08-27", "0:05:00")
        self.assertEqual(dt.isoformat(sep=" "), "2025-08-27 00:05:00")


class TestFetchWithRetries(unittest.TestCase):
    @patch("main.time.sleep", return_value=None)  # snabba tester (ingen verklig väntan)
    def test_retries_then_success(self, _sleep):
        seq = [
            FakeResponse(500, text_data="server error", headers={"Retry-After": "0"}),
            FakeResponse(200, json_data={"ok": True}),
        ]
        with patch("main.requests.get", side_effect=seq):
            r = app.fetch_with_retries("http://example.com", {})
            self.assertEqual(r.json()["ok"], True)

    def test_unauthorized_raises(self):
        with patch("main.requests.get", return_value=FakeResponse(401, text_data="unauthorized")):
            with self.assertRaises(Exception):
                app.fetch_with_retries("http://example.com", {})


class TestFetchHoursParsing(unittest.TestCase):
    def test_parses_days_and_hours(self):
        fake_json = {
            "timezone": "Europe/Stockholm",
            "days": [
                {"datetime": "2025-08-26",
                 "hours": [
                     {"datetime": "0:00:00", "temp": 10, "feelslike": 9, "humidity": 80,
                      "precip": 0, "precipprob": 0, "windspeed": 2, "windgust": 4,
                      "pressure": 1015, "cloudcover": 50, "conditions": "Clear", "icon": "clear-night"},
                     {"datetime": "1:00:00", "temp": 9.5, "feelslike": 8.5, "humidity": 82,
                      "precip": 0, "precipprob": 0, "windspeed": 3, "windgust": 5,
                      "pressure": 1016, "cloudcover": 40, "conditions": "Clear", "icon": "clear-night"},
                 ]},
            ]
        }
        with patch("main.fetch_with_retries", return_value=FakeResponse(200, json_data=fake_json)):
            rows = app.fetch_hours("Kungsbacka", "metric")
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["location"], "Kungsbacka")
            self.assertEqual(rows[0]["timezone_name"], "Europe/Stockholm")
            self.assertIn("temp", rows[0])
            self.assertIn("timestamp_local", rows[0])


class TestSQLiteUpsert(unittest.TestCase):
    def setUp(self):
        # SQLite in-memory med StaticPool så samma connection återanvänds
        self.engine = create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        app.Base.metadata.create_all(self.engine)

    def tearDown(self):
        self.engine.dispose()

    def test_insert_and_update(self):
        ts = app.combine_date_time("2025-08-27", "00:00:00")
        ts2 = ts + timedelta(seconds=1)  # jämför i intervall, undvik mikrosekunder-mismatch

        row1 = {
            "location": "Kungsbacka",
            "timestamp_local": ts,
            "timezone_name": "Europe/Stockholm",
            "temp": 10.0, "feelslike": 9.0, "humidity": 80.0,
            "precip": 0.0, "precipprob": 0.0, "windspeed": 2.0, "windgust": 4.0,
            "pressure": 1015.0, "cloudcover": 50.0, "conditions": "Clear", "icon": "clear-night",
            "source": "VisualCrossing",
        }
        app.upsert_sqlite(self.engine, [row1])

        with self.engine.connect() as conn:
            v1 = conn.execute(
                text("""
                    SELECT temp, feelslike FROM weather_hourly
                    WHERE location = :loc
                      AND timestamp_local >= :ts
                      AND timestamp_local < :ts2
                """),
                {"loc": "Kungsbacka", "ts": ts, "ts2": ts2},
            ).fetchone()
        self.assertIsNotNone(v1)
        self.assertAlmostEqual(v1[0], 10.0)

        # Uppdatera samma PK med ny temp
        row1b = dict(row1)
        row1b["temp"] = 12.5
        app.upsert_sqlite(self.engine, [row1b])

        with self.engine.connect() as conn:
            v2 = conn.execute(
                text("""
                    SELECT temp FROM weather_hourly
                    WHERE location = :loc
                      AND timestamp_local >= :ts
                      AND timestamp_local < :ts2
                """),
                {"loc": "Kungsbacka", "ts": ts, "ts2": ts2},
            ).fetchone()
        self.assertIsNotNone(v2)
        self.assertAlmostEqual(v2[0], 12.5)


if __name__ == "__main__":
    unittest.main()
