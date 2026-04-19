import requests
import psycopg2
import os
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

load_dotenv()

TZ = ZoneInfo("America/New_York")

def fetch_and_store_weather():
    now = datetime.now(TZ)
    safe_now = now - timedelta(hours=1)
    end_date = safe_now.date()

    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    cursor = conn.cursor()

    cursor.execute("SELECT MAX(timestamp) FROM weather;")
    last_timestamp = cursor.fetchone()[0]

    if last_timestamp:
        start_date = (last_timestamp.astimezone(TZ) + timedelta(hours=1)).date()
    else:
        start_date = date(2026, 4, 13)

    print(f"Fetching from {start_date} to {end_date}")

    if start_date > end_date:
        print("No new data to fetch yet.")
        conn.close()
        return

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": 40.4415995,
        "longitude": -79.9462885,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "hourly": [
            "temperature_2m",
            "precipitation",
            "snowfall",
            "windspeed_10m",
            "weathercode"
        ],
        "timezone": "America/New_York"
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()

    hourly = data.get("hourly")
    if not hourly:
        print("No hourly data returned.")
        conn.close()
        return

    inserted = 0

    for i, time_str in enumerate(hourly["time"]):
        timestamp = datetime.fromisoformat(time_str).replace(tzinfo=TZ)

        if last_timestamp and timestamp <= last_timestamp.astimezone(TZ):
            continue

        cursor.execute("""
            INSERT INTO weather (timestamp, temperature, precipitation, snowfall, windspeed, condition)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (timestamp) DO NOTHING
        """, (
            timestamp,
            hourly["temperature_2m"][i],
            hourly["precipitation"][i],
            hourly["snowfall"][i],
            hourly["windspeed_10m"][i],
            hourly["weathercode"][i]
        ))

        if cursor.rowcount > 0:
            inserted += 1

    conn.commit()
    conn.close()

    print(f"Inserted {inserted} new rows")

if __name__ == "__main__":
    fetch_and_store_weather()
