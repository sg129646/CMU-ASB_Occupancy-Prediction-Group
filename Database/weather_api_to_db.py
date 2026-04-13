import requests
import psycopg2
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
def fetch_and_store_weather():
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 40.4415995 ,
        "longitude": -79.9462885,
        "hourly": [
            "temperature_2m",
            "precipitation",
            "snowfall",
            "windspeed_10m",
            "weathercode"
        ],
        "timezone": "America/New_York",
        "forecast_days": 1
    }

    response = requests.get(url, params=params).json()
    hourly = response["hourly"]

    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    cursor = conn.cursor()

    for i, time_str in enumerate(hourly["time"]):
        timestamp = datetime.fromisoformat(time_str)
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

    conn.commit()
    conn.close()

if __name__ == "__main__":
    fetch_and_store_weather()
