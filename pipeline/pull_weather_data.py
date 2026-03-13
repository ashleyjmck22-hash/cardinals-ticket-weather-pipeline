# pipeline/pull_weather_data.py

import pandas as pd
import sqlite3
from pathlib import Path
import requests


def pull_weather_data() -> None:
    project_root = Path(__file__).resolve().parent.parent
    db_file = project_root / "cardinals_pipeline.db"

    # Busch Stadium / St. Louis
    latitude = 38.6270
    longitude = -90.1994

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "daily": "temperature_2m_max,precipitation_probability_max,weathercode",
        "timezone": "America/Chicago",
        "forecast_days": 16
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    weather_df = pd.DataFrame({
        "forecast_date": pd.to_datetime(data["daily"]["time"]),
        "forecast_temp": data["daily"]["temperature_2m_max"],
        "forecast_precip_prob": data["daily"]["precipitation_probability_max"],
        "weather_code": data["daily"]["weathercode"],
    })

    conn = sqlite3.connect(db_file)
    weather_df.to_sql("weather_forecast", conn, if_exists="replace", index=False)
    conn.close()

    print("Weather forecast successfully loaded into cardinals_pipeline.db")


if __name__ == "__main__":
    pull_weather_data()