# pipeline/transform_data.py

import pandas as pd
import sqlite3
from pathlib import Path


def map_weather_code(code):
    weather_map = {
        0: "Clear",
        1: "Mainly Clear",
        2: "Partly Cloudy",
        3: "Overcast",
        45: "Fog",
        48: "Fog",
        51: "Drizzle",
        61: "Rain",
        63: "Rain",
        65: "Heavy Rain",
        71: "Snow",
        73: "Snow",
        75: "Heavy Snow",
        80: "Rain Showers",
        95: "Thunderstorm"
    }
    return weather_map.get(code, "Other")


def transform_data():

    project_root = Path(__file__).resolve().parent.parent
    db_file = project_root / "cardinals_pipeline.db"

    conn = sqlite3.connect(db_file)

    ticket_df = pd.read_sql("SELECT * FROM ticket_activity", conn)
    weather_df = pd.read_sql("SELECT * FROM weather_forecast", conn)

    ticket_df["snapshot_date"] = pd.to_datetime(ticket_df["snapshot_date"])
    weather_df["forecast_date"] = pd.to_datetime(weather_df["forecast_date"])

    # FIX: join weather to snapshot date
    final_df = ticket_df.merge(
        weather_df,
        left_on="snapshot_date",
        right_on="forecast_date",
        how="left"
    )

    final_df["forecast_condition"] = final_df["weather_code"].apply(
        lambda x: map_weather_code(int(x)) if pd.notnull(x) else None
    )

    final_df["forecast_snapshot_date"] = final_df["snapshot_date"]

    final_columns = [
        "snapshot_date",
        "game_date",
        "days_before_game",
        "unique_tickets_sold",
        "unique_page_clicks",
        "forecast_temp",
        "forecast_precip_prob",
        "forecast_condition",
        "forecast_snapshot_date"
    ]

    final_df = final_df[final_columns]

    final_df.to_sql(
        "fact_ticket_interest_weather",
        conn,
        if_exists="replace",
        index=False
    )

    conn.close()

    print("Final table fact_ticket_interest_weather created successfully")


if __name__ == "__main__":
    transform_data()