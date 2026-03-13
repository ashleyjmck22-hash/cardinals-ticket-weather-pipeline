# pipeline/ingest_ticket_data.py

import pandas as pd
import sqlite3
from pathlib import Path


def ingest_ticket_data() -> None:
    project_root = Path(__file__).resolve().parent.parent
    input_file = project_root / "data" / "raw" / "ticket_activity_clean.csv"
    db_file = project_root / "cardinals_pipeline.db"

    df = pd.read_csv(input_file)

    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"])
    df["game_date"] = pd.to_datetime(df["game_date"])

    df["days_before_game"] = (df["game_date"] - df["snapshot_date"]).dt.days

    conn = sqlite3.connect(db_file)
    df.to_sql("ticket_activity", conn, if_exists="replace", index=False)
    conn.close()

    print("Ticket activity successfully loaded into cardinals_pipeline.db")


if __name__ == "__main__":
    ingest_ticket_data()