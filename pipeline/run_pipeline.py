# pipeline/run_pipeline.py

from ingest_ticket_data import ingest_ticket_data
from pull_weather_data import pull_weather_data
from transform_data import transform_data


def run_pipeline() -> None:
    print("Starting pipeline...")
    ingest_ticket_data()
    pull_weather_data()
    transform_data()
    print("Pipeline completed successfully.")


if __name__ == "__main__":
    run_pipeline()