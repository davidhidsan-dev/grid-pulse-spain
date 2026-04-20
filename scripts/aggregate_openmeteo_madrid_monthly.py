"""Aggregate the normalized Open-Meteo daily CSV into a monthly CSV."""

import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

INPUT_FILE = PROJECT_ROOT / "data" / "processed" / "openmeteo" / "openmeteo_daily_normalized.csv"
OUTPUT_FOLDER = PROJECT_ROOT / "data" / "processed" / "openmeteo"
OUTPUT_FILE_NAME = "openmeteo_monthly_normalized.csv"


def ensure_folder(path: Path) -> Path:
    """Create the folder if it does not exist yet."""
    path.mkdir(parents=True, exist_ok=True)
    return path


def aggregate_daily_to_monthly(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Aggregate the daily weather table to monthly grain by region."""
    dataframe = dataframe.copy()
    dataframe["date"] = pd.to_datetime(dataframe["date"])
    dataframe["year_month"] = dataframe["date"].dt.strftime("%Y-%m")

    monthly = (
        dataframe.groupby(
            [
                "source",
                "ingestion_timestamp",
                "region_slug",
                "region_name",
                "location_name",
                "latitude",
                "longitude",
                "timezone",
                "weather_point_type",
                "year_month",
            ],
            as_index=False,
        )
        .agg(
            temperature_2m_max_avg=("temperature_2m_max", "mean"),
            temperature_2m_mean_avg=("temperature_2m_mean", "mean"),
            temperature_2m_min_avg=("temperature_2m_min", "mean"),
            precipitation_sum_total=("precipitation_sum", "sum"),
            wind_speed_10m_max_avg=("wind_speed_10m_max", "mean"),
            shortwave_radiation_sum_total=("shortwave_radiation_sum", "sum"),
        )
    )

    monthly = monthly[
        [
            "source",
            "ingestion_timestamp",
            "region_slug",
            "region_name",
            "location_name",
            "latitude",
            "longitude",
            "timezone",
            "weather_point_type",
            "year_month",
            "temperature_2m_max_avg",
            "temperature_2m_mean_avg",
            "temperature_2m_min_avg",
            "precipitation_sum_total",
            "wind_speed_10m_max_avg",
            "shortwave_radiation_sum_total",
        ]
    ]

    return monthly


def main() -> None:
    """Read the daily Open-Meteo CSV and save a monthly aggregated CSV."""
    dataframe = pd.read_csv(INPUT_FILE)
    monthly = aggregate_daily_to_monthly(dataframe)

    output_folder = ensure_folder(OUTPUT_FOLDER)
    output_file = output_folder / OUTPUT_FILE_NAME
    monthly.to_csv(output_file, index=False)

    print(f"Input file: {INPUT_FILE}")
    print(f"Monthly rows: {len(monthly)}")
    print(f"Saved monthly CSV to: {output_file}")


if __name__ == "__main__":
    main()
