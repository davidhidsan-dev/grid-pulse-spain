"""Normalize the raw Open-Meteo Madrid JSON into a flat daily CSV."""

import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

INPUT_FILE = PROJECT_ROOT / "data" / "raw" / "openmeteo" / "openmeteo_madrid_daily_sample.json"
OUTPUT_FOLDER = PROJECT_ROOT / "data" / "processed" / "openmeteo"
OUTPUT_FILE_NAME = "openmeteo_madrid_daily_normalized.csv"
DAILY_COLUMNS = [
    "temperature_2m_max",
    "temperature_2m_mean",
    "temperature_2m_min",
    "precipitation_sum",
    "weather_code",
    "wind_speed_10m_max",
    "shortwave_radiation_sum",
]


def ensure_folder(path: Path) -> Path:
    """Create the folder if it does not exist yet."""
    path.mkdir(parents=True, exist_ok=True)
    return path


def load_json(path: Path) -> dict[str, Any]:
    """Load the raw Open-Meteo JSON from disk."""
    with path.open("r", encoding="utf-8") as handler:
        payload = json.load(handler)

    if not isinstance(payload, dict):
        raise ValueError("Expected a JSON object at the top level.")
    return payload


def normalize_daily_payload(raw_json: dict[str, Any]) -> list[dict[str, Any]]:
    """Flatten the Open-Meteo daily payload into one row per day."""
    payload = raw_json.get("payload", {})
    if not isinstance(payload, dict):
        raise ValueError("Expected 'payload' to be a JSON object.")

    daily = payload.get("daily", {})
    if not isinstance(daily, dict):
        raise ValueError("Expected 'payload.daily' to be a JSON object.")

    # Units are not written to the CSV yet, but keeping them explicit helps readability.
    daily_units = payload.get("daily_units", {})
    if not isinstance(daily_units, dict):
        daily_units = {}

    dates = daily.get("time", [])
    if not isinstance(dates, list):
        raise ValueError("Expected 'payload.daily.time' to be a list.")

    rows: list[dict[str, Any]] = []
    for index, date_value in enumerate(dates):
        row = {
            "source": raw_json.get("source"),
            "location_name": raw_json.get("location_name"),
            "latitude": raw_json.get("latitude"),
            "longitude": raw_json.get("longitude"),
            "timezone": raw_json.get("timezone"),
            "date": date_value,
        }

        for column in DAILY_COLUMNS:
            values = daily.get(column, [])
            if isinstance(values, list) and index < len(values):
                row[column] = values[index]
            else:
                row[column] = None

        rows.append(row)

    _ = daily_units
    return rows


def save_csv(rows: list[dict[str, Any]]) -> Path:
    """Write the normalized daily rows to the processed Open-Meteo folder."""
    output_folder = ensure_folder(OUTPUT_FOLDER)
    output_file = output_folder / OUTPUT_FILE_NAME
    dataframe = pd.DataFrame(rows)
    dataframe.to_csv(output_file, index=False)
    return output_file


def main() -> None:
    """Read the raw Madrid sample and save a flat daily CSV."""
    raw_json = load_json(INPUT_FILE)
    rows = normalize_daily_payload(raw_json)
    output_file = save_csv(rows)

    print(f"Input file: {INPUT_FILE}")
    print(f"Normalized rows: {len(rows)}")
    if rows:
        print("Example normalized row:")
        print(json.dumps(rows[0], indent=2, ensure_ascii=False))
    print(f"Saved normalized CSV to: {output_file}")


if __name__ == "__main__":
    main()
