"""Reusable Open-Meteo normalization helpers."""

import json
from pathlib import Path
from typing import Any

import pandas as pd

from src.config.settings import DATA_ROOT

OPENMETEO_RAW_PATH = DATA_ROOT / "raw" / "openmeteo"
OPENMETEO_PROCESSED_PATH = DATA_ROOT / "processed" / "openmeteo"
OPENMETEO_DAILY_OUTPUT_FILE_NAME = "openmeteo_daily_normalized.csv"
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


def resolve_input_files(region_slugs: list[str] | None) -> list[Path]:
    """Resolve one or more raw Open-Meteo files."""
    input_folder = ensure_folder(OPENMETEO_RAW_PATH)
    if region_slugs:
        input_files = []
        for region_slug in region_slugs:
            input_file = input_folder / f"openmeteo_{region_slug}_daily_sample.json"
            if not input_file.exists():
                raise FileNotFoundError(f"JSON file not found: {input_file}")
            input_files.append(input_file)
        return input_files

    json_files = sorted(
        input_folder.glob("*.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not json_files:
        raise FileNotFoundError(f"No JSON files found in: {input_folder}")
    return json_files


def normalize_daily_payload(raw_json: dict[str, Any]) -> list[dict[str, Any]]:
    """Flatten the Open-Meteo daily payload into one row per day."""
    payload = raw_json.get("payload", {})
    if not isinstance(payload, dict):
        raise ValueError("Expected 'payload' to be a JSON object.")

    daily = payload.get("daily", {})
    if not isinstance(daily, dict):
        raise ValueError("Expected 'payload.daily' to be a JSON object.")

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
            "ingestion_timestamp": raw_json.get("extracted_at"),
            "region_slug": raw_json.get("region_slug"),
            "region_name": raw_json.get("region_name"),
            "location_name": raw_json.get("location_name"),
            "latitude": raw_json.get("latitude"),
            "longitude": raw_json.get("longitude"),
            "timezone": raw_json.get("timezone"),
            "weather_point_type": raw_json.get("weather_point_type"),
            "date": date_value,
        }

        for column in DAILY_COLUMNS:
            values = daily.get(column, [])
            if not isinstance(values, list):
                raise ValueError(f"Expected 'payload.daily.{column}' to be a list.")
            if len(values) != len(dates):
                raise ValueError(
                    f"Expected 'payload.daily.{column}' to have {len(dates)} values, "
                    f"but found {len(values)}."
                )
            row[column] = values[index]

        rows.append(row)

    _ = daily_units
    return rows


def save_csv(rows: list[dict[str, Any]]) -> Path:
    """Write the normalized daily rows to the processed Open-Meteo folder."""
    output_folder = ensure_folder(OPENMETEO_PROCESSED_PATH)
    output_file = output_folder / OPENMETEO_DAILY_OUTPUT_FILE_NAME
    dataframe = pd.DataFrame(rows)
    dataframe.to_csv(output_file, index=False)
    return output_file
