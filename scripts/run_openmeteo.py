"""Run a simple Open-Meteo extraction for one or more regions."""

import argparse
import copy
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config.settings import OPENMETEO_RAW_PATH
from src.config.regions import load_regions, prompt_for_regions, resolve_regions_by_slugs
from src.extract.weather.client import OpenMeteoClient
from src.utils.logger import get_logger
from src.utils.terminal_ui import prompt_for_language, prompt_for_year_range, translate

DEFAULT_START_YEAR = 2015
DEFAULT_END_YEAR = 2025
DAILY_VARIABLES = [
    "temperature_2m_max",
    "temperature_2m_mean",
    "temperature_2m_min",
    "precipitation_sum",
    "weather_code",
    "wind_speed_10m_max",
    "shortwave_radiation_sum",
]
logger = get_logger(__name__)


def ensure_data_folder(path: Path) -> Path:
    """Create the output folder if it does not exist yet."""
    path.mkdir(parents=True, exist_ok=True)
    return path


def parse_args() -> argparse.Namespace:
    """Read optional CLI overrides for language, regions and year range."""
    parser = argparse.ArgumentParser(
        description="Run Open-Meteo extraction for one or more regions."
    )
    parser.add_argument("--language", choices=["es", "en"])
    parser.add_argument("--regions", help="Comma-separated region slugs, for example: madrid,andalucia")
    parser.add_argument("--start-year", type=int)
    parser.add_argument("--end-year", type=int)
    return parser.parse_args()


def merge_openmeteo_payloads(base_payload: dict, yearly_payload: dict) -> dict:
    """Merge one yearly Open-Meteo payload into the accumulated payload."""
    merged_payload = copy.deepcopy(base_payload)

    merged_daily = merged_payload.get("daily", {})
    yearly_daily = yearly_payload.get("daily", {})
    if not isinstance(merged_daily, dict) or not isinstance(yearly_daily, dict):
        raise ValueError("Expected Open-Meteo payloads to contain a 'daily' object.")

    for key, yearly_values in yearly_daily.items():
        if not isinstance(yearly_values, list):
            raise ValueError(
                f"Expected Open-Meteo daily series '{key}' to be a list, got {type(yearly_values).__name__}."
            )

        merged_values = merged_daily.get(key, [])
        if not isinstance(merged_values, list):
            raise ValueError(
                f"Expected accumulated Open-Meteo daily series '{key}' to be a list, "
                f"got {type(merged_values).__name__}."
            )
        merged_values.extend(copy.deepcopy(yearly_values))

    if "daily_units" not in merged_payload and "daily_units" in yearly_payload:
        merged_payload["daily_units"] = copy.deepcopy(yearly_payload["daily_units"])

    return merged_payload


def main() -> None:
    """Fetch daily historical samples for the selected regions and save raw JSON."""
    args = parse_args()
    language = args.language or prompt_for_language()
    client = OpenMeteoClient()
    output_folder = ensure_data_folder(OPENMETEO_RAW_PATH)
    regions = load_regions()

    if args.regions:
        region_slugs = [item.strip().lower() for item in args.regions.split(",") if item.strip()]
        selected_regions = resolve_regions_by_slugs(regions, region_slugs)
    else:
        selected_regions = prompt_for_regions(regions, language)

    if args.start_year is not None and args.end_year is not None:
        start_year = args.start_year
        end_year = args.end_year
        if start_year > end_year:
            raise ValueError(translate(language, "year_invalid_order"))
    elif args.start_year is None and args.end_year is None:
        start_year, end_year = prompt_for_year_range(
            language, DEFAULT_START_YEAR, DEFAULT_END_YEAR
        )
    else:
        raise ValueError("Use both --start-year and --end-year together.")

    start_date = f"{start_year}-01-01"
    end_date = f"{end_year}-12-31"
    logger.info(
        "Starting Open-Meteo extraction for %s region(s) between %s and %s",
        len(selected_regions),
        start_date,
        end_date,
    )

    for region in selected_regions:
        # We keep daily weather data, then aggregate it monthly to match REData.
        logger.info("Fetching Open-Meteo payload for region %s", region.region_slug)
        payload: dict | None = None
        requested_years: list[int] = []

        for year in range(start_year, end_year + 1):
            yearly_payload = client.fetch_daily_history(
                latitude=region.latitude,
                longitude=region.longitude,
                start_date=f"{year}-01-01",
                end_date=f"{year}-12-31",
                timezone=region.timezone,
                daily=DAILY_VARIABLES,
            )
            requested_years.append(year)

            if payload is None:
                payload = yearly_payload
            else:
                payload = merge_openmeteo_payloads(payload, yearly_payload)

        if payload is None:
            raise RuntimeError("No Open-Meteo payload was generated for the selected year range.")

        raw_response = {
            "source": client.SOURCE,
            "region_slug": region.region_slug,
            "region_name": region.display_name,
            "location_name": region.weather_location_name,
            "latitude": region.latitude,
            "longitude": region.longitude,
            "timezone": region.timezone,
            "weather_point_type": region.weather_point_type,
            "requested_start_date": start_date,
            "requested_end_date": end_date,
            "requested_years": requested_years,
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "payload": payload,
        }

        output_file = output_folder / f"openmeteo_{region.region_slug}_daily_sample.json"
        with output_file.open("w", encoding="utf-8") as handler:
            json.dump(raw_response, handler, indent=2, ensure_ascii=False)

        logger.info(
            "Saved Open-Meteo raw response for region %s to %s",
            region.region_slug,
            output_file,
        )
        print(translate(language, "saved_openmeteo", path=output_file))


if __name__ == "__main__":
    main()
