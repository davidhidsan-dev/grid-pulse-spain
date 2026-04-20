"""Run a simple Open-Meteo extraction for one or more regions."""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config.regions import load_regions, prompt_for_regions, resolve_regions_by_slugs
from src.extract.weather.client import OpenMeteoClient
from src.utils.terminal_ui import prompt_for_language, prompt_for_year_range, translate

OPENMETEO_RAW_PATH = PROJECT_ROOT / "data" / "raw" / "openmeteo"

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

    for region in selected_regions:
        # We keep daily weather data, then aggregate it monthly to match REData.
        payload = client.fetch_daily_history(
            latitude=region.latitude,
            longitude=region.longitude,
            start_date=start_date,
            end_date=end_date,
            timezone=region.timezone,
            daily=DAILY_VARIABLES,
        )

        raw_response = {
            "source": client.SOURCE,
            "region_slug": region.region_slug,
            "region_name": region.display_name,
            "location_name": region.weather_location_name,
            "latitude": region.latitude,
            "longitude": region.longitude,
            "timezone": region.timezone,
            "weather_point_type": region.weather_point_type,
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "payload": payload,
        }

        output_file = output_folder / f"openmeteo_{region.region_slug}_daily_sample.json"
        with output_file.open("w", encoding="utf-8") as handler:
            json.dump(raw_response, handler, indent=2, ensure_ascii=False)

        print(translate(language, "saved_openmeteo", path=output_file))


if __name__ == "__main__":
    main()
