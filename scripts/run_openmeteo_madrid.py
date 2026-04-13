"""Run a simple Open-Meteo extraction for Madrid and save raw JSON locally."""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.extract.weather.client import OpenMeteoClient

OPENMETEO_RAW_PATH = PROJECT_ROOT / "data" / "raw" / "openmeteo"

LOCATION_NAME = "Madrid"
LATITUDE = 40.4168
LONGITUDE = -3.7038
TIMEZONE = "Europe/Madrid"
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


def main() -> None:
    """Fetch a small daily historical sample for Madrid and save the raw JSON."""
    client = OpenMeteoClient()
    output_folder = ensure_data_folder(OPENMETEO_RAW_PATH)

    # Keep the first run small and easy to inspect.
    payload = client.fetch_daily_history(
        latitude=LATITUDE,
        longitude=LONGITUDE,
        start_date="2025-01-01",
        end_date="2025-01-07",
        timezone=TIMEZONE,
        daily=DAILY_VARIABLES,
    )

    raw_response = {
        "source": client.SOURCE,
        "location_name": LOCATION_NAME,
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "timezone": TIMEZONE,
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }

    output_file = output_folder / "openmeteo_madrid_daily_sample.json"
    with output_file.open("w", encoding="utf-8") as handler:
        json.dump(raw_response, handler, indent=2, ensure_ascii=False)

    print(f"Saved Open-Meteo response to {output_file}")


if __name__ == "__main__":
    main()
