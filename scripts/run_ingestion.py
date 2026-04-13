"""Run a simple REData extraction and save raw JSON locally."""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config.settings import RE_DATA_RAW_PATH
from src.extract.redata.client import REDataClient

MADRID_AUTONOMOUS_COMMUNITY_ID = 13
MADRID_TIME_TRUNC = "month"
START_DATE = "2025-01-01T00:00"
END_DATE = "2025-03-31T23:59"


def ensure_data_folder(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def main() -> None:
    """Fetch the Madrid regional balance as a monthly series.

    The Madrid REData regional endpoint responds to daily requests, but in
    practice it only exposes a usable regional series at monthly granularity.
    """
    client = REDataClient()
    output_folder = ensure_data_folder(RE_DATA_RAW_PATH)
    payload = client.fetch_balance(
        start_date=START_DATE,
        end_date=END_DATE,
        autonomous_community_id=MADRID_AUTONOMOUS_COMMUNITY_ID,
        time_trunc=MADRID_TIME_TRUNC,
    )
    raw_response = {
        "source": client.SOURCE,
        "endpoint": client.ENDPOINT,
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }

    output_file = output_folder / "redata_madrid_monthly_sample.json"
    with output_file.open("w", encoding="utf-8") as handler:
        json.dump(raw_response, handler, indent=2, ensure_ascii=False)

    print(f"Saved REData response to {output_file}")


if __name__ == "__main__":
    main()
