"""Run a simple REData extraction and save raw JSON locally."""

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config.settings import RE_DATA_RAW_PATH
from src.extract.redata.client import REDataClient


def ensure_data_folder(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def main() -> None:
    client = REDataClient()
    output_folder = ensure_data_folder(RE_DATA_RAW_PATH)
    response = client.fetch_balance(
        start_date="2025-01-01T00:00",
        end_date="2025-01-07T23:59",
    )

    output_file = output_folder / "redata_sample.json"
    with output_file.open("w", encoding="utf-8") as handler:
        json.dump(response, handler, indent=2, ensure_ascii=False)

    print(f"Saved REData response to {output_file}")


if __name__ == "__main__":
    main()
