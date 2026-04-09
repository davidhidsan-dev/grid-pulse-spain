"""Run a simple REData extraction and save raw JSON locally."""

import json
from pathlib import Path

from src.config.settings import RE_DATA_RAW_PATH
from src.extract.redata.client import REDataClient


def ensure_data_folder(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def main() -> None:
    client = REDataClient()
    output_folder = ensure_data_folder(RE_DATA_RAW_PATH)
    response = client.fetch_prices(start_date="2024-01-01", end_date="2024-01-01")

    output_file = output_folder / "redata_sample.json"
    with output_file.open("w", encoding="utf-8") as handler:
        json.dump(response, handler, indent=2, ensure_ascii=False)

    print(f"Saved REData response to {output_file}")


if __name__ == "__main__":
    main()
