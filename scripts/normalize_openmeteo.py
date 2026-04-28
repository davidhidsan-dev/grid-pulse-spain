"""Normalize one or more raw Open-Meteo JSON files into a flat daily CSV."""

import argparse
import json
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.transform.openmeteo import load_json, normalize_daily_payload, resolve_input_files, save_csv


def main() -> None:
    """Read raw Open-Meteo files and save a flat daily CSV."""
    parser = argparse.ArgumentParser(description="Normalize raw Open-Meteo JSON files.")
    parser.add_argument("--regions", help="Comma-separated region slugs, for example: madrid,asturias")
    args = parser.parse_args()

    region_slugs = (
        [item.strip().lower() for item in args.regions.split(",") if item.strip()]
        if args.regions
        else None
    )
    input_files = resolve_input_files(region_slugs)
    rows: list[dict[str, Any]] = []

    for input_file in input_files:
        raw_json = load_json(input_file)
        file_rows = normalize_daily_payload(raw_json)
        rows.extend(file_rows)
        print(f"Input file: {input_file}")
        print(f"Normalized rows from file: {len(file_rows)}")

    output_file = save_csv(rows)

    print(f"Normalized rows: {len(rows)}")
    if rows:
        print("Example normalized row:")
        print(json.dumps(rows[0], indent=2, ensure_ascii=False))
    print(f"Saved normalized CSV to: {output_file}")


if __name__ == "__main__":
    main()
