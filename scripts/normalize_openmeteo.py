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
from src.utils.logger import get_logger

logger = get_logger(__name__)


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
    logger.info("Normalizing %s Open-Meteo raw file(s)", len(input_files))

    for input_file in input_files:
        raw_json = load_json(input_file)
        file_rows = normalize_daily_payload(raw_json)
        rows.extend(file_rows)
        logger.info("Input file: %s", input_file)
        logger.info("Normalized rows from file: %s", len(file_rows))

    output_file = save_csv(rows)

    logger.info("Normalized rows: %s", len(rows))
    if rows:
        logger.info("Example normalized row:\n%s", json.dumps(rows[0], indent=2, ensure_ascii=False))
    logger.info("Saved normalized CSV to: %s", output_file)


if __name__ == "__main__":
    main()
