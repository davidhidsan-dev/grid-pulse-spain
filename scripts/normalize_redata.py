"""Normalize one or more raw REData balance JSON files into a flat monthly table."""

import argparse
import json
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.transform.redata import (
    build_fallback_ingestion_timestamp,
    extract_blocks,
    extract_metadata,
    load_json,
    normalize_blocks,
    resolve_input_files,
    save_normalized_csv,
)
from src.utils.logger import get_logger

logger = get_logger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Normalize a raw monthly REData balance JSON file.")
    parser.add_argument("--file", help="JSON file name inside data/raw/redata/")
    parser.add_argument("--regions", help="Comma-separated region slugs, for example: madrid,asturias")
    args = parser.parse_args()

    region_slugs = (
        [item.strip().lower() for item in args.regions.split(",") if item.strip()]
        if args.regions
        else None
    )
    input_files = resolve_input_files(args.file, region_slugs)
    rows: list[dict[str, Any]] = []
    logger.info("Normalizing %s REData raw file(s)", len(input_files))

    for input_file in input_files:
        raw_json = load_json(input_file)
        fallback_ingestion_timestamp = build_fallback_ingestion_timestamp()
        payload, metadata = extract_metadata(raw_json, fallback_ingestion_timestamp)
        blocks = extract_blocks(payload)
        file_rows = normalize_blocks(blocks, metadata)
        rows.extend(file_rows)

        logger.info("Input file: %s", input_file)
        logger.info("Top-level keys: %s", ", ".join(raw_json.keys()))
        logger.info("Source: %s", metadata["source"])
        logger.info("Endpoint: %s", metadata["endpoint"])
        logger.info("Region: %s", metadata["region_slug"])
        logger.info("Main blocks found: %s", len(blocks))
        logger.info("Block names: %s", ", ".join(str(block.get("type")) for block in blocks))
        logger.info("Normalized rows from file: %s", len(file_rows))

    logger.info("Total normalized rows: %s", len(rows))

    if rows:
        logger.info("Example normalized row:\n%s", json.dumps(rows[0], indent=2, ensure_ascii=False))

    output_file = save_normalized_csv(rows)
    logger.info("Saved normalized CSV to: %s", output_file)


if __name__ == "__main__":
    main()
