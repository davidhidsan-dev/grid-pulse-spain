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

    for input_file in input_files:
        raw_json = load_json(input_file)
        fallback_ingestion_timestamp = build_fallback_ingestion_timestamp()
        payload, metadata = extract_metadata(raw_json, fallback_ingestion_timestamp)
        blocks = extract_blocks(payload)
        file_rows = normalize_blocks(blocks, metadata)
        rows.extend(file_rows)

        print(f"Input file: {input_file}")
        print(f"Top-level keys: {', '.join(raw_json.keys())}")
        print(f"Source: {metadata['source']}")
        print(f"Endpoint: {metadata['endpoint']}")
        print(f"Region: {metadata['region_slug']}")
        print(f"Main blocks found: {len(blocks)}")
        print(f"Block names: {', '.join(str(block.get('type')) for block in blocks)}")
        print(f"Normalized rows from file: {len(file_rows)}")

    print(f"Total normalized rows: {len(rows)}")

    if rows:
        print("Example normalized row:")
        print(json.dumps(rows[0], indent=2, ensure_ascii=False))

    output_file = save_normalized_csv(rows)
    print(f"Saved normalized CSV to: {output_file}")


if __name__ == "__main__":
    main()
