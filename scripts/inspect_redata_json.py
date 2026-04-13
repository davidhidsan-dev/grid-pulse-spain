"""Normalize the raw REData Madrid balance JSON into a flat monthly table."""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config.settings import RE_DATA_PROCESSED_PATH, RE_DATA_RAW_PATH
from src.extract.redata.client import REDataClient

OUTPUT_FILE_NAME = "redata_balance_electrico_madrid_monthly_normalized.csv"


def ensure_folder(path: Path) -> Path:
    """Create the folder if it does not exist yet."""
    path.mkdir(parents=True, exist_ok=True)
    return path


def resolve_input_file(file_name: str | None) -> Path:
    """Use the requested file or fall back to the most recent JSON file."""
    input_folder = ensure_folder(RE_DATA_RAW_PATH)

    if file_name:
        input_file = input_folder / file_name
        if not input_file.exists():
            raise FileNotFoundError(f"JSON file not found: {input_file}")
        return input_file

    json_files = sorted(input_folder.glob("*.json"), key=lambda path: path.stat().st_mtime, reverse=True)
    if not json_files:
        raise FileNotFoundError(f"No JSON files found in: {input_folder}")
    return json_files[0]


def load_json(path: Path) -> dict[str, Any]:
    """Load the JSON payload from disk."""
    with path.open("r", encoding="utf-8") as handler:
        payload = json.load(handler)

    if not isinstance(payload, dict):
        raise ValueError("Expected a JSON object at the top level.")
    return payload


def extract_metadata(raw_json: dict[str, Any]) -> tuple[dict[str, Any], str, str]:
    """Read extraction metadata from the saved raw file."""
    payload = raw_json.get("payload")
    if isinstance(payload, dict):
        return (
            payload,
            str(raw_json.get("source", REDataClient.SOURCE)),
            str(raw_json.get("endpoint", REDataClient.ENDPOINT)),
        )

    return raw_json, REDataClient.SOURCE, REDataClient.ENDPOINT


def extract_blocks(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Return the main included blocks from the REData response."""
    blocks = payload.get("included", [])
    if not isinstance(blocks, list):
        raise ValueError("Expected 'included' to be a list.")
    return [block for block in blocks if isinstance(block, dict)]


def normalize_blocks(
    blocks: list[dict[str, Any]],
    source: str,
    endpoint: str,
    ingestion_timestamp: str,
) -> list[dict[str, Any]]:
    """Explode content and values into a flat list of monthly observation rows."""
    rows: list[dict[str, Any]] = []

    for block in blocks:
        block_attributes = block.get("attributes", {})
        content = block_attributes.get("content", [])
        if not isinstance(content, list):
            continue

        for record in content:
            if not isinstance(record, dict):
                continue

            record_attributes = record.get("attributes", {})
            values = record_attributes.get("values", [])
            if not isinstance(values, list):
                continue

            for value_item in values:
                if not isinstance(value_item, dict):
                    continue

                rows.append(
                    {
                        "source": source,
                        "endpoint": endpoint,
                        "ingestion_timestamp": ingestion_timestamp,
                        "group_type": block.get("type"),
                        "group_id": block.get("id"),
                        "group_title": block_attributes.get("title"),
                        "metric_type": record.get("type"),
                        "metric_id": record.get("id"),
                        "metric_group_id": record.get("groupId"),
                        "metric_title": record_attributes.get("title"),
                        "metric_description": record_attributes.get("description"),
                        "is_composite": record_attributes.get("composite"),
                        "last_update": record_attributes.get("last-update"),
                        "total": record_attributes.get("total"),
                        "total_percentage": record_attributes.get("total-percentage"),
                        # Madrid regional balance is handled monthly because the
                        # practical REData regional series is not usable daily.
                        "year_month": str(value_item.get("datetime", ""))[:7],
                        "datetime": value_item.get("datetime"),
                        "value": value_item.get("value"),
                        "percentage": value_item.get("percentage"),
                    }
                )

    return rows


def save_normalized_csv(rows: list[dict[str, Any]]) -> Path:
    """Write the normalized rows to the processed REData folder."""
    output_folder = ensure_folder(RE_DATA_PROCESSED_PATH)
    output_file = output_folder / OUTPUT_FILE_NAME
    dataframe = pd.DataFrame(rows)
    dataframe.to_csv(output_file, index=False)
    return output_file


def main() -> None:
    parser = argparse.ArgumentParser(description="Normalize a raw monthly REData balance JSON file.")
    parser.add_argument("--file", help="JSON file name inside data/raw/redata/")
    args = parser.parse_args()

    input_file = resolve_input_file(args.file)
    raw_json = load_json(input_file)
    payload, source, endpoint = extract_metadata(raw_json)
    blocks = extract_blocks(payload)
    ingestion_timestamp = datetime.now(timezone.utc).isoformat()
    rows = normalize_blocks(blocks, source, endpoint, ingestion_timestamp)

    print(f"Input file: {input_file}")
    print(f"Top-level keys: {', '.join(raw_json.keys())}")
    print(f"Source: {source}")
    print(f"Endpoint: {endpoint}")
    print(f"Main blocks found: {len(blocks)}")
    print(f"Block names: {', '.join(str(block.get('type')) for block in blocks)}")
    print(f"Normalized rows: {len(rows)}")

    if rows:
        print("Example normalized row:")
        print(json.dumps(rows[0], indent=2, ensure_ascii=False))

    output_file = save_normalized_csv(rows)
    print(f"Saved normalized CSV to: {output_file}")


if __name__ == "__main__":
    main()
