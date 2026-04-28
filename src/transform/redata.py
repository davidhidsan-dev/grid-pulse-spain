"""Reusable REData normalization helpers."""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from src.config.settings import RE_DATA_PROCESSED_PATH, RE_DATA_RAW_PATH
from src.extract.redata.client import REDataClient

REDATA_OUTPUT_FILE_NAME = "redata_balance_electrico_monthly_normalized.csv"


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

    json_files = sorted(
        input_folder.glob("*.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not json_files:
        raise FileNotFoundError(f"No JSON files found in: {input_folder}")
    return json_files[0]


def resolve_input_files(file_name: str | None, region_slugs: list[str] | None) -> list[Path]:
    """Resolve one or more input files from a file name or region list."""
    if file_name:
        return [resolve_input_file(file_name)]

    input_folder = ensure_folder(RE_DATA_RAW_PATH)
    if region_slugs:
        input_files = []
        for region_slug in region_slugs:
            input_file = input_folder / f"redata_{region_slug}_monthly_sample.json"
            if not input_file.exists():
                raise FileNotFoundError(f"JSON file not found: {input_file}")
            input_files.append(input_file)
        return input_files

    json_files = sorted(
        input_folder.glob("*.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not json_files:
        raise FileNotFoundError(f"No JSON files found in: {input_folder}")
    return json_files


def load_json(path: Path) -> dict[str, Any]:
    """Load the JSON payload from disk."""
    with path.open("r", encoding="utf-8") as handler:
        payload = json.load(handler)

    if not isinstance(payload, dict):
        raise ValueError("Expected a JSON object at the top level.")
    return payload


def extract_metadata(
    raw_json: dict[str, Any], default_ingestion_timestamp: str
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Read extraction metadata from the saved raw file."""
    payload = raw_json.get("payload")
    if isinstance(payload, dict):
        metadata = {
            "source": str(raw_json.get("source", REDataClient.SOURCE)),
            "endpoint": str(raw_json.get("endpoint", REDataClient.ENDPOINT)),
            "region_slug": str(raw_json.get("region_slug", "")),
            "region_name": str(raw_json.get("region_name", "")),
            "redata_geo_id": raw_json.get("redata_geo_id"),
            "ingestion_timestamp": str(
                raw_json.get("extracted_at", default_ingestion_timestamp)
            ),
        }
        return payload, metadata

    metadata = {
        "source": REDataClient.SOURCE,
        "endpoint": REDataClient.ENDPOINT,
        "region_slug": "",
        "region_name": "",
        "redata_geo_id": None,
        "ingestion_timestamp": default_ingestion_timestamp,
    }
    return raw_json, metadata


def extract_blocks(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Return the main included blocks from the REData response."""
    blocks = payload.get("included", [])
    if not isinstance(blocks, list):
        raise ValueError("Expected 'included' to be a list.")
    return [block for block in blocks if isinstance(block, dict)]


def normalize_blocks(
    blocks: list[dict[str, Any]],
    metadata: dict[str, Any],
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
                        "source": metadata["source"],
                        "endpoint": metadata["endpoint"],
                        "region_slug": metadata["region_slug"],
                        "region_name": metadata["region_name"],
                        "redata_geo_id": metadata["redata_geo_id"],
                        "ingestion_timestamp": metadata["ingestion_timestamp"],
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
    output_file = output_folder / REDATA_OUTPUT_FILE_NAME
    dataframe = pd.DataFrame(rows)
    dataframe.to_csv(output_file, index=False)
    return output_file


def build_fallback_ingestion_timestamp() -> str:
    """Build a UTC timestamp for rows that do not carry extraction metadata."""
    return datetime.now(timezone.utc).isoformat()
