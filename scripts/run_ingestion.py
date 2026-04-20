"""Run a simple REData extraction and save raw JSON locally."""

import argparse
import copy
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config.regions import load_regions, prompt_for_regions, resolve_regions_by_slugs
from src.config.settings import RE_DATA_RAW_PATH
from src.extract.redata.client import REDataClient
from src.utils.terminal_ui import prompt_for_language, prompt_for_year_range, translate

REGION_TIME_TRUNC = "month"
DEFAULT_START_YEAR = 2015
DEFAULT_END_YEAR = 2025


def ensure_data_folder(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def parse_args() -> argparse.Namespace:
    """Read optional CLI overrides for language, regions and year range."""
    parser = argparse.ArgumentParser(description="Run REData extraction for one or more regions.")
    parser.add_argument("--language", choices=["es", "en"])
    parser.add_argument("--regions", help="Comma-separated region slugs, for example: madrid,andalucia")
    parser.add_argument("--start-year", type=int)
    parser.add_argument("--end-year", type=int)
    return parser.parse_args()


def merge_redata_payloads(base_payload: dict, yearly_payload: dict) -> dict:
    """Merge one yearly REData payload into the accumulated payload."""
    merged_payload = copy.deepcopy(base_payload)

    merged_included = merged_payload.get("included", [])
    yearly_included = yearly_payload.get("included", [])
    if not isinstance(merged_included, list) or not isinstance(yearly_included, list):
        raise ValueError("Expected REData payloads to contain an 'included' list.")

    merged_blocks_by_id = {
        block.get("id"): block for block in merged_included if isinstance(block, dict)
    }

    for yearly_block in yearly_included:
        if not isinstance(yearly_block, dict):
            continue

        block_id = yearly_block.get("id")
        if block_id not in merged_blocks_by_id:
            merged_included.append(copy.deepcopy(yearly_block))
            merged_blocks_by_id[block_id] = merged_included[-1]
            continue

        merged_block = merged_blocks_by_id[block_id]
        merged_attributes = merged_block.get("attributes", {})
        yearly_attributes = yearly_block.get("attributes", {})
        merged_content = merged_attributes.get("content", [])
        yearly_content = yearly_attributes.get("content", [])
        if not isinstance(merged_content, list) or not isinstance(yearly_content, list):
            continue

        merged_records_by_id = {
            record.get("id"): record for record in merged_content if isinstance(record, dict)
        }

        for yearly_record in yearly_content:
            if not isinstance(yearly_record, dict):
                continue

            record_id = yearly_record.get("id")
            if record_id not in merged_records_by_id:
                merged_content.append(copy.deepcopy(yearly_record))
                merged_records_by_id[record_id] = merged_content[-1]
                continue

            merged_record = merged_records_by_id[record_id]
            merged_record_attributes = merged_record.get("attributes", {})
            yearly_record_attributes = yearly_record.get("attributes", {})
            merged_values = merged_record_attributes.get("values", [])
            yearly_values = yearly_record_attributes.get("values", [])
            if isinstance(merged_values, list) and isinstance(yearly_values, list):
                merged_values.extend(copy.deepcopy(yearly_values))

            # These aggregate fields come back scoped to the requested range.
            # Once we merge several yearly responses, the single-range totals
            # are no longer reliable, so we clear them.
            merged_record_attributes["total"] = None
            merged_record_attributes["total-percentage"] = None

    return merged_payload


def main() -> None:
    """Fetch one or more regional balances as monthly series.

    Regional REData balance data is treated as monthly because the regional
    endpoint does not expose a stable daily series for communities like Madrid.
    """
    args = parse_args()
    language = args.language or prompt_for_language()
    client = REDataClient()
    output_folder = ensure_data_folder(RE_DATA_RAW_PATH)
    regions = load_regions()

    if args.regions:
        region_slugs = [item.strip().lower() for item in args.regions.split(",") if item.strip()]
        selected_regions = resolve_regions_by_slugs(regions, region_slugs)
    else:
        selected_regions = prompt_for_regions(regions, language)

    if args.start_year is not None and args.end_year is not None:
        start_year = args.start_year
        end_year = args.end_year
        if start_year > end_year:
            raise ValueError(translate(language, "year_invalid_order"))
    elif args.start_year is None and args.end_year is None:
        start_year, end_year = prompt_for_year_range(
            language, DEFAULT_START_YEAR, DEFAULT_END_YEAR
        )
    else:
        raise ValueError("Use both --start-year and --end-year together.")

    start_date = f"{start_year}-01-01T00:00"
    end_date = f"{end_year}-12-31T23:59"

    for region in selected_regions:
        payload: dict | None = None
        requested_years: list[int] = []

        for year in range(start_year, end_year + 1):
            yearly_payload = client.fetch_balance(
                start_date=f"{year}-01-01T00:00",
                end_date=f"{year}-12-31T23:59",
                autonomous_community_id=region.redata_geo_id,
                time_trunc=REGION_TIME_TRUNC,
            )
            requested_years.append(year)

            if payload is None:
                payload = yearly_payload
            else:
                payload = merge_redata_payloads(payload, yearly_payload)

        if payload is None:
            raise RuntimeError("No REData payload was generated for the selected year range.")

        raw_response = {
            "source": client.SOURCE,
            "endpoint": client.ENDPOINT,
            "region_slug": region.region_slug,
            "region_name": region.display_name,
            "redata_geo_id": region.redata_geo_id,
            "requested_start_date": start_date,
            "requested_end_date": end_date,
            "requested_years": requested_years,
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "payload": payload,
        }

        output_file = output_folder / f"redata_{region.region_slug}_monthly_sample.json"
        with output_file.open("w", encoding="utf-8") as handler:
            json.dump(raw_response, handler, indent=2, ensure_ascii=False)

        print(translate(language, "saved_redata", path=output_file))


if __name__ == "__main__":
    main()
