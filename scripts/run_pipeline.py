"""Run the end-to-end pipeline for the selected regions with one command."""

import argparse
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config.regions import load_regions, prompt_for_regions, resolve_regions_by_slugs
from src.utils.terminal_ui import prompt_for_language, prompt_for_year_range, translate
from src.utils.logger import get_logger

logger = get_logger(__name__)

SCRIPTS_DIR = PROJECT_ROOT / "scripts"
DEFAULT_START_YEAR = 2015
DEFAULT_END_YEAR = 2025


def parse_args() -> argparse.Namespace:
    """Read optional CLI overrides for language, regions and year range."""
    parser = argparse.ArgumentParser(
        description="Run the full energy and weather pipeline for one or more regions."
    )
    parser.add_argument("--language", choices=["es", "en"])
    parser.add_argument(
        "--regions",
        help="Comma-separated region slugs, for example: madrid,andalucia",
    )
    parser.add_argument("--start-year", type=int)
    parser.add_argument("--end-year", type=int)
    return parser.parse_args()


def run_python_script(script_name: str, args: list[str] | None = None) -> None:
    """Run one Python script from the project scripts folder."""
    command = [sys.executable, str(SCRIPTS_DIR / script_name)]
    if args:
        command.extend(args)

    logger.info("Running script: %s", " ".join(command))
    result = subprocess.run(command, cwd=PROJECT_ROOT, check=False, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Script failed: {' '.join(command)}")


def main() -> None:
    """Run extraction, normalization, loading and dbt in order."""
    args = parse_args()
    language = args.language or prompt_for_language()
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

    selected_region_slugs = [region.region_slug for region in selected_regions]
    logger.info(
        "Starting pipeline for regions=%s years=%s-%s language=%s",
        ",".join(selected_region_slugs),
        start_year,
        end_year,
        language,
    )
    extraction_args = [
        "--language",
        language,
        "--regions",
        ",".join(selected_region_slugs),
        "--start-year",
        str(start_year),
        "--end-year",
        str(end_year),
    ]

    run_python_script("run_ingestion.py", extraction_args)
    run_python_script("run_openmeteo.py", extraction_args)
    run_python_script("normalize_redata.py", ["--regions", ",".join(selected_region_slugs)])
    run_python_script("normalize_openmeteo.py", ["--regions", ",".join(selected_region_slugs)])
    run_python_script("aggregate_openmeteo_monthly.py")
    run_python_script("load_redata_to_bigquery.py")
    run_python_script("load_openmeteo_to_bigquery.py")
    run_python_script("run_dbt.py")

    logger.info("Pipeline completed successfully")
    print()
    print("Pipeline completed successfully.")


if __name__ == "__main__":
    main()
