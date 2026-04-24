"""Run the end-to-end pipeline for the selected regions with one command."""

import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config.regions import load_regions, prompt_for_regions
from src.utils.terminal_ui import prompt_for_language, prompt_for_year_range
from src.utils.logger import get_logger

logger = get_logger(__name__)

SCRIPTS_DIR = PROJECT_ROOT / "scripts"
DEFAULT_START_YEAR = 2015
DEFAULT_END_YEAR = 2025


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
    language = prompt_for_language()
    selected_regions = prompt_for_regions(load_regions(), language)
    start_year, end_year = prompt_for_year_range(
        language, DEFAULT_START_YEAR, DEFAULT_END_YEAR
    )

    selected_region_slugs = [region.region_slug for region in selected_regions]
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
    run_python_script("inspect_redata_json.py", ["--regions", ",".join(selected_region_slugs)])
    run_python_script("normalize_openmeteo.py", ["--regions", ",".join(selected_region_slugs)])
    run_python_script("aggregate_openmeteo_monthly.py")
    run_python_script("load_redata_to_bigquery.py")
    run_python_script("load_openmeteo_to_bigquery.py")
    run_python_script("run_dbt.py")

    print()
    print("Pipeline completed successfully.")


if __name__ == "__main__":
    main()
