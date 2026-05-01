"""Airflow DAG for the grid-pulse-spain pipeline."""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.models.param import Param
from airflow.operators.python import get_current_context

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config.regions import load_regions

logger = logging.getLogger(__name__)

SCRIPTS_DIR = PROJECT_ROOT / "scripts"
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt_project"
PYTHON_BIN = os.getenv("GRID_PULSE_PYTHON_BIN", sys.executable)
AVAILABLE_REGION_SLUGS = [region.region_slug for region in load_regions()]
AVAILABLE_REGION_SLUGS_SET = set(AVAILABLE_REGION_SLUGS)
REGIONS_DESCRIPTION = (
    "Enter one or more regions separated by commas "
    "(for example: madrid,andalucia,asturias) or use 'all'. "
    f"Valid options: {', '.join(AVAILABLE_REGION_SLUGS)}"
)
DBT_TARGET_PATH = "/tmp/grid-pulse-dbt-target"
NON_RETRYABLE_ERROR_MARKERS = [
    "unknown region slug",
    "start_year cannot be greater than end_year",
    "at least one region slug is required",
    "use both --start-year and --end-year together",
    "csv file not found",
    "regional reference file not found",
    "missing gcp_project_id",
    "missing bigquery_dataset_raw",
    "dbt command failed",
    "encountered an error:",
]


def _resolve_region_slugs(regions_value: str) -> list[str]:
    """Resolve region selection from DAG params."""
    normalized_value = regions_value.strip().lower()
    if normalized_value == "all":
        return [region.region_slug for region in load_regions()]
    return [item.strip().lower() for item in regions_value.split(",") if item.strip()]


def _get_runtime_params() -> dict[str, int | list[str]]:
    """Read and validate runtime params from the current task context."""
    context = get_current_context()
    params = context["params"]

    start_year = int(params["start_year"])
    end_year = int(params["end_year"])
    region_slugs = _resolve_region_slugs(str(params["regions"]))

    if start_year > end_year:
        raise AirflowException("start_year cannot be greater than end_year.")
    if not region_slugs:
        raise AirflowException("At least one region slug is required.")

    return {
        "start_year": start_year,
        "end_year": end_year,
        "region_slugs": region_slugs,
    }


def _run_subprocess(
    command: list[str],
    cwd: Path | None = None,
    extra_env: dict[str, str] | None = None,
) -> str:
    """Run one subprocess command and raise a detailed Airflow error on failure."""
    logger.info("Running command: %s", " ".join(command))

    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)

    result = subprocess.run(
        command,
        cwd=str(cwd) if cwd else None,
        text=True,
        capture_output=True,
        env=env,
        check=False,
    )

    stdout = result.stdout.strip()
    stderr = result.stderr.strip()

    if stdout:
        logger.info("STDOUT:\n%s", stdout)
    if stderr:
        logger.error("STDERR:\n%s", stderr)

    if result.returncode != 0:
        combined_output = f"{result.stdout}\n{result.stderr}".lower()
        exception_class = (
            AirflowFailException
            if any(marker in combined_output for marker in NON_RETRYABLE_ERROR_MARKERS)
            else AirflowException
        )
        raise exception_class(
            f"Command failed: {' '.join(command)}\n\nSTDOUT:\n{result.stdout}\n\nSTDERR:\n{result.stderr}"
        )

    return result.stdout


def _build_extraction_command(script_name: str) -> list[str]:
    """Build the command for one extraction script."""
    runtime_params = _get_runtime_params()
    region_slugs = runtime_params["region_slugs"]

    return [
        PYTHON_BIN,
        str(SCRIPTS_DIR / script_name),
        "--language",
        "en",
        "--regions",
        ",".join(region_slugs),
        "--start-year",
        str(runtime_params["start_year"]),
        "--end-year",
        str(runtime_params["end_year"]),
    ]


def _build_openmeteo_region_command(region_slug: str, start_year: str, end_year: str) -> list[str]:
    """Build the Open-Meteo extraction command for one region only."""
    return [
        PYTHON_BIN,
        str(SCRIPTS_DIR / "run_openmeteo.py"),
        "--language",
        "en",
        "--regions",
        region_slug,
        "--start-year",
        start_year,
        "--end-year",
        end_year,
    ]


def _build_region_command(script_name: str) -> list[str]:
    """Build the command for one normalization script that only needs regions."""
    runtime_params = _get_runtime_params()
    region_slugs = runtime_params["region_slugs"]

    return [
        PYTHON_BIN,
        str(SCRIPTS_DIR / script_name),
        "--regions",
        ",".join(region_slugs),
    ]


def _dbt_env() -> dict[str, str]:
    """Return the environment variables needed for dbt inside Airflow."""
    return {
        "DBT_PROFILES_DIR": str(DBT_PROJECT_DIR),
        "DBT_TARGET_PATH": DBT_TARGET_PATH,
    }


@dag(
    dag_id="grid_pulse_pipeline",
    description="Extract, normalize, load and model Spanish energy and weather data.",
    start_date=datetime(2026, 4, 30),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=3),
    tags=["grid-pulse", "energy", "weather"],
    params={
        "regions": Param(
            "madrid",
            type="string",
            description=REGIONS_DESCRIPTION,
        ),
        "start_year": Param(
            2015,
            type="integer",
            description="Start year. If you want a single year, use the same value for start and end.",
        ),
        "end_year": Param(
            2025,
            type="integer",
            description="End year. If you want a single year, use the same value for start and end.",
        ),
    },
    default_args={
        "owner": "grid-pulse-spain",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
)
def grid_pulse_pipeline():
    """Orchestrate the grid-pulse pipeline with explicit Airflow tasks."""

    @task(retries=0, execution_timeout=timedelta(minutes=2))
    def validate_params() -> dict[str, str]:
        """Fail fast for invalid user inputs before starting expensive tasks."""
        runtime_params = _get_runtime_params()
        invalid_regions = [
            region_slug
            for region_slug in runtime_params["region_slugs"]
            if region_slug not in AVAILABLE_REGION_SLUGS_SET
        ]
        if invalid_regions:
            raise AirflowFailException(
                "Invalid region slug(s): "
                f"{', '.join(invalid_regions)}. Valid options: {', '.join(AVAILABLE_REGION_SLUGS)}"
            )

        return {
            "regions": ",".join(runtime_params["region_slugs"]),
            "start_year": str(runtime_params["start_year"]),
            "end_year": str(runtime_params["end_year"]),
        }

    @task_group(group_id="extract")
    def extract_group(validated_params: dict[str, str]) -> tuple[dict[str, str], dict[str, str]]:
        """Extraction tasks."""

        @task(retries=2, retry_delay=timedelta(minutes=3), execution_timeout=timedelta(minutes=45))
        def extract_redata(_: dict[str, str]) -> dict[str, str]:
            """Extract raw REData payloads for the selected regions and years."""
            _run_subprocess(_build_extraction_command("run_ingestion.py"), cwd=PROJECT_ROOT)
            runtime_params = _get_runtime_params()
            return {
                "regions": ",".join(runtime_params["region_slugs"]),
                "start_year": str(runtime_params["start_year"]),
                "end_year": str(runtime_params["end_year"]),
            }

        @task
        def build_openmeteo_region_requests(
            runtime_params: dict[str, str],
        ) -> list[dict[str, str]]:
            """Create one Open-Meteo extraction request per region."""
            return [
                {
                    "region_slug": region_slug,
                    "start_year": runtime_params["start_year"],
                    "end_year": runtime_params["end_year"],
                }
                for region_slug in runtime_params["regions"].split(",")
                if region_slug
            ]

        @task(retries=2, retry_delay=timedelta(minutes=5), execution_timeout=timedelta(minutes=20))
        def extract_openmeteo_region(region_request: dict[str, str]) -> str:
            """Extract raw Open-Meteo payloads for one region."""
            _run_subprocess(
                _build_openmeteo_region_command(
                    region_slug=region_request["region_slug"],
                    start_year=region_request["start_year"],
                    end_year=region_request["end_year"],
                ),
                cwd=PROJECT_ROOT,
            )
            return region_request["region_slug"]

        @task
        def finalize_openmeteo_extract(
            extracted_region_slugs: list[str], runtime_params: dict[str, str]
        ) -> dict[str, str]:
            """Wait for all mapped Open-Meteo region tasks to finish before continuing."""
            if sorted(extracted_region_slugs) != sorted(runtime_params["regions"].split(",")):
                raise AirflowFailException(
                    "Open-Meteo extraction finished with an unexpected region set."
                )
            return {
                "regions": runtime_params["regions"],
                "start_year": runtime_params["start_year"],
                "end_year": runtime_params["end_year"],
            }

        openmeteo_region_requests = build_openmeteo_region_requests(validated_params)
        openmeteo_region_results = extract_openmeteo_region.expand(
            region_request=openmeteo_region_requests
        )
        openmeteo_extract_result = finalize_openmeteo_extract(
            openmeteo_region_results,
            validated_params,
        )

        return extract_redata(validated_params), openmeteo_extract_result

    @task_group(group_id="transform")
    def transform_group(
        redata_extract_result: dict[str, str],
        openmeteo_extract_result: dict[str, str],
    ) -> tuple[dict[str, str], None]:
        """Transformation tasks."""

        @task(execution_timeout=timedelta(minutes=20))
        def normalize_redata(_: dict[str, str]) -> dict[str, str]:
            """Normalize raw REData JSON files into one flat monthly CSV."""
            _run_subprocess(_build_region_command("normalize_redata.py"), cwd=PROJECT_ROOT)
            runtime_params = _get_runtime_params()
            return {"regions": ",".join(runtime_params["region_slugs"])}

        @task(execution_timeout=timedelta(minutes=20))
        def normalize_openmeteo(_: dict[str, str]) -> dict[str, str]:
            """Normalize raw Open-Meteo JSON files into one flat daily CSV."""
            _run_subprocess(_build_region_command("normalize_openmeteo.py"), cwd=PROJECT_ROOT)
            runtime_params = _get_runtime_params()
            return {"regions": ",".join(runtime_params["region_slugs"])}

        @task(execution_timeout=timedelta(minutes=10))
        def aggregate_openmeteo_monthly(_: dict[str, str]) -> None:
            """Aggregate the normalized Open-Meteo daily CSV to monthly grain."""
            _run_subprocess(
                [PYTHON_BIN, str(SCRIPTS_DIR / "aggregate_openmeteo_monthly.py")],
                cwd=PROJECT_ROOT,
            )

        redata_normalized = normalize_redata(redata_extract_result)
        openmeteo_normalized = normalize_openmeteo(openmeteo_extract_result)
        openmeteo_aggregated = aggregate_openmeteo_monthly(openmeteo_normalized)
        return redata_normalized, openmeteo_aggregated

    @task_group(group_id="load")
    def load_group(redata_normalized_result: dict[str, str]) -> tuple[None, None]:
        """BigQuery loading tasks."""

        @task(retries=2, retry_delay=timedelta(minutes=2), execution_timeout=timedelta(minutes=20))
        def load_redata_to_bigquery(_: dict[str, str]) -> None:
            """Load normalized REData rows into the raw BigQuery layer."""
            _run_subprocess(
                [PYTHON_BIN, str(SCRIPTS_DIR / "load_redata_to_bigquery.py")],
                cwd=PROJECT_ROOT,
            )

        @task(retries=2, retry_delay=timedelta(minutes=2), execution_timeout=timedelta(minutes=20))
        def load_openmeteo_to_bigquery() -> None:
            """Load normalized monthly Open-Meteo rows into the raw BigQuery layer."""
            _run_subprocess(
                [PYTHON_BIN, str(SCRIPTS_DIR / "load_openmeteo_to_bigquery.py")],
                cwd=PROJECT_ROOT,
            )

        return load_redata_to_bigquery(redata_normalized_result), load_openmeteo_to_bigquery()

    @task_group(group_id="model")
    def model_group() -> tuple[None, None, None]:
        """dbt modeling tasks."""

        @task(retries=1, execution_timeout=timedelta(minutes=30))
        def dbt_run_staging() -> None:
            """Run only dbt staging models inside the container-specific target path."""
            _run_subprocess(
                ["dbt", "run", "--select", "staging", "--no-partial-parse"],
                cwd=DBT_PROJECT_DIR,
                extra_env=_dbt_env(),
            )

        @task(retries=1, execution_timeout=timedelta(minutes=30))
        def dbt_run_marts() -> None:
            """Run only dbt marts models inside the container-specific target path."""
            _run_subprocess(
                ["dbt", "run", "--select", "marts", "--no-partial-parse"],
                cwd=DBT_PROJECT_DIR,
                extra_env=_dbt_env(),
            )

        @task(retries=1, execution_timeout=timedelta(minutes=20))
        def dbt_test() -> None:
            """Run dbt tests after model execution."""
            _run_subprocess(
                ["dbt", "test", "--no-partial-parse"],
                cwd=DBT_PROJECT_DIR,
                extra_env=_dbt_env(),
            )

        staging = dbt_run_staging()
        marts = dbt_run_marts()
        tests = dbt_test()
        staging >> marts >> tests
        return staging, marts, tests

    validated_params = validate_params()
    redata_extract_result, openmeteo_extract_result = extract_group(validated_params)
    redata_normalized_result, openmeteo_aggregated_result = transform_group(
        redata_extract_result, openmeteo_extract_result
    )
    redata_loaded, openmeteo_loaded = load_group(redata_normalized_result)
    staging, marts, tests = model_group()

    openmeteo_aggregated_result >> openmeteo_loaded
    redata_loaded >> staging
    openmeteo_loaded >> staging


grid_pulse_pipeline()
