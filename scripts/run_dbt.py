"""Run dbt models and tests with environment variables loaded from .env."""

import os
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.logger import get_logger

DBT_PROJECT_DIR = PROJECT_ROOT / "dbt_project"

logger = get_logger(__name__)


def run_dbt_command(command: list[str]) -> None:
    """Run a dbt command inside the dbt project directory."""
    logger.info("Running dbt command: %s", " ".join(command))

    result = subprocess.run(
        command,
        cwd=DBT_PROJECT_DIR,
        check=False,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(f"dbt command failed: {' '.join(command)}")


def main() -> None:
    load_dotenv()
    os.environ["DBT_PROFILES_DIR"] = str(DBT_PROJECT_DIR)

    logger.info("Running dbt models")
    run_dbt_command(["dbt", "run"])

    logger.info("Running dbt tests")
    run_dbt_command(["dbt", "test"])


if __name__ == "__main__":
    main()
