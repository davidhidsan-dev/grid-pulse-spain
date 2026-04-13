"""Load the normalized Open-Meteo Madrid CSV into a BigQuery raw table."""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config.settings import BIGQUERY_DATASET_RAW, GCP_PROJECT_ID
from src.load.bigquery_loader import (
    OPENMETEO_MADRID_MONTHLY_SCHEMA,
    get_bigquery_client,
    load_csv_to_bigquery,
)

CSV_PATH = PROJECT_ROOT / "data" / "processed" / "openmeteo" / "openmeteo_madrid_monthly_normalized.csv"
TABLE_NAME = "openmeteo_madrid_monthly"


def main() -> None:
    """Load the normalized monthly Open-Meteo CSV into the raw BigQuery dataset."""
    table_id = load_csv_to_bigquery(
        csv_path=CSV_PATH,
        table_name=TABLE_NAME,
        schema=OPENMETEO_MADRID_MONTHLY_SCHEMA,
    )

    client = get_bigquery_client()
    table = client.get_table(table_id)

    print(f"CSV file: {CSV_PATH}")
    print(f"Project: {GCP_PROJECT_ID}")
    print(f"Dataset: {BIGQUERY_DATASET_RAW}")
    print(f"Table: {table_id}")
    print(f"Rows loaded: {table.num_rows}")


if __name__ == "__main__":
    main()
