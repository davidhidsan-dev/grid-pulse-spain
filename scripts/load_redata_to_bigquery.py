"""Load the normalized REData CSV into a BigQuery raw table."""

import sys
from pathlib import Path

from google.cloud import bigquery

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config.settings import BIGQUERY_DATASET_RAW, GCP_PROJECT_ID, RE_DATA_PROCESSED_PATH
from src.load.bigquery_loader import (
    REDATA_BALANCE_SCHEMA,
    deduplicate_bigquery_table,
    get_bigquery_client,
    load_csv_to_bigquery,
)

CSV_FILE_NAME = "redata_balance_electrico_monthly_normalized.csv"
TABLE_NAME = "redata_balance_electrico"


def main() -> None:
    """Append processed monthly REData rows to BigQuery and remove duplicates."""
    csv_path = RE_DATA_PROCESSED_PATH / CSV_FILE_NAME
    table_id = load_csv_to_bigquery(
        csv_path=csv_path,
        table_name=TABLE_NAME,
        schema=REDATA_BALANCE_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    deduplicate_bigquery_table(
        table_id=table_id,
        unique_key_columns=["region_slug", "metric_id", "datetime"],
    )

    client = get_bigquery_client()
    table = client.get_table(table_id)

    print(f"CSV file: {csv_path}")
    print(f"Project: {GCP_PROJECT_ID}")
    print(f"Dataset: {BIGQUERY_DATASET_RAW}")
    print(f"Table: {table_id}")
    print(f"Rows after deduplication: {table.num_rows}")


if __name__ == "__main__":
    main()
