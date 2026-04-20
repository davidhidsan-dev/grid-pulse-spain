"""Helpers to load local CSV files into BigQuery raw tables."""

from pathlib import Path

from google.api_core.exceptions import GoogleAPIError, NotFound
from google.cloud import bigquery

from src.config.settings import BIGQUERY_DATASET_RAW, GCP_PROJECT_ID


REDATA_BALANCE_SCHEMA = [
    bigquery.SchemaField("source", "STRING"),
    bigquery.SchemaField("endpoint", "STRING"),
    bigquery.SchemaField("region_slug", "STRING"),
    bigquery.SchemaField("region_name", "STRING"),
    bigquery.SchemaField("redata_geo_id", "INT64"),
    bigquery.SchemaField("ingestion_timestamp", "TIMESTAMP"),
    bigquery.SchemaField("group_type", "STRING"),
    bigquery.SchemaField("group_id", "STRING"),
    bigquery.SchemaField("group_title", "STRING"),
    bigquery.SchemaField("metric_type", "STRING"),
    bigquery.SchemaField("metric_id", "STRING"),
    bigquery.SchemaField("metric_group_id", "STRING"),
    bigquery.SchemaField("metric_title", "STRING"),
    bigquery.SchemaField("metric_description", "STRING"),
    bigquery.SchemaField("is_composite", "BOOL"),
    bigquery.SchemaField("last_update", "TIMESTAMP"),
    bigquery.SchemaField("total", "FLOAT64"),
    bigquery.SchemaField("total_percentage", "FLOAT64"),
    bigquery.SchemaField("year_month", "STRING"),
    bigquery.SchemaField("datetime", "TIMESTAMP"),
    bigquery.SchemaField("value", "FLOAT64"),
    bigquery.SchemaField("percentage", "FLOAT64"),
]

OPENMETEO_MONTHLY_SCHEMA = [
    bigquery.SchemaField("source", "STRING"),
    bigquery.SchemaField("ingestion_timestamp", "TIMESTAMP"),
    bigquery.SchemaField("region_slug", "STRING"),
    bigquery.SchemaField("region_name", "STRING"),
    bigquery.SchemaField("location_name", "STRING"),
    bigquery.SchemaField("latitude", "FLOAT64"),
    bigquery.SchemaField("longitude", "FLOAT64"),
    bigquery.SchemaField("timezone", "STRING"),
    bigquery.SchemaField("weather_point_type", "STRING"),
    bigquery.SchemaField("year_month", "STRING"),
    bigquery.SchemaField("temperature_2m_max_avg", "FLOAT64"),
    bigquery.SchemaField("temperature_2m_mean_avg", "FLOAT64"),
    bigquery.SchemaField("temperature_2m_min_avg", "FLOAT64"),
    bigquery.SchemaField("precipitation_sum_total", "FLOAT64"),
    bigquery.SchemaField("wind_speed_10m_max_avg", "FLOAT64"),
    bigquery.SchemaField("shortwave_radiation_sum_total", "FLOAT64"),
]


def get_bigquery_client() -> bigquery.Client:
    """Return a BigQuery client using the configured project."""
    if not GCP_PROJECT_ID:
        raise ValueError("Missing GCP_PROJECT_ID in environment configuration.")

    return bigquery.Client(project=GCP_PROJECT_ID)


def ensure_dataset_exists(client: bigquery.Client, dataset_name: str) -> str:
    """Create the dataset if it does not exist yet."""
    dataset_id = f"{client.project}.{dataset_name}"

    try:
        client.get_dataset(dataset_id)
    except NotFound:
        dataset = bigquery.Dataset(dataset_id)
        client.create_dataset(dataset)
    except GoogleAPIError as error:
        raise RuntimeError(f"Unable to verify dataset {dataset_id}: {error}") from error

    return dataset_id


def load_csv_to_bigquery(
    csv_path: Path,
    table_name: str,
    schema: list[bigquery.SchemaField],
    write_disposition: str = bigquery.WriteDisposition.WRITE_APPEND,
) -> str:
    """Load a local CSV file into a BigQuery table."""
    if not BIGQUERY_DATASET_RAW:
        raise ValueError("Missing BIGQUERY_DATASET_RAW in environment configuration.")
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    client = get_bigquery_client()
    ensure_dataset_exists(client, BIGQUERY_DATASET_RAW)

    table_id = f"{client.project}.{BIGQUERY_DATASET_RAW}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition=write_disposition,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )

    try:
        with csv_path.open("rb") as file_obj:
            job = client.load_table_from_file(file_obj, table_id, job_config=job_config)
        job.result()
    except GoogleAPIError as error:
        raise RuntimeError(f"BigQuery load failed for {table_id}: {error}") from error

    return table_id
