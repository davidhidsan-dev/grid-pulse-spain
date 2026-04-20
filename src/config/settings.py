"""Configuration settings for grid-pulse-spain."""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

DATA_ROOT = Path(os.getenv("DATA_ROOT", "data"))
REFERENCE_DATA_PATH = DATA_ROOT / "reference"
RE_DATA_RAW_PATH = DATA_ROOT / "raw" / "redata"
RE_DATA_PROCESSED_PATH = DATA_ROOT / "processed" / "redata"

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BIGQUERY_DATASET_RAW = os.getenv("BIGQUERY_DATASET_RAW")
BIGQUERY_DATASET_ANALYTICS = os.getenv("BIGQUERY_DATASET_ANALYTICS", "grid_pulse_analytics")
