"""Configuration settings for grid-pulse-spain."""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[2]
_data_root_env = os.getenv("DATA_ROOT")
if _data_root_env:
    DATA_ROOT = Path(_data_root_env)
    if not DATA_ROOT.is_absolute():
        DATA_ROOT = PROJECT_ROOT / DATA_ROOT
else:
    DATA_ROOT = PROJECT_ROOT / "data"

REFERENCE_DATA_PATH = DATA_ROOT / "reference"
RE_DATA_RAW_PATH = DATA_ROOT / "raw" / "redata"
RE_DATA_PROCESSED_PATH = DATA_ROOT / "processed" / "redata"
OPENMETEO_RAW_PATH = DATA_ROOT / "raw" / "openmeteo"
OPENMETEO_PROCESSED_PATH = DATA_ROOT / "processed" / "openmeteo"

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BIGQUERY_DATASET_RAW = os.getenv("BIGQUERY_DATASET_RAW")
BIGQUERY_DATASET_ANALYTICS = os.getenv("BIGQUERY_DATASET_ANALYTICS", "grid_pulse_analytics")
