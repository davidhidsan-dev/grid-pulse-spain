"""Configuration settings for grid-pulse-spain."""

import os
from pathlib import Path

DATA_ROOT = Path(os.getenv("DATA_ROOT", "data"))
RE_DATA_RAW_PATH = DATA_ROOT / "raw" / "redata"
