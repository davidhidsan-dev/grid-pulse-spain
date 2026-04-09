"""Minimal REData client for the grid-pulse-spain project."""

import os
from typing import Any

import requests

from src.utils.logger import get_logger

logger = get_logger(__name__)


class REDataClient:
    """Minimal client for the REData public API."""

    DEFAULT_BASE_URL = "https://apidatos.ree.es"
    ENDPOINT = "/es/datos/mercados/precios-mercados-tiempo-real"

    def __init__(self, base_url: str | None = None):
        self.base_url = base_url or os.getenv("REDATA_BASE_URL", self.DEFAULT_BASE_URL)

    def fetch_prices(self, start_date: str, end_date: str) -> dict[str, Any]:
        """Fetch hourly price data from REData for a small date range."""
        url = f"{self.base_url}{self.ENDPOINT}"
        params = {
            "start_date": f"{start_date}T00:00",
            "end_date": f"{end_date}T23:59",
            "time_trunc": "hour",
        }

        logger.info("Requesting REData prices")
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as error:
            logger.error("REData request failed: %s", error)
            raise
