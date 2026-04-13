"""Minimal REData client for the grid-pulse-spain project."""

import os
from typing import Any

import requests

from src.utils.logger import get_logger

logger = get_logger(__name__)


class REDataClient:
    """Minimal client for the REData public API."""

    SOURCE = "redata"
    DEFAULT_BASE_URL = "https://apidatos.ree.es"
    ENDPOINT = "/es/datos/balance/balance-electrico"
    # REData documents hour broadly, but this widget currently works with day.
    DEFAULT_TIME_TRUNC = "day"
    DEFAULT_GEO_TRUNC = "electric_system"
    AUTONOMOUS_COMMUNITY_GEO_LIMIT = "ccaa"

    def __init__(self, base_url: str | None = None):
        self.base_url = base_url or os.getenv("REDATA_BASE_URL", self.DEFAULT_BASE_URL)

    def _get(self, endpoint: str, params: dict[str, Any]) -> dict[str, Any]:
        """Run a GET request and return the decoded JSON payload."""
        url = f"{self.base_url}{endpoint}"

        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
        except requests.RequestException as error:
            logger.error("REData request failed: %s", error)
            raise

        try:
            data = response.json()
        except ValueError as error:
            logger.error("REData returned a non-JSON response: %s", error)
            raise

        if isinstance(data, dict) and "errors" in data:
            logger.error("REData API error: %s", data["errors"])
            raise ValueError(f"API returned errors: {data['errors']}")

        return data

    def fetch_balance(
        self,
        start_date: str,
        end_date: str,
        autonomous_community_id: int | str | None = None,
    ) -> dict[str, Any]:
        """Fetch balance-electrico data using daily granularity.

        When an autonomous community is provided, REData requires the
        geo_trunc/geo_limit/geo_ids combination documented in the official API.
        """
        params = {
            "start_date": start_date,
            "end_date": end_date,
            "time_trunc": self.DEFAULT_TIME_TRUNC,
        }

        if autonomous_community_id is not None:
            params.update(
                {
                    "geo_trunc": self.DEFAULT_GEO_TRUNC,
                    "geo_limit": self.AUTONOMOUS_COMMUNITY_GEO_LIMIT,
                    "geo_ids": str(autonomous_community_id),
                }
            )

        logger.info("Requesting REData balance data with params: %s", params)
        return self._get(self.ENDPOINT, params)
