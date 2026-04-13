
"""Small client for the Open-Meteo Historical Weather API."""

from typing import Any

import requests

from src.utils.logger import get_logger

logger = get_logger(__name__)


class OpenMeteoClient:
    """Minimal client for querying Open-Meteo historical daily weather data."""

    SOURCE = "open_meteo"
    DEFAULT_BASE_URL = "https://archive-api.open-meteo.com/v1/archive"
    DEFAULT_TIMEOUT_SECONDS = 30

    def __init__(self, base_url: str | None = None, timeout: int = DEFAULT_TIMEOUT_SECONDS):
        self.base_url = base_url or self.DEFAULT_BASE_URL
        self.timeout = timeout

    def _get(self, params: dict[str, Any]) -> dict[str, Any]:
        """Send a GET request to Open-Meteo and return the parsed JSON body."""
        try:
            response = requests.get(self.base_url, params=params, timeout=self.timeout)
            response.raise_for_status()
        except requests.RequestException as error:
            logger.error("Open-Meteo request failed: %s", error)
            raise

        try:
            return response.json()
        except ValueError as error:
            logger.error("Open-Meteo returned a non-JSON response: %s", error)
            raise

    def fetch_daily_history(
        self,
        latitude: float,
        longitude: float,
        start_date: str,
        end_date: str,
        timezone: str,
        daily: list[str],
    ) -> dict[str, Any]:
        """Fetch daily historical weather data for a location and date range."""
        self._validate_request(
            latitude=latitude,
            longitude=longitude,
            start_date=start_date,
            end_date=end_date,
            timezone=timezone,
            daily=daily,
        )

        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start_date,
            "end_date": end_date,
            "timezone": timezone,
            "daily": daily,
        }

        logger.info("Requesting Open-Meteo daily history with params: %s", params)
        return self._get(params)

    @staticmethod
    def _validate_request(
        latitude: float,
        longitude: float,
        start_date: str,
        end_date: str,
        timezone: str,
        daily: list[str],
    ) -> None:
        """Perform small input checks before calling the API."""
        if not -90 <= latitude <= 90:
            raise ValueError("latitude must be between -90 and 90")
        if not -180 <= longitude <= 180:
            raise ValueError("longitude must be between -180 and 180")
        if not start_date:
            raise ValueError("start_date is required")
        if not end_date:
            raise ValueError("end_date is required")
        if not timezone:
            raise ValueError("timezone is required")
        if not daily:
            raise ValueError("daily must contain at least one variable")
