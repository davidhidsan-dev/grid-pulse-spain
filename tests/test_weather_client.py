import unittest
import sys
from pathlib import Path
from unittest.mock import Mock, patch

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.extract.weather.client import OpenMeteoClient


class TestOpenMeteoClient(unittest.TestCase):
    def setUp(self) -> None:
        self.client = OpenMeteoClient()

    @patch("src.extract.weather.client.requests.get")
    def test_fetch_daily_history_returns_parsed_json(self, mock_get: Mock) -> None:
        expected_payload = {
            "latitude": 40.4,
            "longitude": -3.7,
            "daily": {"time": ["2025-01-01"], "temperature_2m_mean": [8.4]},
        }

        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = expected_payload
        mock_get.return_value = mock_response

        payload = self.client.fetch_daily_history(
            latitude=40.4168,
            longitude=-3.7038,
            start_date="2025-01-01",
            end_date="2025-01-07",
            timezone="Europe/Madrid",
            daily=["temperature_2m_mean"],
        )

        self.assertEqual(payload, expected_payload)
        mock_get.assert_called_once_with(
            self.client.base_url,
            params={
                "latitude": 40.4168,
                "longitude": -3.7038,
                "start_date": "2025-01-01",
                "end_date": "2025-01-07",
                "timezone": "Europe/Madrid",
                "daily": ["temperature_2m_mean"],
            },
            timeout=self.client.timeout,
        )

    @patch("src.extract.weather.client.requests.get")
    def test_fetch_daily_history_validates_daily_variables(self, mock_get: Mock) -> None:
        with self.assertRaises(ValueError):
            self.client.fetch_daily_history(
                latitude=40.4168,
                longitude=-3.7038,
                start_date="2025-01-01",
                end_date="2025-01-07",
                timezone="Europe/Madrid",
                daily=[],
            )

        mock_get.assert_not_called()

    @patch("src.extract.weather.client.requests.get")
    def test_fetch_daily_history_raises_http_errors(self, mock_get: Mock) -> None:
        mock_get.side_effect = requests.RequestException("network error")

        with self.assertRaises(requests.RequestException):
            self.client.fetch_daily_history(
                latitude=40.4168,
                longitude=-3.7038,
                start_date="2025-01-01",
                end_date="2025-01-07",
                timezone="Europe/Madrid",
                daily=["temperature_2m_mean"],
            )


if __name__ == "__main__":
    unittest.main()
