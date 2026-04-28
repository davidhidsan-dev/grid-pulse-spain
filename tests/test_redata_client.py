import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.extract.redata.client import REDataClient


class TestREDataClient(unittest.TestCase):
    def setUp(self) -> None:
        """Create a client instance for each test case."""
        self.client = REDataClient()

    @patch("src.extract.redata.client.requests.get")
    def test_fetch_balance_returns_parsed_json(self, mock_get: Mock) -> None:
        """Return parsed JSON when the REData request succeeds."""
        expected_payload = {
            "data": {"type": "balance-electrico"},
            "included": [],
        }

        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = expected_payload
        mock_get.return_value = mock_response

        payload = self.client.fetch_balance(
            start_date="2025-01-01T00:00",
            end_date="2025-12-31T23:59",
            autonomous_community_id=13,
            time_trunc="month",
        )

        self.assertEqual(payload, expected_payload)
        mock_get.assert_called_once_with(
            f"{self.client.base_url}{self.client.ENDPOINT}",
            params={
                "start_date": "2025-01-01T00:00",
                "end_date": "2025-12-31T23:59",
                "time_trunc": "month",
                "geo_trunc": self.client.DEFAULT_GEO_TRUNC,
                "geo_limit": self.client.AUTONOMOUS_COMMUNITY_GEO_LIMIT,
                "geo_ids": "13",
            },
            timeout=self.client.timeout,
        )

    @patch("src.extract.redata.client.requests.get")
    def test_fetch_balance_without_region_uses_only_base_params(
        self, mock_get: Mock
    ) -> None:
        """Avoid regional params when no autonomous community is requested."""
        expected_payload = {"included": []}

        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = expected_payload
        mock_get.return_value = mock_response

        payload = self.client.fetch_balance(
            start_date="2025-01-01T00:00",
            end_date="2025-01-31T23:59",
            autonomous_community_id=None,
            time_trunc="day",
        )

        self.assertEqual(payload, expected_payload)
        mock_get.assert_called_once_with(
            f"{self.client.base_url}{self.client.ENDPOINT}",
            params={
                "start_date": "2025-01-01T00:00",
                "end_date": "2025-01-31T23:59",
                "time_trunc": "day",
            },
            timeout=self.client.timeout,
        )

    @patch("src.extract.redata.client.requests.get")
    def test_fetch_balance_raises_http_errors(self, mock_get: Mock) -> None:
        """Propagate request errors raised by the underlying HTTP client."""
        mock_get.side_effect = requests.RequestException("network error")

        with self.assertRaises(requests.RequestException):
            self.client.fetch_balance(
                start_date="2025-01-01T00:00",
                end_date="2025-12-31T23:59",
                autonomous_community_id=13,
                time_trunc="month",
            )

    @patch("src.extract.redata.client.requests.get")
    def test_fetch_balance_raises_when_api_returns_errors(self, mock_get: Mock) -> None:
        """Raise a ValueError when the API encodes an errors block in JSON."""
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {"errors": [{"detail": "Bad request"}]}
        mock_get.return_value = mock_response

        with self.assertRaises(ValueError):
            self.client.fetch_balance(
                start_date="2025-01-01T00:00",
                end_date="2025-12-31T23:59",
                autonomous_community_id=13,
                time_trunc="month",
            )


if __name__ == "__main__":
    unittest.main()
