import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.run_openmeteo import merge_openmeteo_payloads


class TestRunOpenMeteo(unittest.TestCase):
    def test_merge_openmeteo_payloads_rejects_non_list_daily_series(self) -> None:
        """Reject unexpected scalar values inside payload['daily']."""
        base_payload = {
            "daily": {
                "time": ["2025-01-01"],
                "temperature_2m_mean": [8.4],
            }
        }
        yearly_payload = {
            "daily": {
                "time": ["2025-01-02"],
                "temperature_2m_mean": 9.1,
            }
        }

        with self.assertRaises(ValueError):
            merge_openmeteo_payloads(base_payload, yearly_payload)


if __name__ == "__main__":
    unittest.main()
