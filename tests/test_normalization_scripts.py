import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.transform import openmeteo as normalize_openmeteo
from src.transform import redata as normalize_redata


class TestNormalizeOpenMeteoScript(unittest.TestCase):
    def test_load_json_raises_when_top_level_is_not_an_object(self) -> None:
        """Reject Open-Meteo JSON files whose top level is not an object."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            input_file = Path(temporary_directory) / "payload.json"
            input_file.write_text("[]", encoding="utf-8")

            with self.assertRaises(ValueError):
                normalize_openmeteo.load_json(input_file)

    def test_normalize_daily_payload_flattens_daily_series(self) -> None:
        """Flatten a valid Open-Meteo daily payload into one row per day."""
        raw_json = {
            "source": "open_meteo",
            "extracted_at": "2026-01-01T00:00:00+00:00",
            "region_slug": "madrid",
            "region_name": "Comunidad de Madrid",
            "location_name": "Madrid",
            "latitude": 40.4,
            "longitude": -3.7,
            "timezone": "Europe/Madrid",
            "weather_point_type": "centroid",
            "payload": {
                "daily": {
                    "time": ["2025-01-01", "2025-01-02"],
                    "temperature_2m_max": [10.0, 11.0],
                    "temperature_2m_mean": [7.0, 8.0],
                    "temperature_2m_min": [4.0, 5.0],
                    "precipitation_sum": [0.0, 1.2],
                    "weather_code": [1, 3],
                    "wind_speed_10m_max": [15.0, 20.0],
                    "shortwave_radiation_sum": [100.0, 120.0],
                },
                "daily_units": {},
            },
        }

        rows = normalize_openmeteo.normalize_daily_payload(raw_json)

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["region_slug"], "madrid")
        self.assertEqual(rows[1]["date"], "2025-01-02")
        self.assertEqual(rows[1]["shortwave_radiation_sum"], 120.0)

    def test_normalize_daily_payload_raises_for_mismatched_series_lengths(self) -> None:
        """Raise when one daily series has a different length than the date list."""
        raw_json = {
            "payload": {
                "daily": {
                    "time": ["2025-01-01", "2025-01-02"],
                    "temperature_2m_max": [10.0],
                    "temperature_2m_mean": [7.0, 8.0],
                    "temperature_2m_min": [4.0, 5.0],
                    "precipitation_sum": [0.0, 1.2],
                    "weather_code": [1, 3],
                    "wind_speed_10m_max": [15.0, 20.0],
                    "shortwave_radiation_sum": [100.0, 120.0],
                }
            }
        }

        with self.assertRaises(ValueError):
            normalize_openmeteo.normalize_daily_payload(raw_json)

    def test_resolve_input_files_returns_all_files_when_no_regions_are_given(self) -> None:
        """Return every raw Open-Meteo file when no region filter is provided."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            input_folder = Path(temporary_directory)
            (input_folder / "openmeteo_a_daily_sample.json").write_text("{}", encoding="utf-8")
            (input_folder / "openmeteo_b_daily_sample.json").write_text("{}", encoding="utf-8")

            with patch.object(normalize_openmeteo, "OPENMETEO_RAW_PATH", input_folder):
                input_files = normalize_openmeteo.resolve_input_files(None)

        self.assertEqual(len(input_files), 2)

    def test_resolve_input_files_raises_when_requested_region_file_is_missing(self) -> None:
        """Raise when a requested Open-Meteo region file does not exist."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            input_folder = Path(temporary_directory)

            with patch.object(normalize_openmeteo, "OPENMETEO_RAW_PATH", input_folder):
                with self.assertRaises(FileNotFoundError):
                    normalize_openmeteo.resolve_input_files(["asturias"])


class TestNormalizeReDataScript(unittest.TestCase):
    def test_extract_blocks_raises_when_included_is_not_a_list(self) -> None:
        """Raise when REData 'included' is not returned as a list."""
        with self.assertRaises(ValueError):
            normalize_redata.extract_blocks({"included": {}})

    def test_extract_metadata_reads_wrapped_payload(self) -> None:
        """Read metadata from the saved wrapper around the raw REData payload."""
        raw_json = {
            "source": "redata",
            "endpoint": "/balance",
            "region_slug": "madrid",
            "region_name": "Comunidad de Madrid",
            "redata_geo_id": 13,
            "extracted_at": "2026-01-01T00:00:00+00:00",
            "payload": {"included": []},
        }

        payload, metadata = normalize_redata.extract_metadata(raw_json, "fallback")

        self.assertEqual(payload, {"included": []})
        self.assertEqual(metadata["region_slug"], "madrid")
        self.assertEqual(metadata["redata_geo_id"], 13)

    def test_extract_metadata_falls_back_when_payload_wrapper_is_missing(self) -> None:
        """Use fallback metadata when the saved wrapper is not present."""
        raw_json = {"included": []}

        payload, metadata = normalize_redata.extract_metadata(raw_json, "fallback-ts")

        self.assertEqual(payload, raw_json)
        self.assertEqual(metadata["region_slug"], "")
        self.assertEqual(metadata["ingestion_timestamp"], "fallback-ts")

    def test_normalize_blocks_flattens_one_monthly_observation(self) -> None:
        """Explode one monthly REData observation into a flat normalized row."""
        blocks = [
            {
                "type": "Renovable",
                "id": "renovable",
                "attributes": {
                    "title": "Renovable",
                    "content": [
                        {
                            "type": "Eólica",
                            "id": "eolica",
                            "groupId": "renovable",
                            "attributes": {
                                "title": "Eólica",
                                "description": "Generación eólica",
                                "composite": False,
                                "last-update": "2026-01-01T00:00:00+00:00",
                                "total": 100.0,
                                "total-percentage": 0.4,
                                "values": [
                                    {
                                        "datetime": "2025-01-01T00:00:00.000+01:00",
                                        "value": 42.0,
                                        "percentage": 0.42,
                                    }
                                ],
                            },
                        }
                    ],
                },
            }
        ]
        metadata = {
            "source": "redata",
            "endpoint": "/balance",
            "region_slug": "madrid",
            "region_name": "Comunidad de Madrid",
            "redata_geo_id": 13,
            "ingestion_timestamp": "2026-01-01T00:00:00+00:00",
        }

        rows = normalize_redata.normalize_blocks(blocks, metadata)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["metric_type"], "Eólica")
        self.assertEqual(rows[0]["year_month"], "2025-01")
        self.assertEqual(rows[0]["value"], 42.0)

    def test_resolve_input_files_returns_all_files_when_no_filters_are_given(self) -> None:
        """Return every raw REData file when no file or region filter is passed."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            input_folder = Path(temporary_directory)
            (input_folder / "redata_a_monthly_sample.json").write_text("{}", encoding="utf-8")
            (input_folder / "redata_b_monthly_sample.json").write_text("{}", encoding="utf-8")

            with patch.object(normalize_redata, "RE_DATA_RAW_PATH", input_folder):
                input_files = normalize_redata.resolve_input_files(None, None)

        self.assertEqual(len(input_files), 2)

    def test_normalize_blocks_skips_records_without_valid_values_list(self) -> None:
        """Skip malformed REData records that do not contain a valid values list."""
        blocks = [
            {
                "type": "Renovable",
                "id": "renovable",
                "attributes": {
                    "content": [
                        {
                            "type": "Eólica",
                            "id": "eolica",
                            "groupId": "renovable",
                            "attributes": {"values": "invalid"},
                        },
                        "bad-record",
                    ]
                },
            }
        ]
        metadata = {
            "source": "redata",
            "endpoint": "/balance",
            "region_slug": "madrid",
            "region_name": "Comunidad de Madrid",
            "redata_geo_id": 13,
            "ingestion_timestamp": "2026-01-01T00:00:00+00:00",
        }

        rows = normalize_redata.normalize_blocks(blocks, metadata)

        self.assertEqual(rows, [])


if __name__ == "__main__":
    unittest.main()
