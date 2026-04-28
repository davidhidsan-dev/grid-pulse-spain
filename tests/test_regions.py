import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import regions


CSV_HEADER = (
    "region_slug,display_name,region_type,redata_geo_limit,redata_geo_id,"
    "weather_location_name,latitude,longitude,timezone,weather_point_type,is_active\n"
)


class TestRegions(unittest.TestCase):
    def test_load_regions_raises_when_reference_file_is_missing(self) -> None:
        """Raise when the regional reference CSV does not exist."""
        missing_path = PROJECT_ROOT / "tests" / "missing_regions.csv"

        with patch.object(regions, "REGIONS_FILE", missing_path):
            with self.assertRaises(FileNotFoundError):
                regions.load_regions()

    def test_load_regions_filters_inactive_and_parses_types(self) -> None:
        """Load only active regions and coerce their typed fields."""
        csv_content = (
            CSV_HEADER
            + "madrid,Comunidad de Madrid,autonomous_community,ccaa,13,Madrid,40.4,-3.7,Europe/Madrid,centroid,TRUE\n"
            + "test,Inactive Region,autonomous_community,ccaa,99,Nowhere,0.0,0.0,UTC,centroid,FALSE\n"
        )

        with tempfile.TemporaryDirectory() as temporary_directory:
            csv_path = Path(temporary_directory) / "regions.csv"
            csv_path.write_text(csv_content, encoding="utf-8")

            with patch.object(regions, "REGIONS_FILE", csv_path):
                loaded_regions = regions.load_regions()

        self.assertEqual(len(loaded_regions), 1)
        self.assertEqual(loaded_regions[0].region_slug, "madrid")
        self.assertEqual(loaded_regions[0].redata_geo_id, 13)
        self.assertEqual(loaded_regions[0].latitude, 40.4)
        self.assertTrue(loaded_regions[0].is_active)

    def test_resolve_regions_by_slugs_preserves_order(self) -> None:
        """Return selected regions in the same order requested by the user."""
        loaded_regions = [
            regions.SpanishRegion(
                region_slug="madrid",
                display_name="Comunidad de Madrid",
                region_type="autonomous_community",
                redata_geo_limit="ccaa",
                redata_geo_id=13,
                weather_location_name="Madrid",
                latitude=40.4,
                longitude=-3.7,
                timezone="Europe/Madrid",
                weather_point_type="centroid",
                is_active=True,
            ),
            regions.SpanishRegion(
                region_slug="andalucia",
                display_name="Andalucía",
                region_type="autonomous_community",
                redata_geo_limit="ccaa",
                redata_geo_id=4,
                weather_location_name="Sevilla",
                latitude=37.3,
                longitude=-5.9,
                timezone="Europe/Madrid",
                weather_point_type="centroid",
                is_active=True,
            ),
        ]

        selected_regions = regions.resolve_regions_by_slugs(
            loaded_regions, ["andalucia", "madrid"]
        )

        self.assertEqual(
            [region.region_slug for region in selected_regions],
            ["andalucia", "madrid"],
        )

    def test_resolve_regions_by_slugs_raises_for_unknown_slug(self) -> None:
        """Raise when a requested region slug is not in the reference data."""
        loaded_regions = [
            regions.SpanishRegion(
                region_slug="madrid",
                display_name="Comunidad de Madrid",
                region_type="autonomous_community",
                redata_geo_limit="ccaa",
                redata_geo_id=13,
                weather_location_name="Madrid",
                latitude=40.4,
                longitude=-3.7,
                timezone="Europe/Madrid",
                weather_point_type="centroid",
                is_active=True,
            )
        ]

        with self.assertRaises(ValueError):
            regions.resolve_regions_by_slugs(loaded_regions, ["asturias"])

    @patch("builtins.print")
    @patch("builtins.input", side_effect=["", "foo", "9", "2,2,1"])
    def test_prompt_for_regions_retries_and_deduplicates_indexes(
        self, mock_input: Mock, mock_print: Mock
    ) -> None:
        """Retry on bad selections and remove duplicate numeric choices."""
        loaded_regions = [
            regions.SpanishRegion(
                region_slug="madrid",
                display_name="Comunidad de Madrid",
                region_type="autonomous_community",
                redata_geo_limit="ccaa",
                redata_geo_id=13,
                weather_location_name="Madrid",
                latitude=40.4,
                longitude=-3.7,
                timezone="Europe/Madrid",
                weather_point_type="centroid",
                is_active=True,
            ),
            regions.SpanishRegion(
                region_slug="andalucia",
                display_name="Andalucía",
                region_type="autonomous_community",
                redata_geo_limit="ccaa",
                redata_geo_id=4,
                weather_location_name="Sevilla",
                latitude=37.3,
                longitude=-5.9,
                timezone="Europe/Madrid",
                weather_point_type="centroid",
                is_active=True,
            ),
        ]

        selected_regions = regions.prompt_for_regions(loaded_regions, "es")

        self.assertEqual(
            [region.region_slug for region in selected_regions],
            ["andalucia", "madrid"],
        )
        self.assertEqual(mock_input.call_count, 4)
        printed_messages = " ".join(
            str(call.args[0]) for call in mock_print.call_args_list if call.args
        )
        self.assertIn("vac", printed_messages.lower())
        self.assertIn("formato", printed_messages.lower())
        self.assertIn("rango", printed_messages.lower())

    @patch("builtins.print")
    @patch("builtins.input", side_effect=["all"])
    def test_prompt_for_regions_accepts_all(
        self, mock_input: Mock, mock_print: Mock
    ) -> None:
        """Return every region when the user types 'all'."""
        loaded_regions = [
            regions.SpanishRegion(
                region_slug="madrid",
                display_name="Comunidad de Madrid",
                region_type="autonomous_community",
                redata_geo_limit="ccaa",
                redata_geo_id=13,
                weather_location_name="Madrid",
                latitude=40.4,
                longitude=-3.7,
                timezone="Europe/Madrid",
                weather_point_type="centroid",
                is_active=True,
            )
        ]

        selected_regions = regions.prompt_for_regions(loaded_regions, "es")

        self.assertEqual(selected_regions, loaded_regions)


if __name__ == "__main__":
    unittest.main()
