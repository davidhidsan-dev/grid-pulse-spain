import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils import terminal_ui


class TestTerminalUi(unittest.TestCase):
    def test_translate_formats_values(self) -> None:
        """Format placeholders correctly in translated messages."""
        message = terminal_ui.translate("es", "year_defaults", start=2015, end=2025)
        self.assertIn("2015-2025", message)

    @patch("builtins.print")
    @patch("builtins.input", side_effect=["", ""])
    def test_prompt_for_year_range_accepts_defaults(
        self, mock_input: Mock, mock_print: Mock
    ) -> None:
        """Return the default years when the user presses Enter twice."""
        start_year, end_year = terminal_ui.prompt_for_year_range("es", 2015, 2025)

        self.assertEqual((start_year, end_year), (2015, 2025))
        self.assertEqual(mock_input.call_count, 2)
        self.assertGreater(mock_print.call_count, 0)

    @patch("builtins.print")
    @patch("builtins.input", side_effect=["2025", "2020", "2019", "2020"])
    def test_prompt_for_year_range_retries_when_order_is_invalid(
        self, mock_input: Mock, mock_print: Mock
    ) -> None:
        """Retry when the start year is greater than the end year."""
        start_year, end_year = terminal_ui.prompt_for_year_range("es", 2015, 2025)

        self.assertEqual((start_year, end_year), (2019, 2020))
        printed_messages = " ".join(str(call.args[0]) for call in mock_print.call_args_list if call.args)
        self.assertIn("no puede ser mayor", printed_messages)

    @patch("builtins.print")
    @patch("builtins.input", side_effect=["abcd", "2020", "2018", "2018"])
    def test_prompt_for_year_range_retries_when_year_is_not_numeric(
        self, mock_input: Mock, mock_print: Mock
    ) -> None:
        """Retry when one of the year inputs is not numeric."""
        start_year, end_year = terminal_ui.prompt_for_year_range("es", 2015, 2025)

        self.assertEqual((start_year, end_year), (2018, 2018))
        self.assertEqual(mock_input.call_count, 4)
        printed_messages = " ".join(
            str(call.args[0]) for call in mock_print.call_args_list if call.args
        )
        self.assertIn("4", printed_messages)

    def test_translate_raises_for_unknown_language(self) -> None:
        """Raise a KeyError when the requested language is not defined."""
        with self.assertRaises(KeyError):
            terminal_ui.translate("fr", "year_title")

    @patch("builtins.print")
    @patch("builtins.input", side_effect=["foo", "2"])
    def test_prompt_for_language_retries_when_selection_is_invalid(
        self, mock_input: Mock, mock_print: Mock
    ) -> None:
        """Retry until the user selects a valid language option."""
        language = terminal_ui.prompt_for_language()

        self.assertEqual(language, "en")
        printed_messages = " ".join(str(call.args[0]) for call in mock_print.call_args_list if call.args)
        self.assertIn("no válida", printed_messages)


if __name__ == "__main__":
    unittest.main()
