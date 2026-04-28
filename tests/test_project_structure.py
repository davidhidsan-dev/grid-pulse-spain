import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class TestProjectStructure(unittest.TestCase):
    def test_expected_top_level_paths_exist(self) -> None:
        """Ensure the repository keeps its expected top-level folders and files."""
        expected_paths = [
            PROJECT_ROOT / "src",
            PROJECT_ROOT / "scripts",
            PROJECT_ROOT / "dbt_project",
            PROJECT_ROOT / "streamlit_app",
            PROJECT_ROOT / "tests",
            PROJECT_ROOT / "requirements.txt",
        ]

        for expected_path in expected_paths:
            with self.subTest(path=str(expected_path)):
                self.assertTrue(expected_path.exists())

    def test_expected_pipeline_files_exist(self) -> None:
        """Ensure the key pipeline entrypoints and config files still exist."""
        expected_files = [
            PROJECT_ROOT / "scripts" / "run_pipeline.py",
            PROJECT_ROOT / "scripts" / "run_ingestion.py",
            PROJECT_ROOT / "scripts" / "run_openmeteo.py",
            PROJECT_ROOT / "streamlit_app" / "app.py",
            PROJECT_ROOT / "dbt_project" / "dbt_project.yml",
            PROJECT_ROOT / ".github" / "workflows" / "ci.yml",
            PROJECT_ROOT / "data" / "reference" / "spanish_regions.csv",
        ]

        for expected_file in expected_files:
            with self.subTest(path=str(expected_file)):
                self.assertTrue(expected_file.is_file())


if __name__ == "__main__":
    unittest.main()
