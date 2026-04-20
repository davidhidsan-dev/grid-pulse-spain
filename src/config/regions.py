"""Regional reference helpers shared by extraction scripts."""

import csv
from dataclasses import dataclass

from src.config.settings import REFERENCE_DATA_PATH
from src.utils.terminal_ui import translate

REGIONS_FILE = REFERENCE_DATA_PATH / "spanish_regions.csv"


@dataclass(frozen=True)
class SpanishRegion:
    """Reference row for one autonomous community or autonomous city."""

    region_slug: str
    display_name: str
    region_type: str
    redata_geo_limit: str
    redata_geo_id: int
    weather_location_name: str
    latitude: float
    longitude: float
    timezone: str
    weather_point_type: str
    is_active: bool


def load_regions() -> list[SpanishRegion]:
    """Load active regions from the CSV reference table."""
    if not REGIONS_FILE.exists():
        raise FileNotFoundError(f"Regional reference file not found: {REGIONS_FILE}")

    regions: list[SpanishRegion] = []
    with REGIONS_FILE.open("r", encoding="utf-8", newline="") as handler:
        reader = csv.DictReader(handler)
        for row in reader:
            is_active = row["is_active"].strip().lower() == "true"
            if not is_active:
                continue

            regions.append(
                SpanishRegion(
                    region_slug=row["region_slug"],
                    display_name=row["display_name"],
                    region_type=row["region_type"],
                    redata_geo_limit=row["redata_geo_limit"],
                    redata_geo_id=int(row["redata_geo_id"]),
                    weather_location_name=row["weather_location_name"],
                    latitude=float(row["latitude"]),
                    longitude=float(row["longitude"]),
                    timezone=row["timezone"],
                    weather_point_type=row["weather_point_type"],
                    is_active=is_active,
                )
            )

    return regions


def resolve_regions_by_slugs(
    regions: list[SpanishRegion], region_slugs: list[str]
) -> list[SpanishRegion]:
    """Resolve a list of region slugs against the loaded reference rows."""
    regions_by_slug = {region.region_slug: region for region in regions}
    selected_regions: list[SpanishRegion] = []

    for region_slug in region_slugs:
        try:
            selected_regions.append(regions_by_slug[region_slug])
        except KeyError as error:
            raise ValueError(f"Unknown region slug: {region_slug}") from error

    return selected_regions


def prompt_for_regions(regions: list[SpanishRegion], language: str) -> list[SpanishRegion]:
    """Show a numbered menu and return the selected regions."""
    print(translate(language, "regions_title"))
    print(translate(language, "regions_help"))
    print(translate(language, "regions_example"))
    selection_prompt = translate(language, "regions_prompt")
    empty_message = translate(language, "regions_empty")
    invalid_message = translate(language, "regions_invalid")
    range_message = translate(language, "regions_range", count=len(regions))

    for index, region in enumerate(regions, start=1):
        print(f"{index}. {region.display_name}")

    print()

    while True:
        raw_selection = input(selection_prompt).strip().lower()
        if raw_selection == "all":
            return regions

        if not raw_selection:
            print(empty_message)
            continue

        try:
            indexes = [int(item.strip()) for item in raw_selection.split(",")]
        except ValueError:
            print(invalid_message)
            continue

        if not indexes:
            print(empty_message)
            continue

        if any(index < 1 or index > len(regions) for index in indexes):
            print(range_message)
            continue

        selected_regions: list[SpanishRegion] = []
        seen_slugs: set[str] = set()
        for index in indexes:
            region = regions[index - 1]
            if region.region_slug not in seen_slugs:
                selected_regions.append(region)
                seen_slugs.add(region.region_slug)

        return selected_regions
