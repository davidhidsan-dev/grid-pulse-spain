"""Small terminal prompts shared by the extraction scripts."""

MESSAGES = {
    "es": {
        "language_title": "Selecciona el idioma / Select language:",
        "language_option_es": "1. Espanol",
        "language_option_en": "2. English",
        "language_example": "Ejemplo: 1",
        "language_prompt": "Idioma: ",
        "language_invalid": "Seleccion no valida. Escribe 1, 2, es o en.",
        "year_title": "Selecciona el rango de anos a extraer.",
        "year_defaults": "Pulsa Enter para usar los valores por defecto: {start}-{end}",
        "year_single": "Si quieres solo un ano, escribe el mismo ano en inicio y fin.",
        "year_example": "Ejemplo: 2020 y 2020",
        "year_start_prompt": "Ano inicial [{default}]: ",
        "year_end_prompt": "Ano final [{default}]: ",
        "year_invalid_order": "El ano inicial no puede ser mayor que el ano final.",
        "regions_title": "Selecciona una o varias comunidades autonomas:",
        "regions_help": "Escribe numeros separados por comas o escribe 'all' para todas.",
        "regions_example": "Ejemplo: 1,3,5",
        "regions_prompt": "Seleccion: ",
        "regions_empty": "La seleccion no puede estar vacia. Ejemplo: 1,3,5 o all",
        "regions_invalid": "Formato no valido. Usa numeros separados por comas o all.",
        "regions_range": "Seleccion fuera de rango. Elige numeros entre 1 y {count}.",
        "saved_redata": "Respuesta de REData guardada en {path}",
        "saved_openmeteo": "Respuesta de Open-Meteo guardada en {path}",
    },
    "en": {
        "language_title": "Select language / Selecciona el idioma:",
        "language_option_es": "1. Espanol",
        "language_option_en": "2. English",
        "language_example": "Example: 2",
        "language_prompt": "Language: ",
        "language_invalid": "Invalid selection. Type 1, 2, es or en.",
        "year_title": "Select the year range to extract.",
        "year_defaults": "Press Enter to use the default values: {start}-{end}",
        "year_single": "If you want only one year, use the same year for start and end.",
        "year_example": "Example: 2020 and 2020",
        "year_start_prompt": "Start year [{default}]: ",
        "year_end_prompt": "End year [{default}]: ",
        "year_invalid_order": "Start year cannot be greater than end year.",
        "regions_title": "Select one or more autonomous communities:",
        "regions_help": "Type numbers separated by commas or type 'all' for all of them.",
        "regions_example": "Example: 1,3,5",
        "regions_prompt": "Selection: ",
        "regions_empty": "Selection cannot be empty. Example: 1,3,5 or all",
        "regions_invalid": "Invalid format. Use numbers separated by commas or all.",
        "regions_range": "Selection out of range. Choose numbers between 1 and {count}.",
        "saved_redata": "Saved REData response to {path}",
        "saved_openmeteo": "Saved Open-Meteo response to {path}",
    },
}


def translate(language: str, key: str, **kwargs: object) -> str:
    """Return one terminal message in the selected language."""
    template = MESSAGES[language][key]
    return template.format(**kwargs)


def prompt_for_language() -> str:
    """Ask the user which language to use for terminal prompts."""
    while True:
        print(translate("es", "language_title"))
        print(translate("es", "language_option_es"))
        print(translate("es", "language_option_en"))
        print(translate("es", "language_example"))

        raw_language = input(translate("es", "language_prompt")).strip().lower()
        if raw_language in {"1", "es", "esp", "espanol"}:
            return "es"
        if raw_language in {"2", "en", "eng", "english"}:
            return "en"

        print(translate("es", "language_invalid"))
        print()


def prompt_for_year_range(
    language: str, default_start_year: int, default_end_year: int
) -> tuple[int, int]:
    """Ask for a start and end year, allowing a single-year range."""
    print()
    print(translate(language, "year_title"))
    print(
        translate(
            language,
            "year_defaults",
            start=default_start_year,
            end=default_end_year,
        )
    )
    print(translate(language, "year_single"))
    print(translate(language, "year_example"))

    start_raw = input(
        translate(language, "year_start_prompt", default=default_start_year)
    ).strip()
    end_raw = input(
        translate(language, "year_end_prompt", default=default_end_year)
    ).strip()

    start_year = int(start_raw) if start_raw else default_start_year
    end_year = int(end_raw) if end_raw else default_end_year

    if start_year > end_year:
        raise ValueError(translate(language, "year_invalid_order"))

    return start_year, end_year
