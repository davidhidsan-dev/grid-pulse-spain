"""Streamlit app to explore curated monthly regional energy and weather relationships."""

import sys
from pathlib import Path
from typing import cast

import altair as alt
import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config.settings import BIGQUERY_DATASET_ANALYTICS, GCP_PROJECT_ID
from src.load.bigquery_loader import get_bigquery_client

TABLE_NAME = "mart_energy_weather_monthly_curated"

UI_TEXTS = {
    "es": {
        "page_title": "Energía y clima regional",
        "title": "Energía y clima regional",
        "description": (
            "Aplicación para explorar relaciones mensuales entre métricas energéticas y "
            "variables climáticas por comunidad autónoma."
        ),
        "language": "Idioma",
        "spanish": "Español",
        "english": "English",
        "filters": "Filtros",
        "region": "Comunidad",
        "metric": "Métrica energética",
        "period": "Periodo",
        "load_error": "No se pudieron cargar los datos desde BigQuery: {error}",
        "empty_table": "La tabla curada está vacía.",
        "no_data": "No hay datos para los filtros seleccionados.",
        "selected_caption": (
            "Comunidad seleccionada: {region} · Métrica: {metric} · "
            "Variable climática principal: {weather_label}"
        ),
        "months": "Meses",
        "average_value": "Valor medio",
        "average_share": "Participación media",
        "month_axis": "Mes",
        "region_tooltip": "Comunidad",
        "metric_tooltip": "Métrica",
        "energy_value": "Valor energético",
        "quick_read": "Lectura rápida",
        "group_a": "Relación climática principal",
        "group_b": "Relación climática exploratoria",
        "group_c": "Métrica de contexto",
        "evolution": "Evolución temporal",
        "energy_series": "Serie temporal del valor energético",
        "weather_series": "Serie temporal de {weather_label}",
        "energy_vs_weather": "Relación energía vs clima",
        "context_caption": (
            "Para esta métrica la variable climática se muestra solo como contexto, "
            "así que no se presenta un scatter principal de clima frente a energía."
        ),
        "summary_tables": "Tablas resumen",
        "monthly_detail": "Detalle mensual filtrado",
        "period_summary": "Resumen estadístico del periodo",
        "current_correlation": "Correlación simple del filtro actual",
        "series": "serie",
        "mean": "media",
        "min": "mínimo",
        "max": "máximo",
        "std": "desviación estándar",
        "energy_variable": "variable_energética",
        "weather_variable": "variable_climática",
        "pearson": "correlación_pearson",
        "year_month": "Año-mes",
    },
    "en": {
        "page_title": "Regional energy and weather",
        "title": "Regional energy and weather",
        "description": (
            "App to explore monthly relationships between energy metrics and weather "
            "variables by autonomous community."
        ),
        "language": "Language",
        "spanish": "Español",
        "english": "English",
        "filters": "Filters",
        "region": "Region",
        "metric": "Energy metric",
        "period": "Period",
        "load_error": "Could not load data from BigQuery: {error}",
        "empty_table": "The curated table is empty.",
        "no_data": "No data is available for the selected filters.",
        "selected_caption": (
            "Selected region: {region} · Metric: {metric} · "
            "Primary weather variable: {weather_label}"
        ),
        "months": "Months",
        "average_value": "Average value",
        "average_share": "Average share",
        "month_axis": "Month",
        "region_tooltip": "Region",
        "metric_tooltip": "Metric",
        "energy_value": "Energy value",
        "quick_read": "Quick read",
        "group_a": "Primary weather relationship",
        "group_b": "Exploratory weather relationship",
        "group_c": "System context metric",
        "evolution": "Time evolution",
        "energy_series": "Energy value time series",
        "weather_series": "{weather_label} time series",
        "energy_vs_weather": "Energy vs weather relationship",
        "context_caption": (
            "For this metric the weather variable is shown only as context, so the app "
            "does not display a main climate-versus-energy scatter plot."
        ),
        "summary_tables": "Summary tables",
        "monthly_detail": "Filtered monthly detail",
        "period_summary": "Period summary statistics",
        "current_correlation": "Simple correlation for the current filter",
        "series": "series",
        "mean": "mean",
        "min": "min",
        "max": "max",
        "std": "standard deviation",
        "energy_variable": "energy_variable",
        "weather_variable": "weather_variable",
        "pearson": "pearson_correlation",
        "year_month": "Year-month",
    },
}

METRIC_LABELS = {
    "Demanda en b.c.": {"es": "Demanda en b.c.", "en": "Demand at busbars"},
    "Generación renovable": {"es": "Generación renovable", "en": "Renewable generation"},
    "Generación no renovable": {"es": "Generación no renovable", "en": "Non-renewable generation"},
    "Eólica": {"es": "Eólica", "en": "Wind"},
    "Solar fotovoltaica": {"es": "Solar fotovoltaica", "en": "Solar photovoltaic"},
    "Solar térmica": {"es": "Solar térmica", "en": "Solar thermal"},
    "Hidráulica": {"es": "Hidráulica", "en": "Hydro"},
    "Ciclo combinado": {"es": "Ciclo combinado", "en": "Combined cycle"},
    "Cogeneración": {"es": "Cogeneración", "en": "Cogeneration"},
}

RELATIONSHIP_CONFIG = {
    "Demanda en b.c.": {
        "group": "A",
        "weather_column": "temperature_2m_mean_avg",
        "weather_label": {"es": "Temperatura media", "en": "Average temperature"},
        "insight_title": {
            "es": "Relación razonable con temperatura media",
            "en": "Reasonable relationship with average temperature",
        },
        "insight_text": {
            "es": (
                "La demanda eléctrica puede variar con la temperatura media mensual, "
                "sobre todo en periodos de mayor uso de climatización."
            ),
            "en": (
                "Electricity demand can vary with monthly average temperature, "
                "especially in periods with heavier heating or cooling use."
            ),
        },
        "show_scatter": True,
    },
    "Generación renovable": {
        "group": "C",
        "weather_column": "shortwave_radiation_sum_total",
        "weather_label": {"es": "Radiación solar total", "en": "Total solar radiation"},
        "insight_title": {
            "es": "Métrica de contexto del sistema",
            "en": "System context metric",
        },
        "insight_text": {
            "es": (
                "Esta métrica agrega varias tecnologías renovables, así que la relación "
                "con una sola variable climática debe leerse solo como contexto del periodo."
            ),
            "en": (
                "This metric aggregates several renewable technologies, so any link "
                "with a single weather variable should be read only as period context."
            ),
        },
        "show_scatter": False,
    },
    "Generación no renovable": {
        "group": "C",
        "weather_column": "temperature_2m_mean_avg",
        "weather_label": {"es": "Temperatura media", "en": "Average temperature"},
        "insight_title": {
            "es": "Métrica de contexto del sistema",
            "en": "System context metric",
        },
        "insight_text": {
            "es": (
                "Esta métrica agrega varias tecnologías no renovables, por lo que la "
                "señal climática se muestra solo como contexto y no como relación directa."
            ),
            "en": (
                "This metric aggregates several non-renewable technologies, so the weather "
                "signal is shown only as context and not as a direct relationship."
            ),
        },
        "show_scatter": False,
    },
    "Eólica": {
        "group": "A",
        "weather_column": "wind_speed_10m_max_avg",
        "weather_label": {"es": "Viento máximo medio", "en": "Average maximum wind speed"},
        "insight_title": {
            "es": "Relación razonable con velocidad del viento",
            "en": "Reasonable relationship with wind speed",
        },
        "insight_text": {
            "es": (
                "La generación eólica puede mostrar una relación visible con la velocidad "
                "del viento, aunque a nivel mensual no siempre será perfectamente lineal."
            ),
            "en": (
                "Wind generation can show a visible relationship with wind speed, "
                "although at monthly level it will not always be perfectly linear."
            ),
        },
        "show_scatter": True,
    },
    "Solar fotovoltaica": {
        "group": "A",
        "weather_column": "shortwave_radiation_sum_total",
        "weather_label": {"es": "Radiación total", "en": "Total radiation"},
        "insight_title": {
            "es": "Relación razonable con radiación solar",
            "en": "Reasonable relationship with solar radiation",
        },
        "insight_text": {
            "es": (
                "La generación solar fotovoltaica suele alinearse con la radiación solar "
                "acumulada del periodo."
            ),
            "en": (
                "Solar photovoltaic generation often aligns with accumulated solar "
                "radiation over the period."
            ),
        },
        "show_scatter": True,
    },
    "Solar térmica": {
        "group": "A",
        "weather_column": "shortwave_radiation_sum_total",
        "weather_label": {"es": "Radiación total", "en": "Total radiation"},
        "insight_title": {
            "es": "Relación razonable con radiación solar",
            "en": "Reasonable relationship with solar radiation",
        },
        "insight_text": {
            "es": (
                "La generación solar térmica puede relacionarse con la radiación solar "
                "acumulada, aunque el comportamiento operativo puede introducir variabilidad."
            ),
            "en": (
                "Solar thermal generation can relate to accumulated solar radiation, "
                "although operational behavior may introduce variability."
            ),
        },
        "show_scatter": True,
    },
    "Hidráulica": {
        "group": "B",
        "weather_column": "precipitation_sum_total",
        "weather_label": {"es": "Precipitación total", "en": "Total precipitation"},
        "insight_title": {
            "es": "Relación exploratoria con precipitación acumulada",
            "en": "Exploratory relationship with accumulated precipitation",
        },
        "insight_text": {
            "es": (
                "La generación hidráulica puede mostrar relación con la precipitación, "
                "pero no debe interpretarse como causalidad inmediata mes a mes ni como "
                "respuesta directa dentro del mismo periodo."
            ),
            "en": (
                "Hydro generation can show a relationship with precipitation, but it "
                "should not be interpreted as immediate month-to-month causality or as "
                "a direct response within the same period."
            ),
        },
        "show_scatter": True,
    },
    "Ciclo combinado": {
        "group": "C",
        "weather_column": "temperature_2m_mean_avg",
        "weather_label": {"es": "Temperatura media", "en": "Average temperature"},
        "insight_title": {
            "es": "Métrica de contexto del sistema",
            "en": "System context metric",
        },
        "insight_text": {
            "es": (
                "El ciclo combinado puede responder de forma indirecta a cambios en demanda "
                "o en menor producción renovable, no a una señal climática directa."
            ),
            "en": (
                "Combined cycle generation may respond indirectly to changes in demand "
                "or lower renewable output, not to a direct weather signal."
            ),
        },
        "show_scatter": False,
    },
    "Cogeneración": {
        "group": "C",
        "weather_column": "temperature_2m_mean_avg",
        "weather_label": {"es": "Temperatura media", "en": "Average temperature"},
        "insight_title": {
            "es": "Métrica de contexto del sistema",
            "en": "System context metric",
        },
        "insight_text": {
            "es": (
                "La cogeneración puede reflejar patrones de consumo industrial y térmico, "
                "por lo que la señal climática debe leerse solo como contexto."
            ),
            "en": (
                "Cogeneration may reflect industrial and thermal consumption patterns, "
                "so the weather signal should be read only as context."
            ),
        },
        "show_scatter": False,
    },
}


def t(language: str, key: str, **kwargs: object) -> str:
    """Translate one UI string."""
    return UI_TEXTS[language][key].format(**kwargs)


def get_metric_label(metric_type: str, language: str) -> str:
    """Return a translated label for one energy metric."""
    return METRIC_LABELS.get(metric_type, {}).get(language, metric_type)


def get_metric_label_safe(metric_type: object, language: str) -> str:
    """Return a translated metric label while handling nullable pandas values."""
    if isinstance(metric_type, str):
        return get_metric_label(metric_type, language)
    return ""


@st.cache_data(ttl=3600)
def load_data() -> pd.DataFrame:
    """Read the curated monthly mart from BigQuery into a pandas DataFrame."""
    if not GCP_PROJECT_ID:
        raise ValueError("Missing GCP_PROJECT_ID in environment configuration.")

    table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET_ANALYTICS}.{TABLE_NAME}"
    query = f"""
        select
            region_slug,
            region_name,
            year_month,
            group_type,
            metric_type,
            value,
            percentage,
            temperature_2m_max_avg,
            temperature_2m_mean_avg,
            temperature_2m_min_avg,
            precipitation_sum_total,
            wind_speed_10m_max_avg,
            shortwave_radiation_sum_total
        from `{table_id}`
        order by region_name, year_month, metric_type
    """

    client = get_bigquery_client()
    dataframe = client.query(query).to_dataframe()
    dataframe["year_month"] = pd.to_datetime(dataframe["year_month"] + "-01")
    return dataframe


def build_sidebar_filters(
    dataframe: pd.DataFrame,
    language: str,
) -> tuple[str, str, tuple[pd.Timestamp, pd.Timestamp]]:
    """Create the main sidebar filters used by the app."""
    st.sidebar.header(t(language, "filters"))

    region_options = sorted(dataframe["region_name"].dropna().unique().tolist())
    default_region_index = (
        region_options.index("Comunidad de Madrid")
        if "Comunidad de Madrid" in region_options
        else 0
    )
    selected_region = cast(
        str,
        st.sidebar.selectbox(t(language, "region"), region_options, index=default_region_index),
    )

    metric_options = sorted(dataframe["metric_type"].dropna().unique().tolist())
    default_metric_index = (
        metric_options.index("Demanda en b.c.")
        if "Demanda en b.c." in metric_options
        else 0
    )
    metric_display_map = {metric: get_metric_label(metric, language) for metric in metric_options}
    selected_metric = cast(
        str,
        st.sidebar.selectbox(
            t(language, "metric"),
            metric_options,
            index=default_metric_index,
            format_func=lambda metric: metric_display_map[metric],
        ),
    )

    month_options = sorted(dataframe["year_month"].dropna().unique().tolist())
    selected_period = st.sidebar.select_slider(
        t(language, "period"),
        options=month_options,
        value=(month_options[0], month_options[-1]),
        format_func=lambda value: value.strftime("%Y-%m"),
    )

    return selected_region, selected_metric, selected_period


def filter_data(
    dataframe: pd.DataFrame,
    selected_region: str,
    selected_metric: str,
    selected_period: tuple[pd.Timestamp, pd.Timestamp],
) -> pd.DataFrame:
    """Apply the sidebar filters to the full curated mart data."""
    start_month, end_month = selected_period
    return dataframe[
        (dataframe["region_name"] == selected_region)
        & (dataframe["metric_type"] == selected_metric)
        & (dataframe["year_month"] >= start_month)
        & (dataframe["year_month"] <= end_month)
    ].copy()


def get_relationship_config(metric_type: str) -> dict[str, object]:
    """Return the configured weather relationship for the selected metric."""
    return RELATIONSHIP_CONFIG.get(
        metric_type,
        {
            "group": "C",
            "weather_column": "temperature_2m_mean_avg",
            "weather_label": {"es": "Temperatura media", "en": "Average temperature"},
            "insight_title": {
                "es": "Métrica de contexto del sistema",
                "en": "System context metric",
            },
            "insight_text": {
                "es": "No hay una relación climática directa configurada para esta métrica.",
                "en": "There is no direct weather relationship configured for this metric.",
            },
            "show_scatter": False,
        },
    )


def calculate_percentage_kpi(dataframe: pd.DataFrame) -> tuple[str, str] | None:
    """Prepare a percentage KPI only when the filtered slice contains usable values."""
    percentage_series = dataframe["percentage"].dropna()
    if percentage_series.empty:
        return None

    mean_value = percentage_series.mean()
    if percentage_series.abs().max() <= 1:
        mean_value *= 100

    return "percentage", f"{mean_value:,.1f}%"


def render_kpis(
    dataframe: pd.DataFrame,
    weather_column: str,
    weather_label: str,
    language: str,
) -> None:
    """Show a compact set of KPIs for the current filtered slice."""
    percentage_kpi = calculate_percentage_kpi(dataframe)
    num_columns = 4 if percentage_kpi else 3
    columns = st.columns(num_columns)
    col1, col2, col3 = columns[:3]
    col1.metric(t(language, "months"), dataframe["year_month"].nunique())
    col2.metric(t(language, "average_value"), f"{dataframe['value'].mean():,.1f}")
    col3.metric(weather_label, f"{dataframe[weather_column].mean():,.1f}")
    if percentage_kpi:
        _, value = percentage_kpi
        columns[3].metric(t(language, "average_share"), value)


def build_line_chart(
    dataframe: pd.DataFrame,
    value_column: str,
    y_axis_title: str,
    language: str,
) -> alt.Chart:
    """Build a labeled line chart for the filtered region."""
    return (
        alt.Chart(dataframe)
        .mark_line(point=True)
        .encode(
            x=alt.X("year_month:T", title=t(language, "month_axis")),
            y=alt.Y(f"{value_column}:Q", title=y_axis_title),
            tooltip=[
                alt.Tooltip("region_name:N", title=t(language, "region_tooltip")),
                alt.Tooltip("year_month:T", title=t(language, "month_axis")),
                alt.Tooltip(f"{value_column}:Q", title=y_axis_title, format=",.2f"),
            ],
        )
    )


def build_scatter_chart(
    dataframe: pd.DataFrame,
    weather_column: str,
    weather_label: str,
    language: str,
) -> alt.Chart:
    """Build a labeled scatter chart for the filtered energy and weather relationship."""
    return (
        alt.Chart(dataframe)
        .mark_circle(size=90, color="#1f77b4")
        .encode(
            x=alt.X(f"{weather_column}:Q", title=weather_label),
            y=alt.Y("value:Q", title=t(language, "energy_value")),
            tooltip=[
                alt.Tooltip("region_name:N", title=t(language, "region_tooltip")),
                alt.Tooltip("year_month:T", title=t(language, "month_axis")),
                alt.Tooltip("metric_type:N", title=t(language, "metric_tooltip")),
                alt.Tooltip(f"{weather_column}:Q", title=weather_label, format=",.2f"),
                alt.Tooltip("value:Q", title=t(language, "energy_value"), format=",.2f"),
            ],
        )
    )


def build_monthly_detail_table(
    dataframe: pd.DataFrame,
    weather_column: str,
    weather_label: str,
    language: str,
) -> pd.DataFrame:
    """Build the monthly detail table used in the summary section."""
    table_df = dataframe[
        ["year_month", "region_name", "metric_type", "value", weather_column]
    ].copy()
    table_df["year_month"] = table_df["year_month"].dt.strftime("%Y-%m")
    table_df["metric_type"] = table_df["metric_type"].map(
        lambda value: get_metric_label_safe(value, language)
    )
    table_df = table_df.rename(
        columns={
            "year_month": t(language, "year_month"),
            "region_name": t(language, "region"),
            "metric_type": t(language, "metric"),
            "value": t(language, "energy_value"),
            weather_column: weather_label,
        }
    )
    return table_df


def build_summary_stats_table(
    dataframe: pd.DataFrame,
    weather_column: str,
    weather_label: str,
    language: str,
) -> pd.DataFrame:
    """Build a small summary table with basic descriptive statistics."""
    summary = pd.DataFrame(
        {
            t(language, "mean"): [dataframe["value"].mean(), dataframe[weather_column].mean()],
            t(language, "min"): [dataframe["value"].min(), dataframe[weather_column].min()],
            t(language, "max"): [dataframe["value"].max(), dataframe[weather_column].max()],
            t(language, "std"): [dataframe["value"].std(), dataframe[weather_column].std()],
        },
        index=[t(language, "energy_value"), weather_label],
    )
    return summary.reset_index(names=t(language, "series"))


def build_correlation_table(
    dataframe: pd.DataFrame,
    weather_column: str,
    weather_label: str,
    language: str,
) -> pd.DataFrame:
    """Build a simple correlation table for the current filter."""
    correlation = dataframe["value"].corr(dataframe[weather_column])
    return pd.DataFrame(
        {
            t(language, "energy_variable"): [t(language, "energy_value")],
            t(language, "weather_variable"): [weather_label],
            t(language, "pearson"): [correlation],
        }
    )


def render_insight(
    metric_label: str,
    weather_label: str,
    insight_title: str,
    insight_text: str,
    relationship_group: str,
    language: str,
) -> None:
    """Render a small explanatory block for the selected relationship."""
    st.subheader(t(language, "quick_read"))
    prefix = {
        "A": t(language, "group_a"),
        "B": t(language, "group_b"),
        "C": t(language, "group_c"),
    }.get(relationship_group, t(language, "quick_read"))
    st.info(
        f"**{prefix}:** **{metric_label}** -> **{weather_label}**\n\n"
        f"**{insight_title}.** {insight_text}"
    )


def main() -> None:
    language_labels = {"es": UI_TEXTS["es"]["spanish"], "en": UI_TEXTS["en"]["english"]}
    default_language = st.session_state.get("app_language", "es")

    st.set_page_config(page_title=t(default_language, "page_title"), layout="wide")

    st.sidebar.selectbox(
        UI_TEXTS[default_language]["language"],
        options=["es", "en"],
        index=0 if default_language == "es" else 1,
        format_func=lambda value: language_labels[value],
        key="app_language",
    )
    language = cast(str, st.session_state["app_language"])

    st.title(t(language, "title"))
    st.write(t(language, "description"))

    try:
        dataframe = load_data()
    except Exception as error:
        st.error(t(language, "load_error", error=error))
        st.stop()

    if dataframe.empty:
        st.warning(t(language, "empty_table"))
        st.stop()

    selected_region, selected_metric, selected_period = build_sidebar_filters(dataframe, language)
    filtered_df = filter_data(dataframe, selected_region, selected_metric, selected_period)

    if filtered_df.empty:
        st.warning(t(language, "no_data"))
        st.stop()

    relationship_config = get_relationship_config(selected_metric)
    weather_column = cast(str, relationship_config["weather_column"])
    weather_label = cast(dict[str, str], relationship_config["weather_label"])[language]
    insight_title = cast(dict[str, str], relationship_config["insight_title"])[language]
    insight_text = cast(dict[str, str], relationship_config["insight_text"])[language]
    show_scatter = cast(bool, relationship_config["show_scatter"])
    relationship_group = cast(str, relationship_config["group"])
    metric_label = get_metric_label(selected_metric, language)

    st.caption(
        t(
            language,
            "selected_caption",
            region=selected_region,
            metric=metric_label,
            weather_label=weather_label,
        )
    )

    render_kpis(filtered_df, weather_column, weather_label, language)
    render_insight(
        metric_label,
        weather_label,
        insight_title,
        insight_text,
        relationship_group,
        language,
    )

    st.subheader(t(language, "evolution"))
    col1, col2 = st.columns(2)
    with col1:
        st.markdown(f"**{t(language, 'energy_series')}**")
        st.altair_chart(
            build_line_chart(filtered_df, "value", t(language, "energy_value"), language),
            use_container_width=True,
        )
    with col2:
        st.markdown(f"**{t(language, 'weather_series', weather_label=weather_label.lower())}**")
        st.altair_chart(
            build_line_chart(filtered_df, weather_column, weather_label, language),
            use_container_width=True,
        )

    st.subheader(t(language, "energy_vs_weather"))
    if show_scatter:
        st.altair_chart(
            build_scatter_chart(filtered_df, weather_column, weather_label, language),
            use_container_width=True,
        )
    else:
        st.caption(t(language, "context_caption"))

    st.subheader(t(language, "summary_tables"))
    summary_col1, summary_col2 = st.columns(2)
    with summary_col1:
        st.markdown(f"**{t(language, 'monthly_detail')}**")
        st.dataframe(
            build_monthly_detail_table(filtered_df, weather_column, weather_label, language),
            use_container_width=True,
        )
    with summary_col2:
        st.markdown(f"**{t(language, 'period_summary')}**")
        st.dataframe(
            build_summary_stats_table(filtered_df, weather_column, weather_label, language),
            use_container_width=True,
        )

    st.markdown(f"**{t(language, 'current_correlation')}**")
    st.dataframe(
        build_correlation_table(filtered_df, weather_column, weather_label, language),
        use_container_width=True,
    )


if __name__ == "__main__":
    main()
