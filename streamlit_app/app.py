"""Small Streamlit app to explore monthly regional energy and weather data."""

import sys
from pathlib import Path
from typing import cast

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config.settings import BIGQUERY_DATASET_ANALYTICS, GCP_PROJECT_ID
from src.load.bigquery_loader import get_bigquery_client

TABLE_NAME = "mart_energy_weather_monthly"


@st.cache_data(ttl=3600)
def load_data() -> pd.DataFrame:
    """Read the final monthly mart from BigQuery into a pandas DataFrame."""
    if not GCP_PROJECT_ID:
        raise ValueError("Missing GCP_PROJECT_ID in environment configuration.")

    table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET_ANALYTICS}.{TABLE_NAME}"
    query = f"""
        select
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
        order by year_month, group_type, metric_type
    """

    client = get_bigquery_client()
    dataframe = client.query(query).to_dataframe()
    dataframe["year_month"] = pd.to_datetime(dataframe["year_month"] + "-01")
    return dataframe


def build_sidebar_filters(dataframe: pd.DataFrame) -> tuple[str, tuple[pd.Timestamp, pd.Timestamp]]:
    """Create the small set of sidebar filters used by the app."""
    st.sidebar.header("Filtros")

    metric_options = sorted(dataframe["metric_type"].dropna().unique().tolist())
    selected_metric = cast(str, st.sidebar.selectbox("Metric type", metric_options))

    month_options = sorted(dataframe["year_month"].dropna().unique().tolist())
    selected_period = st.sidebar.select_slider(
        "Periodo",
        options=month_options,
        value=(month_options[0], month_options[-1]),
        format_func=lambda value: value.strftime("%Y-%m"),
    )

    return selected_metric, selected_period


def filter_data(
    dataframe: pd.DataFrame,
    selected_metric: str,
    selected_period: tuple[pd.Timestamp, pd.Timestamp],
) -> pd.DataFrame:
    """Apply the sidebar filters to the full mart data."""
    start_month, end_month = selected_period
    return dataframe[
        (dataframe["metric_type"] == selected_metric)
        & (dataframe["year_month"] >= start_month)
        & (dataframe["year_month"] <= end_month)
    ].copy()


def render_kpis(dataframe: pd.DataFrame) -> None:
    """Show a few small KPIs for the current filtered slice."""
    col1, col2, col3 = st.columns(3)
    col1.metric("Meses", len(dataframe))
    col2.metric("Valor medio", f"{dataframe['value'].mean():,.1f}")
    col3.metric("Temp media", f"{dataframe['temperature_2m_mean_avg'].mean():.1f} °C")


def main() -> None:
    st.set_page_config(page_title="Regional Energy + Weather", layout="wide")

    st.title("Regional Energy + Weather")
    st.write(
        "Primera version de la app para explorar la relacion entre el balance electrico "
        "regional y el clima mensual de Open-Meteo."
    )

    try:
        dataframe = load_data()
    except Exception as error:
        st.error(f"No se pudieron cargar los datos desde BigQuery: {error}")
        st.stop()

    selected_metric, selected_period = build_sidebar_filters(dataframe)
    filtered_df = filter_data(dataframe, selected_metric, selected_period)

    if filtered_df.empty:
        st.warning("No hay datos para los filtros seleccionados.")
        st.stop()

    render_kpis(filtered_df)

    st.subheader("Serie temporal de energia")
    st.line_chart(filtered_df.set_index("year_month")["value"])

    st.subheader("Serie temporal de temperatura media")
    climate_df = filtered_df[["year_month", "temperature_2m_mean_avg"]].drop_duplicates()
    st.line_chart(climate_df.set_index("year_month")["temperature_2m_mean_avg"])

    st.subheader("Relacion entre energia y temperatura")
    scatter_df = filtered_df.rename(
        columns={
            "value": "Valor energia",
            "temperature_2m_mean_avg": "Temperatura media",
        }
    )
    st.scatter_chart(scatter_df, x="Temperatura media", y="Valor energia")

    st.subheader("Datos filtrados")
    table_df = filtered_df.copy()
    table_df["year_month"] = table_df["year_month"].dt.strftime("%Y-%m")
    st.dataframe(table_df, use_container_width=True)


if __name__ == "__main__":
    main()
