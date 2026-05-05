# grid-pulse-spain

![CI](https://github.com/davidhidsan-dev/grid-pulse-spain/actions/workflows/ci.yml/badge.svg)

Data engineering project that combines real Spanish electricity data with regional weather variables.

The project follows an end-to-end approach: it extracts public data, shapes it into a consistent analytical structure, loads it into BigQuery, models it with dbt, and exposes the results through a Streamlit app.

[Versión en español](README.md)

## What this project solves

Electricity systems do not exist in isolation. Demand, renewable generation, wind, solar and hydro output can be better understood when they are compared with weather signals such as temperature, wind speed, solar radiation and precipitation.

This project builds a monthly regional dataset for Spanish autonomous communities so those relationships can be explored in a reproducible way.

## What the pipeline does

1. Extracts regional electricity balance data from the REData API.
2. Extracts historical weather data from Open-Meteo.
3. Normalizes raw JSON payloads into tabular CSV files.
4. Aggregates daily weather data to monthly grain.
5. Loads raw tables into BigQuery.
6. Builds staging and mart models with dbt.
7. Produces a curated table for analysis and visualization.
8. Makes the results explorable through a Streamlit app.

## Data sources

- **REData**: public electricity balance data from Red Eléctrica by autonomous community.
- **Open-Meteo Archive API**: daily historical weather data by regional coordinates.

The project uses monthly granularity because it gives a stable level of detail for joining regional electricity metrics with weather variables.

## Stack

- Python for extraction, normalization and loading.
- Google BigQuery as the analytical warehouse.
- dbt for data modeling.
- Apache Airflow for orchestration.
- Docker for reproducible Airflow execution.
- Streamlit for visualization.
- GitHub Actions for automated checks.

## Architecture

```text
Public APIs
  -> local raw JSON
  -> normalized CSV files
  -> BigQuery raw
  -> dbt staging
  -> dbt marts
  -> Streamlit
```

Airflow orchestrates the full flow from `dags/grid_pulse_pipeline.py`. Open-Meteo extraction is split by region so API limits have a smaller impact and failed regions can be retried independently.

## Data behavior

Raw and processed data are generated as local files with stable names. For example:

- `data/raw/redata/redata_madrid_monthly_sample.json`
- `data/raw/openmeteo/openmeteo_madrid_daily_sample.json`
- `data/processed/redata/redata_balance_electrico_monthly_normalized.csv`
- `data/processed/openmeteo/openmeteo_monthly_normalized.csv`

If you run extraction again for the same region, the local JSON file for that region is replaced with the latest API response. The same applies to normalized CSV files: each normalization step regenerates its output file.

BigQuery behaves differently: loads use append mode and then deduplicate by natural keys. This makes it possible to rerun the pipeline without keeping duplicate rows for the same regions, months and metrics.

In Airflow, if one weather extraction region fails, regions that already completed may have already written their local JSON files. On retry, that region can write its file again without affecting the others.

## Repository structure

```text
.
├── airflow/                  # Airflow image and requirements
├── dags/                     # Main pipeline DAG
├── data/reference/           # Regional catalog used by the project
├── dbt_project/              # dbt staging and mart models
├── scripts/                  # Executable pipeline entrypoints
├── src/                      # Reusable extraction, transformation and loading code
├── streamlit_app/            # Visualization app
├── tests/                    # Unit and structure tests
└── .github/workflows/        # CI
```

Raw and processed data are generated locally under `data/`, but they are not versioned in Git.

## Configuration

1. Create a virtual environment and install dependencies:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

2. Create your `.env` file from the example:

```powershell
Copy-Item .env.example .env
```

3. Fill in these variables:

```env
GCP_PROJECT_ID=your-gcp-project-id
BIGQUERY_DATASET_RAW=grid_pulse_raw
BIGQUERY_DATASET_ANALYTICS=grid_pulse_analytics
GOOGLE_APPLICATION_CREDENTIALS=path/to/service-account.json
```

`DATA_ROOT` is optional. If it is not defined, the project uses the local `data/` folder.

## Local execution

To run the full pipeline from scripts:

```powershell
python scripts/run_pipeline.py --language en --regions madrid --start-year 2022 --end-year 2025
```

You can also run each step separately:

```powershell
python scripts/run_ingestion.py --language en --regions madrid --start-year 2022 --end-year 2025
python scripts/run_openmeteo.py --language en --regions madrid --start-year 2022 --end-year 2025
python scripts/normalize_redata.py --regions madrid
python scripts/normalize_openmeteo.py --regions madrid
python scripts/aggregate_openmeteo_monthly.py
python scripts/load_redata_to_bigquery.py
python scripts/load_openmeteo_to_bigquery.py
python scripts/run_dbt.py
```

## Airflow execution

Airflow runs through Docker Compose:

```powershell
docker compose -f docker-compose.airflow.yml up airflow-init --build
docker compose -f docker-compose.airflow.yml up -d
```

Then open `http://localhost:8080`.

Default local credentials:

- Username: `admin`
- Password: `admin`

To stop the services:

```powershell
docker compose -f docker-compose.airflow.yml down
```

## dbt

The dbt project lives in `dbt_project/`.

Useful commands:

```powershell
dbt parse --project-dir dbt_project --profiles-dir dbt_project --no-partial-parse
dbt run --project-dir dbt_project --profiles-dir dbt_project --no-partial-parse
dbt test --project-dir dbt_project --profiles-dir dbt_project --no-partial-parse
```

Main models:

- `stg_redata_balance_electrico`
- `stg_openmeteo_monthly`
- `mart_energy_weather_monthly`
- `mart_energy_weather_monthly_curated`

## Streamlit

Once the curated table exists in BigQuery:

```powershell
streamlit run streamlit_app/app.py
```

The app lets users filter by region, energy metric and period, then compare energy trends with relevant weather variables.

Some comparisons are reasonable direct relationships, such as solar with radiation or wind generation with wind speed. Others are shown only as exploratory context: the app helps observe patterns, but it does not try to prove causality between weather and electricity generation.

## Quality

The project includes unit tests and CI checks:

```powershell
python -m compileall src scripts streamlit_app tests dags
python -m unittest discover -s tests -p "test_*.py"
```

GitHub Actions runs:

- Python compilation
- unit tests
- dbt project parsing

## Technical documentation

Additional documentation is available in:

- [Pipeline architecture](docs/architecture.md)
- [Data model](docs/data_model.md)

## Development note

This project was developed with support from Codex as a programming and learning assistant. It was used to better understand how the pipeline components work, compare technical decisions, debug issues, and speed up documentation and implementation tasks.

The pipeline design, result validation, final decisions, and code review were handled manually throughout the development process.

## Current status

The project already has a working end-to-end version:

- extraction from public APIs
- local normalization
- BigQuery loading
- dbt modeling
- Airflow orchestration
- Streamlit exploration app
- basic CI in GitHub Actions

Natural next steps:

- improve the visual design of the app
- expand dbt tests
- add screenshots or a dashboard demo
- prepare more detailed deployment notes
