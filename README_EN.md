# grid-pulse-spain

End-to-end data engineering platform to analyze the Spanish electricity system using real electricity and weather data.

The project goal is to ingest hourly data from public sources, load it into BigQuery, transform it with dbt, and expose metrics and visualizations through a Streamlit application.

## Stack

- Python
- Google BigQuery
- dbt
- Apache Airflow
- Streamlit
- Public APIs for electricity and weather data

## Repository Structure

- `.github/workflows/` - automation and CI workflows
- `airflow/dags/` - orchestration DAGs
- `dbt_project/models/staging/` - staging models in dbt
- `dbt_project/models/marts/` - final analytical models
- `dbt_project/seeds/` - seed data for dbt
- `dbt_project/tests/` - quality tests in dbt
- `docs/` - project documentation
- `scripts/` - auxiliary scripts and runners
- `src/extract/redata/` - electricity data extraction
- `src/extract/weather/` - weather data extraction
- `src/load/` - data loading to BigQuery
- `src/config/` - configuration and constants
- `src/utils/` - shared utilities
- `streamlit_app/pages/` - Streamlit app pages
- `streamlit_app/components/` - reusable UI components
- `tests/` - unit and integration tests

## Roadmap in Phases

1. **Phase 1: Project Foundation**
   - Define repository structure
   - Configure environment and dependencies
   - Prepare initial project configuration

2. **Phase 2: Data Ingestion**
   - Connect to real electricity and weather APIs
   - Extract initial historical data
   - Store and validate raw data

3. **Phase 3: Loading and Modeling**
   - Load raw data into BigQuery
   - Build staging and marts models with dbt
   - Implement incremental models

4. **Phase 4: Quality and Automation**
   - Add basic quality tests
   - Create a reproducible runner
   - Orchestrate pipeline execution

5. **Phase 5: Visualization**
   - Develop the Streamlit application
   - Explore demand, renewable generation, weather, and price
   - Publish a final project version for portfolio
