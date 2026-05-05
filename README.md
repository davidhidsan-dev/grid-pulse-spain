# grid-pulse-spain

![CI](https://github.com/davidhidsan-dev/grid-pulse-spain/actions/workflows/ci.yml/badge.svg)

Proyecto de data engineering para cruzar datos reales del sistema eléctrico español con variables meteorológicas regionales.

El proyecto sigue un enfoque end-to-end: extrae datos públicos, los transforma en una estructura analítica consistente, los carga en BigQuery, los modela con dbt y los expone desde una app de Streamlit.

[English version](README_EN.md)

## Qué problema resuelve

El sistema eléctrico no vive aislado. La demanda, la generación renovable o tecnologías como la eólica, la solar y la hidráulica pueden analizarse mejor si se ponen al lado de señales climáticas como temperatura, viento, radiación solar o precipitación.

Este proyecto construye una base mensual por comunidad autónoma para explorar esas relaciones de forma reproducible.

## Qué hace el pipeline

1. Extrae datos de balance eléctrico regional desde la API de REData.
2. Extrae histórico meteorológico desde Open-Meteo.
3. Normaliza los JSON crudos a CSV tabulares.
4. Agrega el clima diario a granularidad mensual.
5. Carga las capas raw en BigQuery.
6. Modela staging y marts con dbt.
7. Expone una tabla curada para análisis y visualización.
8. Permite explorar los resultados desde una app de Streamlit.

## Fuentes de datos

- **REData**: datos públicos de Red Eléctrica sobre balance eléctrico por comunidad autónoma.
- **Open-Meteo Archive API**: histórico meteorológico diario por coordenadas regionales.

El proyecto trabaja con una granularidad mensual porque permite alinear de forma estable los datos eléctricos regionales con las variables meteorológicas.

## Stack

- Python para extracción, normalización y carga.
- Google BigQuery como almacén analítico.
- dbt para modelado de datos.
- Apache Airflow para orquestación.
- Docker para ejecutar Airflow de forma reproducible.
- Streamlit para la capa de visualización.
- GitHub Actions para validaciones automáticas.

## Arquitectura

```text
APIs públicas
  -> JSON raw local
  -> CSV normalizados
  -> BigQuery raw
  -> dbt staging
  -> dbt marts
  -> Streamlit
```

Airflow orquesta el flujo completo desde `dags/grid_pulse_pipeline.py`. Para Open-Meteo, la extracción se divide por región para reducir el impacto de límites de peticiones y poder reintentar solo la parte que falle.

## Comportamiento de los datos

Los datos raw y procesados se generan como archivos locales con nombres estables. Por ejemplo:

- `data/raw/redata/redata_madrid_monthly_sample.json`
- `data/raw/openmeteo/openmeteo_madrid_daily_sample.json`
- `data/processed/redata/redata_balance_electrico_monthly_normalized.csv`
- `data/processed/openmeteo/openmeteo_monthly_normalized.csv`

Si vuelves a ejecutar la extracción para la misma región, el JSON local de esa región se reemplaza por la nueva respuesta de la API. Lo mismo ocurre con los CSV normalizados: cada normalización genera de nuevo el archivo de salida.

En BigQuery el comportamiento es distinto: las cargas se hacen en modo append y después se deduplican por claves naturales. Esto permite relanzar el pipeline sin ir acumulando filas duplicadas para las mismas regiones, meses y métricas.

En Airflow, si una región falla durante la extracción meteorológica, las regiones que ya terminaron pueden haber dejado sus JSON guardados localmente. Al reintentar, la tarea de esa región puede volver a escribir su archivo sin afectar a las demás.

## Estructura del repositorio

```text
.
├── airflow/                  # Imagen y dependencias para Airflow
├── dags/                     # DAG principal del pipeline
├── data/reference/           # Catálogo de regiones usado por el proyecto
├── dbt_project/              # Modelos dbt de staging y marts
├── scripts/                  # Entrypoints ejecutables del pipeline
├── src/                      # Código reutilizable de extracción, transformación y carga
├── streamlit_app/            # App de visualización
├── tests/                    # Tests unitarios y de estructura
└── .github/workflows/        # CI
```

Los datos raw y procesados se generan localmente bajo `data/`, pero no se versionan en Git.

## Configuración

1. Crea un entorno virtual e instala dependencias:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

2. Crea tu archivo `.env` a partir del ejemplo:

```powershell
Copy-Item .env.example .env
```

3. Completa estas variables:

```env
GCP_PROJECT_ID=your-gcp-project-id
BIGQUERY_DATASET_RAW=grid_pulse_raw
BIGQUERY_DATASET_ANALYTICS=grid_pulse_analytics
GOOGLE_APPLICATION_CREDENTIALS=path/to/service-account.json
```

`DATA_ROOT` es opcional. Si no se define, el proyecto usa la carpeta local `data/`.

## Ejecución local

Para ejecutar el pipeline completo desde scripts:

```powershell
python scripts/run_pipeline.py --language en --regions madrid --start-year 2022 --end-year 2025
```

También puedes ejecutar cada etapa por separado:

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

## Ejecución con Airflow

Airflow se ejecuta con Docker Compose:

```powershell
docker compose -f docker-compose.airflow.yml up airflow-init --build
docker compose -f docker-compose.airflow.yml up -d
```

Después abre `http://localhost:8080`.

Credenciales locales por defecto:

- Usuario: `admin`
- Password: `admin`

Para parar los servicios:

```powershell
docker compose -f docker-compose.airflow.yml down
```

## dbt

El proyecto dbt vive en `dbt_project/`.

Comandos útiles:

```powershell
dbt parse --project-dir dbt_project --profiles-dir dbt_project --no-partial-parse
dbt run --project-dir dbt_project --profiles-dir dbt_project --no-partial-parse
dbt test --project-dir dbt_project --profiles-dir dbt_project --no-partial-parse
```

Los modelos principales son:

- `stg_redata_balance_electrico`
- `stg_openmeteo_monthly`
- `mart_energy_weather_monthly`
- `mart_energy_weather_monthly_curated`

## Streamlit

Una vez generada la tabla curada en BigQuery:

```powershell
streamlit run streamlit_app/app.py
```

La app permite filtrar por comunidad, métrica energética y periodo, y comparar la evolución energética con variables climáticas relevantes.

Algunas comparaciones son relaciones directas razonables, como solar con radiación o eólica con viento. Otras se muestran solo como contexto exploratorio: la app ayuda a observar patrones, pero no pretende demostrar causalidad entre clima y generación eléctrica.

## Calidad

El proyecto incluye tests unitarios y validaciones de CI:

```powershell
python -m compileall src scripts streamlit_app tests dags
python -m unittest discover -s tests -p "test_*.py"
```

GitHub Actions ejecuta:

- compilación de código Python
- tests unitarios
- parseo del proyecto dbt

## Documentación técnica

Documentación adicional disponible en:

- [Arquitectura del pipeline](docs/architecture.md)
- [Modelo de datos](docs/data_model.md)

## Nota sobre el desarrollo

Este proyecto se ha desarrollado con apoyo de Codex como asistente de programación y aprendizaje. Se ha utilizado para entender mejor cómo funcionan las piezas del pipeline, contrastar decisiones técnicas, depurar errores y acelerar tareas de documentación e implementación.

El diseño del flujo, la validación de resultados, las decisiones finales y la revisión del código se han trabajado de forma manual durante el desarrollo.

## Estado actual

El proyecto ya tiene una versión funcional del flujo end-to-end:

- extracción desde APIs públicas
- normalización local
- carga a BigQuery
- modelado con dbt
- orquestación con Airflow
- app de exploración con Streamlit
- CI básico en GitHub Actions

Próximos pasos naturales:

- mejorar el diseño visual de la app
- ampliar tests de dbt
- añadir capturas o demo del dashboard
- preparar documentación de despliegue más detallada
