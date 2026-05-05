# Pipeline Architecture / Arquitectura del Pipeline

## ES — Objetivo de la arquitectura

Este proyecto implementa un pipeline batch de data engineering para cruzar datos reales del sistema eléctrico español con variables meteorológicas regionales.

La arquitectura busca que el flujo sea fácil de entender, reproducible y observable:

- Python extrae datos desde APIs públicas.
- Python normaliza los ficheros raw a estructuras tabulares.
- BigQuery almacena las tablas raw.
- dbt construye modelos de staging y marts.
- Airflow orquesta el proceso por tareas.
- Streamlit permite explorar el resultado final.

El foco no está en desplegar una plataforma productiva compleja, sino en mostrar un flujo end-to-end sólido, claro y ampliable.

## ES — Componentes principales

### 1. Extracción

La extracción se implementa en Python.

El proyecto consume dos fuentes públicas:

- REData, para datos de balance eléctrico regional.
- Open-Meteo, para histórico meteorológico diario.

REData se extrae por regiones y rango de años. Open-Meteo se divide por región dentro del DAG de Airflow para reducir el impacto de los límites de peticiones y permitir retries más pequeños.

### 2. Persistencia local

Las respuestas de las APIs se guardan primero como JSON raw dentro de `data/raw/`.

Esta capa local permite:

- inspeccionar las respuestas originales.
- repetir transformaciones sin llamar de nuevo a la API.
- separar extracción de normalización.

Si se relanza una extracción para la misma región, el JSON local de esa región se reemplaza.

### 3. Normalización y agregación

Los scripts de transformación convierten los JSON raw en CSV normalizados dentro de `data/processed/`.

En este proyecto:

- REData ya trabaja a granularidad mensual.
- Open-Meteo llega a granularidad diaria y se agrega después a mes.

El objetivo de esta capa es dejar las dos fuentes alineadas por región y mes.

### 4. Carga a BigQuery

Los CSV normalizados se cargan en BigQuery en la capa raw.

Las cargas usan modo append y después se aplica deduplicación por claves naturales:

- REData: `region_slug`, `metric_id`, `datetime`.
- Open-Meteo: `region_slug`, `year_month`.

Esto permite relanzar el pipeline sin acumular duplicados para los mismos periodos.

### 5. Modelado con dbt

dbt transforma las tablas raw en dos capas:

- staging, para limpiar, tipar y deduplicar.
- marts, para construir tablas analíticas listas para consumo.

La lógica principal de negocio vive en SQL y dbt, no en los scripts de Python.

### 6. Orquestación con Airflow

Airflow coordina el flujo completo desde `dags/grid_pulse_pipeline.py`.

El DAG separa el pipeline en grupos:

- `extract`
- `transform`
- `load`
- `model`

También valida parámetros antes de lanzar tareas costosas, distingue errores reintentables de errores de configuración y permite observar cada paso desde la UI.

### 7. Visualización con Streamlit

La app de Streamlit consume la tabla curada generada por dbt.

Su objetivo es convertir el resultado del pipeline en una herramienta sencilla para explorar regiones, métricas energéticas y variables meteorológicas.

Las comparaciones de la app deben leerse como análisis exploratorio. Algunas relaciones tienen una interpretación directa razonable, como solar con radiación o eólica con viento. Otras métricas usan el clima solo como contexto del periodo y no como explicación causal.

## ES — Flujo de ejecución

El flujo completo sigue este orden:

1. Validar regiones y rango de años.
2. Extraer datos de REData.
3. Extraer datos de Open-Meteo por región.
4. Normalizar REData.
5. Normalizar Open-Meteo.
6. Agregar Open-Meteo a nivel mensual.
7. Cargar REData en BigQuery raw.
8. Cargar Open-Meteo en BigQuery raw.
9. Ejecutar modelos dbt de staging.
10. Ejecutar modelos dbt de marts.
11. Ejecutar tests de dbt.
12. Explorar resultados desde Streamlit.

## ES — Capas de datos

El proyecto separa el dato en tres niveles principales:

- Local raw: JSON originales descargados desde las APIs.
- Local processed: CSV normalizados y agregados.
- BigQuery: tablas raw, staging y marts.

Esta separación hace que el pipeline sea más fácil de depurar: si falla una etapa, no siempre hace falta volver al principio ni llamar de nuevo a las APIs.

## ES — Decisiones de diseño

Algunas decisiones importantes del proyecto:

- Mantener Python para extracción, normalización y carga.
- Usar dbt para el modelado analítico.
- Dividir Open-Meteo por región en Airflow para controlar mejor los rate limits.
- Usar append más deduplicación en BigQuery para permitir reejecuciones.
- Mantener una app Streamlit sencilla como capa final de consumo.
- Ejecutar CI ligera sin depender de servicios externos reales.

## ES — Limitaciones actuales

- Airflow está preparado para ejecución local con Docker, no como despliegue cloud productivo.
- La CI no ejecuta el pipeline completo contra BigQuery.
- La app de Streamlit todavía puede mejorar visualmente.
- Los tests de dbt son básicos y podrían ampliarse con reglas de negocio.

---

## EN — Architecture objective

This project implements a batch data engineering pipeline that combines real Spanish electricity data with regional weather variables.

The architecture is designed to be understandable, reproducible and observable:

- Python extracts data from public APIs.
- Python normalizes raw files into tabular structures.
- BigQuery stores raw analytical tables.
- dbt builds staging and mart models.
- Airflow orchestrates the process by task.
- Streamlit makes the final result explorable.

The goal is not to deploy a complex production platform, but to show a solid, clear and extensible end-to-end flow.

## EN — Main components

### 1. Extraction

Extraction is implemented in Python.

The project consumes two public sources:

- REData, for regional electricity balance data.
- Open-Meteo, for daily historical weather data.

REData is extracted by region and year range. Open-Meteo is split by region inside the Airflow DAG to reduce the impact of API limits and make retries smaller.

### 2. Local persistence

API responses are first stored as raw JSON files under `data/raw/`.

This local layer makes it possible to:

- inspect original responses.
- rerun transformations without calling the API again.
- separate extraction from normalization.

If extraction is run again for the same region, that region's local JSON file is replaced.

### 3. Normalization and aggregation

Transformation scripts convert raw JSON files into normalized CSV files under `data/processed/`.

In this project:

- REData already works at monthly grain.
- Open-Meteo arrives at daily grain and is later aggregated to month.

The goal of this layer is to align both sources by region and month.

### 4. BigQuery loading

Normalized CSV files are loaded into the raw BigQuery layer.

Loads use append mode and then deduplicate by natural keys:

- REData: `region_slug`, `metric_id`, `datetime`.
- Open-Meteo: `region_slug`, `year_month`.

This makes reruns safe without accumulating duplicate rows for the same periods.

### 5. dbt modeling

dbt transforms raw tables into two layers:

- staging, for cleaning, casting and deduplication.
- marts, for analytical tables ready for consumption.

The main business modeling logic lives in SQL and dbt, not in Python scripts.

### 6. Airflow orchestration

Airflow coordinates the full flow from `dags/grid_pulse_pipeline.py`.

The DAG separates the pipeline into groups:

- `extract`
- `transform`
- `load`
- `model`

It also validates parameters before expensive tasks start, separates retryable errors from configuration errors, and makes each step observable from the UI.

### 7. Streamlit visualization

The Streamlit app consumes the curated table generated by dbt.

Its purpose is to turn the pipeline output into a simple tool for exploring regions, energy metrics and weather variables.

The app comparisons should be read as exploratory analysis. Some relationships have a reasonable direct interpretation, such as solar with radiation or wind generation with wind speed. Other metrics use weather only as period context, not as a causal explanation.

## EN — Execution flow

The full flow follows this order:

1. Validate regions and year range.
2. Extract REData data.
3. Extract Open-Meteo data by region.
4. Normalize REData.
5. Normalize Open-Meteo.
6. Aggregate Open-Meteo to monthly grain.
7. Load REData into raw BigQuery.
8. Load Open-Meteo into raw BigQuery.
9. Run dbt staging models.
10. Run dbt mart models.
11. Run dbt tests.
12. Explore results from Streamlit.

## EN — Data layers

The project separates data into three main levels:

- Local raw: original JSON downloaded from APIs.
- Local processed: normalized and aggregated CSV files.
- BigQuery: raw, staging and mart tables.

This separation makes the pipeline easier to debug: if one step fails, it is not always necessary to go back to the beginning or call the APIs again.

## EN — Design decisions

Some important project decisions:

- Keep Python for extraction, normalization and loading.
- Use dbt for analytical modeling.
- Split Open-Meteo by region in Airflow to better handle rate limits.
- Use append plus deduplication in BigQuery to allow safe reruns.
- Keep a simple Streamlit app as the final consumption layer.
- Run lightweight CI without depending on real external services.

## EN — Current limitations

- Airflow is prepared for local execution with Docker, not as a production cloud deployment.
- CI does not run the full pipeline against BigQuery.
- The Streamlit app can still improve visually.
- dbt tests are basic and could be expanded with business rules.
