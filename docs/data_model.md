# Data Model / Modelo de Datos

## ES — Objetivo del modelo

El modelo de datos une información mensual de energía y clima por comunidad autónoma.

La pregunta base del proyecto es sencilla: cómo se comportan distintas métricas del sistema eléctrico español cuando se observan junto a variables meteorológicas como temperatura, viento, radiación solar o precipitación.

Para responderla, el proyecto construye una capa analítica común con dos dimensiones compartidas:

- `region_slug`
- `year_month`

## ES — Fuentes principales

### REData

REData aporta métricas del balance eléctrico regional.

Ejemplos de métricas usadas en la capa curada:

- `Demanda en b.c.`
- `Generación renovable`
- `Generación no renovable`
- `Eólica`
- `Solar fotovoltaica`
- `Solar térmica`
- `Hidráulica`
- `Ciclo combinado`
- `Cogeneración`

### Open-Meteo

Open-Meteo aporta variables meteorológicas históricas.

El proyecto trabaja con estas variables mensuales:

- temperatura máxima media.
- temperatura media.
- temperatura mínima media.
- precipitación total.
- viento máximo medio.
- radiación solar total.

## ES — Tablas raw en BigQuery

### `redata_balance_electrico`

Tabla raw cargada desde el CSV normalizado de REData.

Columnas principales:

- `region_slug`
- `region_name`
- `redata_geo_id`
- `ingestion_timestamp`
- `group_type`
- `metric_type`
- `metric_id`
- `metric_title`
- `year_month`
- `datetime`
- `value`
- `percentage`

Clave natural usada para deduplicar:

- `region_slug`
- `metric_id`
- `datetime`

### `openmeteo_monthly`

Tabla raw cargada desde el CSV mensual de Open-Meteo.

Columnas principales:

- `region_slug`
- `region_name`
- `location_name`
- `latitude`
- `longitude`
- `timezone`
- `year_month`
- `temperature_2m_max_avg`
- `temperature_2m_mean_avg`
- `temperature_2m_min_avg`
- `precipitation_sum_total`
- `wind_speed_10m_max_avg`
- `shortwave_radiation_sum_total`

Clave natural usada para deduplicar:

- `region_slug`
- `year_month`

## ES — Modelos staging

### `stg_redata_balance_electrico`

Este modelo lee la tabla raw de REData, conserva la última versión de cada registro según `ingestion_timestamp` y aplica tipos explícitos.

Su grano es:

- una región.
- un mes.
- una métrica energética.

### `stg_openmeteo_monthly`

Este modelo lee la tabla raw mensual de Open-Meteo, conserva la última versión por región y mes, y aplica tipos explícitos.

Su grano es:

- una región.
- un mes.

## ES — Modelos marts

### `mart_energy_weather_monthly`

Modelo analítico que une REData y Open-Meteo por:

- `region_slug`
- `year_month`

El resultado mantiene una fila por región, mes y métrica energética, añadiendo las variables meteorológicas mensuales correspondientes.

Este modelo permite analizar, por ejemplo, generación solar junto a radiación solar, generación eólica junto a viento o demanda junto a temperatura.

No todas las combinaciones energía-clima deben interpretarse igual. Algunas variables se usan como relación principal, mientras que otras se muestran solo como contexto del sistema. El modelo facilita la exploración, pero no demuestra causalidad por sí solo.

### `mart_energy_weather_monthly_curated`

Modelo final pensado para consumo desde Streamlit.

Parte de `mart_energy_weather_monthly` y filtra un conjunto de métricas energéticas más útil para visualización.

Es la tabla recomendada para dashboards y exploración rápida.

## ES — Relaciones del modelo

Relación principal:

- `stg_redata_balance_electrico.region_slug + year_month`
- `stg_openmeteo_monthly.region_slug + year_month`

REData actúa como tabla base del mart, y Open-Meteo se une mediante `left join`. Esto significa que se conservan las métricas energéticas aunque una región o mes no tuviera datos meteorológicos disponibles.

## ES — Calidad y validaciones

El proyecto incluye tests básicos de dbt sobre campos críticos:

- `region_slug`
- `year_month`
- `metric_type`
- `value`
- `datetime`
- `location_name`

Además, los modelos staging deduplican los datos para que las reejecuciones del pipeline no generen duplicados analíticos.

## ES — Decisiones de modelado

Algunas decisiones importantes:

- Se usa grano mensual para alinear las dos fuentes.
- Se conserva `metric_type` para poder filtrar métricas energéticas de forma flexible.
- La tabla curada no pretende contener todo REData, sino las métricas más útiles para análisis visual.
- Las coordenadas meteorológicas son representativas por región, no una malla meteorológica completa.

---

## EN — Model objective

The data model combines monthly energy and weather information by Spanish autonomous community.

The core project question is simple: how different Spanish electricity system metrics behave when they are observed next to weather variables such as temperature, wind, solar radiation or precipitation.

To answer it, the project builds a common analytical layer with two shared dimensions:

- `region_slug`
- `year_month`

## EN — Main sources

### REData

REData provides regional electricity balance metrics.

Examples of metrics kept in the curated layer:

- `Demanda en b.c.`
- `Generación renovable`
- `Generación no renovable`
- `Eólica`
- `Solar fotovoltaica`
- `Solar térmica`
- `Hidráulica`
- `Ciclo combinado`
- `Cogeneración`

### Open-Meteo

Open-Meteo provides historical weather variables.

The project uses these monthly variables:

- average maximum temperature.
- average mean temperature.
- average minimum temperature.
- total precipitation.
- average maximum wind speed.
- total solar radiation.

## EN — Raw BigQuery tables

### `redata_balance_electrico`

Raw table loaded from the normalized REData CSV.

Main columns:

- `region_slug`
- `region_name`
- `redata_geo_id`
- `ingestion_timestamp`
- `group_type`
- `metric_type`
- `metric_id`
- `metric_title`
- `year_month`
- `datetime`
- `value`
- `percentage`

Natural key used for deduplication:

- `region_slug`
- `metric_id`
- `datetime`

### `openmeteo_monthly`

Raw table loaded from the monthly Open-Meteo CSV.

Main columns:

- `region_slug`
- `region_name`
- `location_name`
- `latitude`
- `longitude`
- `timezone`
- `year_month`
- `temperature_2m_max_avg`
- `temperature_2m_mean_avg`
- `temperature_2m_min_avg`
- `precipitation_sum_total`
- `wind_speed_10m_max_avg`
- `shortwave_radiation_sum_total`

Natural key used for deduplication:

- `region_slug`
- `year_month`

## EN — Staging models

### `stg_redata_balance_electrico`

This model reads the raw REData table, keeps the latest version of each record according to `ingestion_timestamp`, and applies explicit types.

Its grain is:

- one region.
- one month.
- one energy metric.

### `stg_openmeteo_monthly`

This model reads the monthly raw Open-Meteo table, keeps the latest version by region and month, and applies explicit types.

Its grain is:

- one region.
- one month.

## EN — Mart models

### `mart_energy_weather_monthly`

Analytical model that joins REData and Open-Meteo by:

- `region_slug`
- `year_month`

The result keeps one row per region, month and energy metric, adding the corresponding monthly weather variables.

This model makes it possible to analyze solar generation next to solar radiation, wind generation next to wind speed, or demand next to temperature.

Not every energy-weather combination should be interpreted in the same way. Some variables are used as a primary relationship, while others are shown only as system context. The model supports exploration, but it does not prove causality by itself.

### `mart_energy_weather_monthly_curated`

Final model intended for Streamlit consumption.

It starts from `mart_energy_weather_monthly` and filters a smaller set of energy metrics that are useful for visualization.

This is the recommended table for dashboards and quick exploration.

## EN — Model relationships

Main relationship:

- `stg_redata_balance_electrico.region_slug + year_month`
- `stg_openmeteo_monthly.region_slug + year_month`

REData acts as the base table for the mart, and Open-Meteo is joined through a `left join`. This means energy metrics are preserved even if weather data is missing for a region or month.

## EN — Quality and validations

The project includes basic dbt tests on critical fields:

- `region_slug`
- `year_month`
- `metric_type`
- `value`
- `datetime`
- `location_name`

Staging models also deduplicate the data so pipeline reruns do not create analytical duplicates.

## EN — Modeling decisions

Some important decisions:

- Monthly grain is used to align both sources.
- `metric_type` is preserved so energy metrics can be filtered flexibly.
- The curated table is not meant to contain all REData metrics, only the most useful ones for visual analysis.
- Weather coordinates are representative by region, not a full meteorological grid.
