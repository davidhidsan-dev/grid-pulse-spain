{{ config(materialized='table') }}

with redata_monthly as (
    select
        year_month,
        group_type,
        metric_type,
        value,
        percentage
    from {{ ref('stg_redata_balance_electrico') }}
),

openmeteo_monthly as (
    select
        year_month,
        temperature_2m_max_avg,
        temperature_2m_mean_avg,
        temperature_2m_min_avg,
        precipitation_sum_total,
        wind_speed_10m_max_avg,
        shortwave_radiation_sum_total
    from {{ ref('stg_openmeteo_madrid_monthly') }}
)

select
    redata.year_month,
    redata.group_type,
    redata.metric_type,
    redata.value,
    redata.percentage,
    weather.temperature_2m_max_avg,
    weather.temperature_2m_mean_avg,
    weather.temperature_2m_min_avg,
    weather.precipitation_sum_total,
    weather.wind_speed_10m_max_avg,
    weather.shortwave_radiation_sum_total
from redata_monthly as redata
left join openmeteo_monthly as weather
    on redata.year_month = weather.year_month
