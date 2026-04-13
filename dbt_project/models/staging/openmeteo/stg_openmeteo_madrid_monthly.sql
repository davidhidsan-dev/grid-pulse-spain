with raw_openmeteo_madrid_monthly as (
    select *
    from {{ source('openmeteo_raw', 'openmeteo_madrid_monthly') }}
)

select
    cast(raw.source as string) as source,
    cast(raw.location_name as string) as location_name,
    cast(raw.latitude as float64) as latitude,
    cast(raw.longitude as float64) as longitude,
    cast(raw.timezone as string) as timezone,
    cast(raw.year_month as string) as year_month,
    cast(raw.temperature_2m_max_avg as float64) as temperature_2m_max_avg,
    cast(raw.temperature_2m_mean_avg as float64) as temperature_2m_mean_avg,
    cast(raw.temperature_2m_min_avg as float64) as temperature_2m_min_avg,
    cast(raw.precipitation_sum_total as float64) as precipitation_sum_total,
    cast(raw.wind_speed_10m_max_avg as float64) as wind_speed_10m_max_avg,
    cast(raw.shortwave_radiation_sum_total as float64) as shortwave_radiation_sum_total
from raw_openmeteo_madrid_monthly as raw
