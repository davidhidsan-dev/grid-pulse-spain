with raw_openmeteo_monthly as (
    select *
    from {{ source('openmeteo_raw', 'openmeteo_monthly') }}
),

deduplicated_openmeteo_monthly as (
    select *
    from raw_openmeteo_monthly
    qualify row_number() over (
        partition by region_slug, year_month
        order by ingestion_timestamp desc
    ) = 1
)

select
    cast(raw.source as string) as source,
    cast(raw.ingestion_timestamp as timestamp) as ingestion_timestamp,
    cast(raw.region_slug as string) as region_slug,
    cast(raw.region_name as string) as region_name,
    cast(raw.location_name as string) as location_name,
    cast(raw.latitude as float64) as latitude,
    cast(raw.longitude as float64) as longitude,
    cast(raw.timezone as string) as timezone,
    cast(raw.weather_point_type as string) as weather_point_type,
    cast(raw.year_month as string) as year_month,
    cast(raw.temperature_2m_max_avg as float64) as temperature_2m_max_avg,
    cast(raw.temperature_2m_mean_avg as float64) as temperature_2m_mean_avg,
    cast(raw.temperature_2m_min_avg as float64) as temperature_2m_min_avg,
    cast(raw.precipitation_sum_total as float64) as precipitation_sum_total,
    cast(raw.wind_speed_10m_max_avg as float64) as wind_speed_10m_max_avg,
    cast(raw.shortwave_radiation_sum_total as float64) as shortwave_radiation_sum_total
from deduplicated_openmeteo_monthly as raw
