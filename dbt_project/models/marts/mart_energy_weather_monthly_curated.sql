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
from {{ ref('mart_energy_weather_monthly') }}
where metric_type in (
    'Demanda en b.c.',
    'Generación renovable',
    'Generación no renovable',
    'Eólica',
    'Solar fotovoltaica',
    'Solar térmica',
    'Hidráulica',
    'Ciclo combinado',
    'Cogeneración'
)
