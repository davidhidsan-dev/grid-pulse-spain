with raw_redata_balance_electrico as (
    select *
    from {{ source('redata_raw', 'redata_balance_electrico') }}
)

select
    cast(raw.source as string) as source,
    cast(raw.endpoint as string) as endpoint,
    cast(raw.ingestion_timestamp as timestamp) as ingestion_timestamp,
    cast(raw.group_type as string) as group_type,
    cast(raw.group_id as string) as group_id,
    cast(raw.group_title as string) as group_title,
    cast(raw.metric_type as string) as metric_type,
    cast(raw.metric_id as string) as metric_id,
    cast(raw.metric_group_id as string) as metric_group_id,
    cast(raw.metric_title as string) as metric_title,
    cast(raw.metric_description as string) as metric_description,
    cast(raw.is_composite as bool) as is_composite,
    cast(raw.last_update as timestamp) as last_update,
    cast(raw.total as float64) as total,
    cast(raw.total_percentage as float64) as total_percentage,
    cast(raw.datetime as timestamp) as datetime,
    cast(raw.value as float64) as value,
    cast(raw.percentage as float64) as percentage
from raw_redata_balance_electrico as raw
