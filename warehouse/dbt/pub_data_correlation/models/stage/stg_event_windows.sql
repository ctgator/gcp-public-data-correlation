{{ config(
    materialized = 'incremental',
    unique_key = 'event_id'
) }}

select
    event_id,
    event_time,

    timestamp_sub(event_time, interval 72 hour) as window_start,
    timestamp_add(event_time, interval 72 hour) as window_end,

    144 as window_hours,
    magnitude as event_magnitude,
    depth_km as event_depth_km

from {{ ref('stg_usgs_events_latest') }}

{% if is_incremental() %}
  where event_time >= (
    select max(event_time) from {{ this }}
  )
{% endif %}

