{{ config(
    materialized = 'incremental',
    partition_by = {
      "field": "hour_ts",
      "data_type": "timestamp"
    },
    cluster_by = ["event_id"]
) }}

with pageviews as (

    select
        project,
        article,
        hour_ts,
        views,
        agent,
        access
    from {{ source('raw', 'wikimedia_pageviews') }}

),

events as (

    select
        event_id,
        event_time,
        window_start,
        window_end
    from {{ ref('stg_event_windows') }}

)

select
    e.event_id,
    e.event_time,
    p.hour_ts,
    timestamp_diff(p.hour_ts, e.event_time, hour) as hours_from_event,
    p.project,
    p.article,
    p.views,
    p.agent,
    p.access

from events e
join pageviews p
  on p.hour_ts between e.window_start and e.window_end

{% if is_incremental() %}
  where p.hour_ts >= (
    select max(hour_ts) from {{ this }}
  )
{% endif %}

