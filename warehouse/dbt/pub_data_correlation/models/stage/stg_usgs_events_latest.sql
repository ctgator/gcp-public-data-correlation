{{ config(
    materialized = 'incremental',
    unique_key = 'event_id'
) }}

with ranked_events as (

    select
        event_id,
        event_time,
        updated_time,
        ingest_time,
        latitude,
        longitude,
        depth_km,
        magnitude,
        magnitude_type,
        place_description,
        status,
        raw_payload,

        row_number() over (
            partition by event_id
            order by updated_time desc, ingest_time desc
        ) as rn

    from {{ source('raw', 'usgs_seismic_events') }}

    {% if is_incremental() %}
      where updated_time >= (
        select max(updated_time) from {{ this }}
      )
    {% endif %}

)

select
    event_id,
    event_time,
    latitude,
    longitude,
    depth_km,
    magnitude,
    magnitude_type,
    place_description,
    status,
    updated_time as last_updated_time
from ranked_events
where rn = 1

