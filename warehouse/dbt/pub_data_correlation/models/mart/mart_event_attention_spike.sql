{{ config(
    materialized = 'incremental',
    unique_key = 'event_id'
) }}

with hourly_attention as (

    select
        event_id,
        hour_ts,
        hours_from_event,
        sum(views) as total_views
    from {{ ref('stg_event_pageview_window') }}
    where agent = 'user'
    group by event_id, hour_ts, hours_from_event

),

baseline as (

    select
        event_id,
        avg(total_views) as baseline_avg_views_per_hour,
        stddev(total_views) as baseline_stddev_views_per_hour,
        sum(total_views) as baseline_total_views,
        count(*) as baseline_hours_observed
    from hourly_attention
    where hours_from_event < 0
    group by event_id

),

response as (

    select
        event_id,
        sum(total_views) as post_event_total_views,
        max(total_views) as peak_hourly_views,
        min(hours_from_event) as peak_hours_from_event,
        count(*) as response_hours_observed
    from hourly_attention
    where hours_from_event >= 0
    group by event_id

),

spike as (

    select
        h.event_id,

        b.baseline_avg_views_per_hour
            + (3 * b.baseline_stddev_views_per_hour)
            as spike_threshold,

        sum(
            case
                when h.total_views > b.baseline_avg_views_per_hour
                then h.total_views - b.baseline_avg_views_per_hour
                else 0
            end
        ) as spike_area_above_baseline,

        countif(
            h.total_views >=
            b.baseline_avg_views_per_hour
            + (3 * b.baseline_stddev_views_per_hour)
        ) as hours_above_threshold

    from hourly_attention h
    join baseline b
      on h.event_id = b.event_id
    where h.hours_from_event >= 0
    group by h.event_id, spike_threshold

)

select
    e.event_id,
    e.event_time,
    e.event_magnitude as magnitude,
    e.event_depth_km as depth_km,

    b.baseline_avg_views_per_hour,
    b.baseline_stddev_views_per_hour,
    b.baseline_total_views,

    r.post_event_total_views,
    r.peak_hourly_views,
    r.peak_hours_from_event,

    safe_divide(
        r.peak_hourly_views,
        b.baseline_avg_views_per_hour
    ) as peak_to_baseline_ratio,

    s.spike_threshold,
    s.hours_above_threshold,
    s.spike_area_above_baseline,

    r.peak_hourly_views >= s.spike_threshold
        as spike_detected,

    b.baseline_hours_observed,
    r.response_hours_observed,

    b.baseline_hours_observed >= 48
      and r.response_hours_observed >= 48
        as data_completeness_flag

from {{ ref('stg_event_windows') }} e
join baseline b
  on e.event_id = b.event_id
join response r
  on e.event_id = r.event_id
join spike s
  on e.event_id = s.event_id

{% if is_incremental() %}
  where e.event_time >= (
    select max(event_time) from {{ this }}
  )
{% endif %}

