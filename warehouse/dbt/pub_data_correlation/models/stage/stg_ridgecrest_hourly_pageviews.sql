with filtered as (

    select
        project,
        article,
        views,
        date,
        hour
    from {{ ref('raw_pageviews') }}
    where project = 'en.wikipedia'
      and article in (
        'Ridgecrest_earthquake',
        'Earthquake',
        'California',
        'Seismology',
        'San_Bernardino_County,_California'
      )

),

hourly as (

    select
        article,
        timestamp(
            datetime(date, time(hour, 0, 0))
        ) as ts_utc,
        sum(views) as hourly_views
    from filtered
    group by 1, 2

)

select
    article,
    ts_utc,
    hourly_views
from hourly
order by ts_utc, article
