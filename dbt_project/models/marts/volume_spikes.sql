with rolling_volume as (
    select
        id,
        symbol,
        event_timestamp,
        price,
        volume,
        avg(volume) over (
            partition by symbol
            order by event_timestamp
            range between interval '1 hour' preceding and current row
        ) as average_volume_last_hour
    from {{ ref('stg_stock_prices') }}
)

select
    id,
    symbol,
    event_timestamp,
    price,
    volume,
    round(average_volume_last_hour::numeric, 2) as average_volume_last_hour,
    round((volume / nullif(average_volume_last_hour, 0))::numeric, 2) as spike_ratio,
    volume > average_volume_last_hour * 2 as is_volume_spike
from rolling_volume
where average_volume_last_hour is not null
