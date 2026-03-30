select
    id,
    symbol,
    event_timestamp,
    price as actual_price,
    round(
        avg(price) over (
            partition by symbol
            order by event_timestamp
            range between interval '5 minutes' preceding and current row
        )::numeric,
        2
    ) as moving_average_5m,
    round(
        avg(price) over (
            partition by symbol
            order by event_timestamp
            range between interval '15 minutes' preceding and current row
        )::numeric,
        2
    ) as moving_average_15m,
    volume
from {{ ref('stg_stock_prices') }}
