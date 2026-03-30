with latest_price as (
    select distinct on (symbol)
        symbol,
        event_timestamp as latest_timestamp,
        price as latest_price
    from {{ ref('stg_stock_prices') }}
    order by symbol, event_timestamp desc
),
baseline_candidates as (
    select
        symbol,
        event_timestamp,
        price,
        case
            when event_timestamp <= current_timestamp - interval '1 hour' then 0
            else 1
        end as fallback_priority,
        abs(
            extract(
                epoch from (
                    event_timestamp - (current_timestamp - interval '1 hour')
                )
            )
        ) as distance_from_target_seconds
    from {{ ref('stg_stock_prices') }}
),
baseline_price as (
    select distinct on (symbol)
        symbol,
        event_timestamp as baseline_timestamp,
        price as baseline_price
    from baseline_candidates
    order by symbol, fallback_priority asc, distance_from_target_seconds asc
),
changes as (
    select
        latest_price.symbol,
        latest_price.latest_timestamp,
        baseline_price.baseline_timestamp,
        latest_price.latest_price,
        baseline_price.baseline_price,
        round(
            (latest_price.latest_price - baseline_price.baseline_price)::numeric,
            2
        ) as price_change,
        round(
            (
                (latest_price.latest_price - baseline_price.baseline_price)
                / nullif(baseline_price.baseline_price, 0)
            )::numeric * 100,
            2
        ) as percent_change
    from latest_price
    inner join baseline_price using (symbol)
),
gainers as (
    select
        symbol,
        latest_timestamp,
        baseline_timestamp,
        latest_price,
        baseline_price,
        price_change,
        percent_change,
        'gainer' as direction,
        row_number() over (order by percent_change desc, symbol asc) as rank
    from changes
    where percent_change >= 0
),
losers as (
    select
        symbol,
        latest_timestamp,
        baseline_timestamp,
        latest_price,
        baseline_price,
        price_change,
        percent_change,
        'loser' as direction,
        row_number() over (order by percent_change asc, symbol asc) as rank
    from changes
    where percent_change < 0
)

select *
from gainers
where rank <= 5

union all

select *
from losers
where rank <= 5
