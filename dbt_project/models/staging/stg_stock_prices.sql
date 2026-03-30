with source_data as (
    select
        id,
        symbol,
        price,
        volume,
        event_timestamp,
        ingested_at
    from {{ source('stocks', 'raw_stock_prices') }}
)

select
    cast(id as bigint) as id,
    upper(trim(symbol)) as symbol,
    cast(price as numeric(10, 2)) as price,
    cast(volume as bigint) as volume,
    cast(event_timestamp as timestamptz) as event_timestamp,
    cast(ingested_at as timestamptz) as ingested_at
from source_data
