with source as (

    select * from {{ source('raw', 'crypto_price_snapshots') }}

),

cleaned as (

    select
        coin_id,
        upper(symbol)                          as symbol,
        name,
        current_price::numeric                 as price_usd,
        market_cap::numeric                    as market_cap,
        market_cap_rank::int                   as market_cap_rank,
        total_volume::numeric                  as volume_24h,
        price_change_pct_24h::numeric          as price_change_pct_24h,
        snapshot_date::date                    as snapshot_date,
        fetched_at::timestamp                  as fetched_at
    from source
    where current_price is not null

)

select * from cleaned
