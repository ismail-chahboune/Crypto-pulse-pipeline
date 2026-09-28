with staged as (

    select * from {{ ref('stg_crypto_prices') }}

),

with_windows as (

    select
        coin_id,
        symbol,
        snapshot_date,
        price_usd,
        avg(price_usd) over (
            partition by coin_id
            order by snapshot_date
            rows between 6 preceding and current row
        ) as moving_avg_7d,
        avg(price_usd) over (
            partition by coin_id
            order by snapshot_date
            rows between 13 preceding and current row
        ) as moving_avg_14d
    from staged

)

select * from with_windows
