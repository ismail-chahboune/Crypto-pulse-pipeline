with staged as (

    select * from {{ ref('stg_crypto_prices') }}

),

with_returns as (

    select
        coin_id,
        symbol,
        snapshot_date,
        price_usd,
        (price_usd - lag(price_usd) over (partition by coin_id order by snapshot_date))
            / nullif(lag(price_usd) over (partition by coin_id order by snapshot_date), 0)
            as daily_return
    from staged

),

with_volatility as (

    select
        coin_id,
        symbol,
        snapshot_date,
        daily_return,
        stddev(daily_return) over (
            partition by coin_id
            order by snapshot_date
            rows between 6 preceding and current row
        ) as volatility_7d
    from with_returns

)

select * from with_volatility
