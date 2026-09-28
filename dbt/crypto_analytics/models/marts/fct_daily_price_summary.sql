with staged as (

    select * from {{ ref('stg_crypto_prices') }}

)

select
    coin_id,
    symbol,
    name,
    snapshot_date,
    price_usd,
    market_cap,
    market_cap_rank,
    volume_24h,
    price_change_pct_24h,
    case
        when price_change_pct_24h >= 5   then 'strong_gain'
        when price_change_pct_24h > 0    then 'gain'
        when price_change_pct_24h > -5   then 'loss'
        else 'strong_loss'
    end as daily_movement_category
from staged
