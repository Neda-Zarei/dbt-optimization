-- Pipeline B: Trade Analytics Pipeline
-- Model: stg_market_prices
-- Description: Staging model for daily market prices
--

with source as (
    select
        security_id,
        price_date,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        created_at
    from {{ source('raw', 'market_prices') }}
    where price_date >= '{{ var("start_date") }}'
),

with_prior_day as (
    select
        curr.security_id,
        curr.price_date,
        curr.open_price,
        curr.high_price,
        curr.low_price,
        curr.close_price,
        curr.volume,
        prev.close_price as prior_close,
        prev.volume as prior_volume
    from source curr
    left join source prev
        on curr.security_id = prev.security_id
        and curr.price_date = dateadd('day', 1, prev.price_date)
),

with_returns as (
    select
        *,
        -- Daily return
        case
            when prior_close > 0
            then (close_price - prior_close) / prior_close
            else null
        end as daily_return,
        avg(close_price) over (
            partition by security_id
            order by price_date
            rows between 19 preceding and current row
        ) as ma_20,
        avg(close_price) over (
            partition by security_id
            order by price_date
            rows between 49 preceding and current row
        ) as ma_50,
        avg(close_price) over (
            partition by security_id
            order by price_date
            rows between 199 preceding and current row
        ) as ma_200,
        stddev(close_price) over (
            partition by security_id
            order by price_date
            rows between 19 preceding and current row
        ) as volatility_20d,
        avg(volume) over (
            partition by security_id
            order by price_date
            rows between 19 preceding and current row
        ) as avg_volume_20d
    from with_prior_day
),

final as (
    select
        *,
        case
            when ma_20 > ma_50 and ma_50 > ma_200 then 'BULLISH'
            when ma_20 < ma_50 and ma_50 < ma_200 then 'BEARISH'
            else 'NEUTRAL'
        end as trend_signal,
        case
            when volume > avg_volume_20d * 2 then 'HIGH'
            when volume < avg_volume_20d * 0.5 then 'LOW'
            else 'NORMAL'
        end as volume_signal
    from with_returns
)

select * from final
