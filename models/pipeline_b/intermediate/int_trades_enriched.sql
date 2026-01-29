-- Pipeline B: Trade Analytics Pipeline
-- Model: int_trades_enriched
-- Description: Intermediate model enriching trades with security and price data
--
-- ISSUES FOR ARTEMIS TO OPTIMIZE:
-- 1. Multiple heavy joins done row-by-row
-- 2. Price lookup repeated for every trade
-- 3. Could pre-aggregate before joining

with trades as (
    select * from {{ ref('stg_trades') }}
),

securities as (
    select * from {{ ref('stg_securities') }}
),

market_prices as (
    select * from {{ ref('stg_market_prices') }}
),

brokers as (
    select * from {{ ref('stg_brokers') }}
),

-- ISSUE: Heavy multi-way join before any aggregation
enriched as (
    select
        t.trade_id,
        t.portfolio_id,
        t.security_id,
        t.trade_date,
        t.settlement_date,
        t.trade_type,
        t.trade_category,
        t.trade_size_bucket,
        t.quantity,
        t.price as execution_price,
        t.gross_amount,
        t.commission,
        t.fees,
        t.net_amount,
        t.currency,
        t.settlement_days,
        t.trade_month,
        t.trade_quarter,
        t.trade_year,
        -- Security attributes
        s.ticker,
        s.security_name,
        s.security_type_standardized as security_type,
        s.asset_class,
        s.sector,
        s.industry,
        -- Broker attributes
        b.broker_name,
        b.broker_type,
        b.region as broker_region,
        b.commission_rate as standard_commission_rate,
        -- Market price on trade date
        mp.close_price as market_close_price,
        mp.ma_20,
        mp.ma_50,
        mp.volatility_20d,
        mp.trend_signal,
        mp.volume_signal,
        -- ISSUE: These calculations done per row
        case
            when mp.close_price > 0
            then (t.price - mp.close_price) / mp.close_price * 100
            else null
        end as execution_vs_close_pct,
        case
            when t.price > mp.close_price then 'ABOVE_MARKET'
            when t.price < mp.close_price then 'BELOW_MARKET'
            else 'AT_MARKET'
        end as execution_quality,
        -- Cost analysis
        t.commission + t.fees as total_costs,
        case
            when t.gross_amount > 0
            then (t.commission + t.fees) / t.gross_amount * 10000
            else null
        end as cost_bps
    from trades t
    inner join securities s
        on t.security_id = s.security_id
    left join brokers b
        on t.broker_id = b.broker_id
    left join market_prices mp
        on t.security_id = mp.security_id
        and t.trade_date = mp.price_date
)

select * from enriched
