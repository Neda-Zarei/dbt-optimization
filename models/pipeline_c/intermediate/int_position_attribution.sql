-- Pipeline C: Complex Portfolio Analytics
-- Model: int_position_attribution
-- Description: Attribution analysis at position level
--
-- ISSUES FOR ARTEMIS TO OPTIMIZE:
-- 1. Heavy multi-way join
-- 2. Complex attribution calculations
-- 3. Multiple window functions

with positions as (
    select * from {{ ref('stg_positions_daily') }}
),

securities as (
    select * from {{ ref('stg_securities') }}
),

market_prices as (
    select * from {{ ref('stg_market_prices') }}
),

-- ISSUE: Heavy 3-way join
enriched_positions as (
    select
        p.portfolio_id,
        p.security_id,
        p.position_date,
        p.quantity,
        p.market_value_usd,
        p.weight_pct,
        s.ticker,
        s.security_type_standardized as security_type,
        s.asset_class,
        s.sector,
        s.industry,
        mp.daily_return as security_return,
        mp.close_price,
        mp.ma_20,
        mp.ma_50,
        mp.volatility_20d
    from positions p
    inner join securities s
        on p.security_id = s.security_id
    left join market_prices mp
        on p.security_id = mp.security_id
        and p.position_date = mp.price_date
),

-- ISSUE: Window functions for prior day weight
with_prior_weight as (
    select
        *,
        lag(weight_pct, 1) over (
            partition by portfolio_id, security_id
            order by position_date
        ) as prior_weight_pct,
        lag(market_value_usd, 1) over (
            partition by portfolio_id, security_id
            order by position_date
        ) as prior_market_value
    from enriched_positions
),

-- ISSUE: Attribution calculations
with_attribution as (
    select
        *,
        -- Contribution to return
        coalesce(prior_weight_pct, weight_pct) * coalesce(security_return, 0) as contribution_to_return,
        -- Allocation effect (simplified Brinson)
        (weight_pct - coalesce(prior_weight_pct, weight_pct)) * coalesce(security_return, 0) as allocation_effect,
        -- Position P&L
        market_value_usd - coalesce(prior_market_value, market_value_usd) as position_pnl
    from with_prior_weight
)

select * from with_attribution
