-- Pipeline C: Complex Portfolio Analytics
-- Model: int_sector_attribution
-- Description: Aggregate attribution to sector level
--
-- ISSUES FOR ARTEMIS TO OPTIMIZE:
-- 1. Re-aggregation of position data
-- 2. Complex grouping logic
-- 3. Could be combined with position attribution

with position_attribution as (
    select * from {{ ref('int_position_attribution') }}
),

-- ISSUE: Aggregation that could be pushed upstream
sector_daily as (
    select
        portfolio_id,
        position_date,
        sector,
        count(distinct security_id) as position_count,
        sum(market_value_usd) as sector_market_value,
        sum(weight_pct) as sector_weight,
        sum(contribution_to_return) as sector_contribution,
        sum(allocation_effect) as sector_allocation_effect,
        sum(position_pnl) as sector_pnl,
        avg(security_return) as avg_security_return
    from position_attribution
    group by 1, 2, 3
),

-- ISSUE: Window functions for rolling metrics
with_rolling as (
    select
        *,
        sum(sector_contribution) over (
            partition by portfolio_id, sector
            order by position_date
            rows between 29 preceding and current row
        ) as sector_contribution_30d,
        avg(sector_weight) over (
            partition by portfolio_id, sector
            order by position_date
            rows between 29 preceding and current row
        ) as avg_sector_weight_30d,
        lag(sector_weight, 1) over (
            partition by portfolio_id, sector
            order by position_date
        ) as prior_sector_weight
    from sector_daily
)

select * from with_rolling
