-- Pipeline C: Complex Portfolio Analytics
-- Model: int_portfolio_returns_daily
-- Description: Calculate daily portfolio returns from NAV
--
-- ISSUES FOR ARTEMIS TO OPTIMIZE:
-- 1. Self-join for prior day NAV (should use LAG)
-- 2. Multiple passes for return calculations
-- 3. Complex window functions

with valuations as (
    select * from {{ ref('stg_valuations') }}
),

cashflows as (
    select
        portfolio_id,
        cashflow_date,
        sum(case when cashflow_type = 'CONTRIBUTION' then amount else 0 end) as contributions,
        sum(case when cashflow_type = 'DISTRIBUTION' then amount else 0 end) as distributions
    from {{ ref('stg_cashflows') }}
    group by 1, 2
),

-- ISSUE: Self-join instead of LAG for prior NAV
with_prior_nav as (
    select
        curr.portfolio_id,
        curr.valuation_date,
        curr.nav,
        curr.nav_usd,
        prev.nav as prior_nav,
        prev.nav_usd as prior_nav_usd,
        coalesce(cf.contributions, 0) as contributions,
        coalesce(cf.distributions, 0) as distributions
    from valuations curr
    left join valuations prev
        on curr.portfolio_id = prev.portfolio_id
        and curr.valuation_date = dateadd('day', 1, prev.valuation_date)
    left join cashflows cf
        on curr.portfolio_id = cf.portfolio_id
        and curr.valuation_date = cf.cashflow_date
),

-- ISSUE: Modified Dietz calculation done inefficiently
with_daily_return as (
    select
        portfolio_id,
        valuation_date,
        nav,
        nav_usd,
        prior_nav,
        prior_nav_usd,
        contributions,
        distributions,
        -- Simple return
        case
            when prior_nav > 0
            then (nav - prior_nav - contributions + distributions) / prior_nav
            else null
        end as daily_return_simple,
        -- Modified Dietz (approximation)
        case
            when (prior_nav + contributions * 0.5) > 0
            then (nav - prior_nav - contributions + distributions) / (prior_nav + contributions * 0.5 - distributions * 0.5)
            else null
        end as daily_return_mod_dietz
    from with_prior_nav
),

-- ISSUE: Multiple window functions for different periods
with_rolling_returns as (
    select
        *,
        -- Cumulative return using log returns
        exp(sum(ln(1 + coalesce(daily_return_mod_dietz, 0))) over (
            partition by portfolio_id
            order by valuation_date
            rows between unbounded preceding and current row
        )) - 1 as cumulative_return,
        -- Rolling period returns
        exp(sum(ln(1 + coalesce(daily_return_mod_dietz, 0))) over (
            partition by portfolio_id
            order by valuation_date
            rows between 6 preceding and current row
        )) - 1 as return_1w,
        exp(sum(ln(1 + coalesce(daily_return_mod_dietz, 0))) over (
            partition by portfolio_id
            order by valuation_date
            rows between 29 preceding and current row
        )) - 1 as return_1m,
        exp(sum(ln(1 + coalesce(daily_return_mod_dietz, 0))) over (
            partition by portfolio_id
            order by valuation_date
            rows between 89 preceding and current row
        )) - 1 as return_3m,
        exp(sum(ln(1 + coalesce(daily_return_mod_dietz, 0))) over (
            partition by portfolio_id
            order by valuation_date
            rows between 179 preceding and current row
        )) - 1 as return_6m,
        exp(sum(ln(1 + coalesce(daily_return_mod_dietz, 0))) over (
            partition by portfolio_id
            order by valuation_date
            rows between 364 preceding and current row
        )) - 1 as return_1y,
        -- Rolling volatility
        stddev(daily_return_mod_dietz) over (
            partition by portfolio_id
            order by valuation_date
            rows between 29 preceding and current row
        ) * sqrt(252) as volatility_1m,
        stddev(daily_return_mod_dietz) over (
            partition by portfolio_id
            order by valuation_date
            rows between 251 preceding and current row
        ) * sqrt(252) as volatility_1y
    from with_daily_return
)

select * from with_rolling_returns
