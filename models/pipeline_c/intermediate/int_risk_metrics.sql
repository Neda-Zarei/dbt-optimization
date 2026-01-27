-- Pipeline C: Complex Portfolio Analytics
-- Model: int_risk_metrics
-- Description: Calculate risk metrics for portfolios
--
-- ISSUES FOR ARTEMIS TO OPTIMIZE:
-- 1. Multiple window functions with same partition
-- 2. VaR calculation inefficiencies
-- 3. Could pre-compute some metrics

with portfolio_returns as (
    select * from {{ ref('int_portfolio_returns_daily') }}
),

-- ISSUE: Multiple passes for different risk calculations
with_risk_metrics as (
    select
        portfolio_id,
        valuation_date,
        daily_return_mod_dietz as daily_return,
        nav_usd,
        -- ISSUE: Repeated window frame definitions
        -- Max drawdown components
        max(nav_usd) over (
            partition by portfolio_id
            order by valuation_date
            rows between unbounded preceding and current row
        ) as running_max_nav,
        -- Downside deviation
        sqrt(avg(
            case when daily_return_mod_dietz < 0 then power(daily_return_mod_dietz, 2) else 0 end
        ) over (
            partition by portfolio_id
            order by valuation_date
            rows between 251 preceding and current row
        )) * sqrt(252) as downside_deviation_1y,
        -- Sortino components
        avg(daily_return_mod_dietz) over (
            partition by portfolio_id
            order by valuation_date
            rows between 251 preceding and current row
        ) * 252 as annualized_return_1y,
        volatility_1y
    from portfolio_returns
),

-- ISSUE: Another CTE for derived metrics
with_derived as (
    select
        *,
        -- Drawdown
        (nav_usd - running_max_nav) / nullif(running_max_nav, 0) as drawdown,
        -- Sortino ratio (assuming 0% risk-free)
        case
            when downside_deviation_1y > 0
            then annualized_return_1y / downside_deviation_1y
            else null
        end as sortino_ratio,
        -- Sharpe ratio (assuming 0% risk-free)
        case
            when volatility_1y > 0
            then annualized_return_1y / volatility_1y
            else null
        end as sharpe_ratio
    from with_risk_metrics
),

-- ISSUE: Max drawdown calculation
with_max_drawdown as (
    select
        *,
        min(drawdown) over (
            partition by portfolio_id
            order by valuation_date
            rows between unbounded preceding and current row
        ) as max_drawdown
    from with_derived
),

-- ISSUE: VaR calculation (simplified parametric)
final as (
    select
        *,
        -- Parametric VaR (95%)
        nav_usd * volatility_1y / sqrt(252) * 1.645 as var_95_1d,
        -- Parametric VaR (99%)
        nav_usd * volatility_1y / sqrt(252) * 2.326 as var_99_1d
    from with_max_drawdown
)

select * from final
