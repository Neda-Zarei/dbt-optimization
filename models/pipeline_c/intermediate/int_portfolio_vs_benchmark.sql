-- Pipeline C: Complex Portfolio Analytics
-- Model: int_portfolio_vs_benchmark
-- Description: Compare portfolio returns to benchmark
--
-- ISSUES FOR ARTEMIS TO OPTIMIZE:
-- 1. Re-joins data that could be joined once upstream
-- 2. Excess return calculation repeated
-- 3. Complex rolling calculations

with portfolio_returns as (
    select * from {{ ref('int_portfolio_returns_daily') }}
),

benchmark_aligned as (
    select * from {{ ref('int_benchmark_aligned') }}
    where is_primary = true
),

-- ISSUE: Another join that combines already-processed data
combined as (
    select
        pr.portfolio_id,
        pr.valuation_date,
        pr.nav,
        pr.nav_usd,
        pr.daily_return_mod_dietz as portfolio_daily_return,
        pr.cumulative_return as portfolio_cumulative_return,
        pr.return_1m as portfolio_return_1m,
        pr.return_3m as portfolio_return_3m,
        pr.return_1y as portfolio_return_1y,
        pr.volatility_1y as portfolio_volatility,
        ba.benchmark_id,
        ba.benchmark_daily_return,
        ba.benchmark_cumulative_return,
        ba.benchmark_return_30d as benchmark_return_1m,
        ba.benchmark_return_90d as benchmark_return_3m,
        ba.benchmark_return_1y,
        ba.benchmark_volatility
    from portfolio_returns pr
    left join benchmark_aligned ba
        on pr.portfolio_id = ba.portfolio_id
        and pr.valuation_date = ba.valuation_date
),

-- ISSUE: Excess return calculations
with_excess as (
    select
        *,
        portfolio_daily_return - coalesce(benchmark_daily_return, 0) as daily_excess_return,
        portfolio_cumulative_return - coalesce(benchmark_cumulative_return, 0) as cumulative_excess_return,
        portfolio_return_1m - coalesce(benchmark_return_1m, 0) as excess_return_1m,
        portfolio_return_3m - coalesce(benchmark_return_3m, 0) as excess_return_3m,
        portfolio_return_1y - coalesce(benchmark_return_1y, 0) as excess_return_1y
    from combined
),

-- ISSUE: Rolling tracking error calculation
with_tracking_error as (
    select
        *,
        stddev(daily_excess_return) over (
            partition by portfolio_id
            order by valuation_date
            rows between 251 preceding and current row
        ) * sqrt(252) as tracking_error_1y,
        avg(daily_excess_return) over (
            partition by portfolio_id
            order by valuation_date
            rows between 251 preceding and current row
        ) * 252 as annualized_alpha
    from with_excess
),

-- ISSUE: Information ratio
final as (
    select
        *,
        case
            when tracking_error_1y > 0
            then annualized_alpha / tracking_error_1y
            else null
        end as information_ratio
    from with_tracking_error
)

select * from final
