-- Pipeline C: Complex Portfolio Analytics
-- Model: int_benchmark_aligned
-- Description: Align benchmark returns with portfolio dates for comparison
--
-- ISSUES FOR ARTEMIS TO OPTIMIZE:
-- 1. Heavy join between portfolios and benchmarks
-- 2. Could pre-filter benchmarks

with portfolio_dates as (
    select distinct
        portfolio_id,
        valuation_date
    from {{ ref('stg_valuations') }}
),

portfolio_benchmarks as (
    select * from {{ ref('stg_portfolio_benchmarks') }}
),

benchmark_returns as (
    select * from {{ ref('stg_benchmark_returns') }}
),

-- ISSUE: Cross-join like pattern (portfolio dates x benchmarks)
aligned as (
    select
        pd.portfolio_id,
        pd.valuation_date,
        pb.benchmark_id,
        pb.benchmark_weight,
        pb.is_primary,
        br.daily_return as benchmark_daily_return,
        br.cumulative_return as benchmark_cumulative_return,
        br.return_30d as benchmark_return_30d,
        br.return_90d as benchmark_return_90d,
        br.return_1y as benchmark_return_1y,
        br.annualized_volatility as benchmark_volatility
    from portfolio_dates pd
    inner join portfolio_benchmarks pb
        on pd.portfolio_id = pb.portfolio_id
        and pd.valuation_date >= pb.effective_from
        and (pb.effective_to is null or pd.valuation_date <= pb.effective_to)
    left join benchmark_returns br
        on pb.benchmark_id = br.benchmark_id
        and pd.valuation_date = br.return_date
)

select * from aligned
