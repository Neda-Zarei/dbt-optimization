-- Pipeline C: Complex Portfolio Analytics
-- Model: stg_benchmark_returns
-- Description: Daily benchmark return data
--
-- ISSUES FOR ARTEMIS TO OPTIMIZE:
-- 1. Self-join for cumulative returns (inefficient)
-- 2. Multiple window functions

with source as (
    select
        benchmark_id,
        return_date,
        daily_return,
        index_level,
        created_at
    from {{ source('raw', 'benchmark_returns') }}
    where return_date >= '{{ var("start_date") }}'
),

-- ISSUE: Multiple window functions that could be consolidated
with_cumulative as (
    select
        benchmark_id,
        return_date,
        daily_return,
        index_level,
        -- ISSUE: Separate window functions for each period
        exp(sum(ln(1 + daily_return)) over (
            partition by benchmark_id
            order by return_date
            rows between unbounded preceding and current row
        )) - 1 as cumulative_return,
        exp(sum(ln(1 + daily_return)) over (
            partition by benchmark_id
            order by return_date
            rows between 29 preceding and current row
        )) - 1 as return_30d,
        exp(sum(ln(1 + daily_return)) over (
            partition by benchmark_id
            order by return_date
            rows between 89 preceding and current row
        )) - 1 as return_90d,
        exp(sum(ln(1 + daily_return)) over (
            partition by benchmark_id
            order by return_date
            rows between 364 preceding and current row
        )) - 1 as return_1y,
        stddev(daily_return) over (
            partition by benchmark_id
            order by return_date
            rows between 251 preceding and current row
        ) * sqrt(252) as annualized_volatility,
        created_at
    from source
)

select * from with_cumulative
