-- Pipeline C: Complex Portfolio Analytics
-- Model: stg_portfolio_benchmarks
-- Description: Mapping of portfolios to their benchmarks

with source as (
    select
        portfolio_id,
        benchmark_id,
        is_primary,
        start_date,
        end_date,
        created_at,
        updated_at
    from {{ source('raw', 'portfolio_benchmarks') }}
)

select
    portfolio_id,
    benchmark_id,
    is_primary,
    cast(start_date as date) as start_date,
    cast(end_date as date) as end_date,
    created_at,
    updated_at
from source
where end_date is null or end_date >= current_date()
