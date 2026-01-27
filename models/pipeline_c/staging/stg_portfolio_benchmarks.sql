-- Pipeline C: Complex Portfolio Analytics
-- Model: stg_portfolio_benchmarks
-- Description: Mapping of portfolios to their benchmarks

with source as (
    select
        portfolio_id,
        benchmark_id,
        benchmark_weight,
        effective_from,
        effective_to,
        is_primary,
        created_at
    from {{ source('raw', 'portfolio_benchmarks') }}
)

select
    portfolio_id,
    benchmark_id,
    cast(benchmark_weight as decimal(5,4)) as benchmark_weight,
    cast(effective_from as date) as effective_from,
    cast(effective_to as date) as effective_to,
    is_primary,
    created_at
from source
where effective_to is null or effective_to >= current_date()
