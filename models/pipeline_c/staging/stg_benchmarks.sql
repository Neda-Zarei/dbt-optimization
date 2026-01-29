-- Pipeline C: Complex Portfolio Analytics
-- Model: stg_benchmarks
-- Description: Benchmark index data for performance comparison

with source as (
    select
        benchmark_id,
        benchmark_name,
        benchmark_ticker,
        asset_class,
        region,
        is_active,
        created_at,
        updated_at
    from {{ source('raw', 'benchmarks') }}
),

-- ISSUE: Subquery for deduplication
deduplicated as (
    select *
    from (
        select
            *,
            row_number() over (partition by benchmark_id order by updated_at desc) as rn
        from source
    )
    where rn = 1
)

select
    benchmark_id,
    trim(benchmark_name) as benchmark_name,
    upper(trim(benchmark_ticker)) as benchmark_ticker,
    upper(asset_class) as asset_class,
    upper(region) as region,
    is_active,
    created_at,
    updated_at
from deduplicated
where is_active = true
