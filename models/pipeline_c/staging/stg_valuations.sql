-- Pipeline C: Complex Portfolio Analytics
-- Model: stg_valuations
-- Description: Portfolio valuation data (NAV, etc.)
--
-- ISSUES FOR ARTEMIS TO OPTIMIZE:
-- 1. Deduplication via subquery
-- 2. Heavy calculations before filtering

with source as (
    select
        valuation_id,
        portfolio_id,
        valuation_date,
        nav,
        nav_per_share,
        shares_outstanding,
        gross_assets,
        total_liabilities,
        net_assets,
        currency,
        fx_rate_to_usd,
        nav_usd,
        created_at,
        updated_at
    from {{ source('raw', 'valuations') }}
),

-- ISSUE: Deduplication using subquery
deduplicated as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by portfolio_id, valuation_date
                order by updated_at desc
            ) as rn
        from source
    )
    where rn = 1
),

-- ISSUE: Filter applied after deduplication
filtered as (
    select
        valuation_id,
        portfolio_id,
        cast(valuation_date as date) as valuation_date,
        cast(nav as decimal(18,2)) as nav,
        cast(nav_per_share as decimal(18,6)) as nav_per_share,
        cast(shares_outstanding as decimal(18,6)) as shares_outstanding,
        cast(gross_assets as decimal(18,2)) as gross_assets,
        cast(total_liabilities as decimal(18,2)) as total_liabilities,
        cast(net_assets as decimal(18,2)) as net_assets,
        upper(currency) as currency,
        cast(fx_rate_to_usd as decimal(18,8)) as fx_rate_to_usd,
        cast(nav_usd as decimal(18,2)) as nav_usd,
        created_at,
        updated_at
    from deduplicated
    where valuation_date >= '{{ var("start_date") }}'
)

select * from filtered
