-- Pipeline A: Simple Cashflow Pipeline
-- Model: stg_portfolios
-- Description: Staging model for portfolio master data
--
-- ISSUES FOR ARTEMIS TO OPTIMIZE:
-- 1. Subquery for deduplication instead of QUALIFY
-- 2. Multiple passes over data

with source as (
    select
        portfolio_id,
        portfolio_name,
        portfolio_type,
        fund_id,
        inception_date,
        status,
        currency,
        created_at,
        updated_at,
        row_number() over (
            partition by portfolio_id
            order by updated_at desc
        ) as rn
    from {{ source('raw', 'portfolios') }}
),

-- ISSUE: Using subquery filter instead of QUALIFY
deduplicated as (
    select
        portfolio_id,
        portfolio_name,
        portfolio_type,
        fund_id,
        inception_date,
        status,
        currency,
        created_at,
        updated_at
    from source
    where rn = 1  -- ISSUE: Should use QUALIFY in Snowflake
),

-- ISSUE: Another pass just for active filter
active_only as (
    select *
    from deduplicated
    where status = 'ACTIVE'
)

select * from active_only
