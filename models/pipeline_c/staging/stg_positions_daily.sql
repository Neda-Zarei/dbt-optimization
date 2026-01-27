-- Pipeline C: Complex Portfolio Analytics
-- Model: stg_positions_daily
-- Description: Daily position snapshots from source system
--
-- ISSUES FOR ARTEMIS TO OPTIMIZE:
-- 1. Heavy transformations before filtering
-- 2. Unnecessary type conversions
-- 3. Could push filters upstream

with source as (
    select
        position_id,
        portfolio_id,
        security_id,
        position_date,
        quantity,
        market_value_local,
        market_value_usd,
        cost_basis_local,
        cost_basis_usd,
        unrealized_pnl_local,
        unrealized_pnl_usd,
        weight_pct,
        currency,
        fx_rate,
        created_at
    from {{ source('raw', 'positions_daily') }}
),

-- ISSUE: Transformations applied to all rows before filter
transformed as (
    select
        position_id,
        portfolio_id,
        security_id,
        cast(position_date as date) as position_date,
        cast(quantity as decimal(18,6)) as quantity,
        cast(market_value_local as decimal(18,2)) as market_value_local,
        cast(market_value_usd as decimal(18,2)) as market_value_usd,
        cast(cost_basis_local as decimal(18,2)) as cost_basis_local,
        cast(cost_basis_usd as decimal(18,2)) as cost_basis_usd,
        cast(unrealized_pnl_local as decimal(18,2)) as unrealized_pnl_local,
        cast(unrealized_pnl_usd as decimal(18,2)) as unrealized_pnl_usd,
        cast(weight_pct as decimal(10,6)) as weight_pct,
        upper(currency) as currency,
        cast(fx_rate as decimal(18,8)) as fx_rate,
        created_at
    from source
),

-- ISSUE: Filter applied last
filtered as (
    select *
    from transformed
    where position_date >= '{{ var("start_date") }}'
)

select * from filtered
