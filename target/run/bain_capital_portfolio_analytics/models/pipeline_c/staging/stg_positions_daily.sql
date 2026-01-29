
  create or replace   view DBT_DEMO.DEV_pipeline_c.stg_positions_daily
  
  
  
  
  as (
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
        cost_basis_price,
        cost_basis_value,
        market_price,
        market_value,
        market_value_usd,
        unrealized_pnl,
        unrealized_pnl_pct,
        weight_pct,
        created_at,
        updated_at
    from DBT_DEMO.DEV.positions_daily
),

-- ISSUE: Transformations applied to all rows before filter
transformed as (
    select
        position_id,
        portfolio_id,
        security_id,
        cast(position_date as date) as position_date,
        cast(quantity as decimal(18,6)) as quantity,
        cast(cost_basis_price as decimal(18,4)) as cost_basis_price,
        cast(cost_basis_value as decimal(18,2)) as cost_basis_value,
        cast(market_price as decimal(18,4)) as market_price,
        cast(market_value as decimal(18,2)) as market_value,
        cast(market_value_usd as decimal(18,2)) as market_value_usd,
        cast(unrealized_pnl as decimal(18,2)) as unrealized_pnl,
        cast(unrealized_pnl_pct as decimal(10,4)) as unrealized_pnl_pct,
        cast(market_value_usd as decimal(18,2))
            / nullif(sum(cast(market_value_usd as decimal(18,2))) over (
                partition by portfolio_id, cast(position_date as date)
            ), 0) as weight_pct,
        created_at,
        updated_at
    from source
),

-- ISSUE: Filter applied last
filtered as (
    select *
    from transformed
    where position_date >= '2020-01-01'
)

select * from filtered
  );

