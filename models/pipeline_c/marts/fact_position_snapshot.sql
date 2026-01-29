-- Pipeline C: Complex Portfolio Analytics
-- Model: fact_position_snapshot
-- Description: Position-level fact table with attribution
--
-- ISSUES FOR ARTEMIS TO OPTIMIZE:
-- 1. Heavy joins that duplicate upstream work
-- 2. Could be more selective in columns

with position_attribution as (
    select * from {{ ref('int_position_attribution') }}
),

portfolios as (
    select * from {{ ref('stg_portfolios') }}
),

-- ISSUE: Re-joining portfolio data
final as (
    select
        {{ dbt_utils.generate_surrogate_key(['pa.portfolio_id', 'pa.security_id', 'pa.position_date']) }} as position_snapshot_key,
        p.portfolio_name,
        p.portfolio_type,
        p.fund_id,
        pa.portfolio_id,
        pa.security_id,
        pa.position_date,
        pa.ticker,
        pa.security_type,
        pa.asset_class,
        pa.sector,
        pa.industry,
        pa.quantity,
        pa.market_value_usd,
        pa.weight_pct,
        pa.close_price,
        pa.ma_20,
        pa.ma_50,
        pa.volatility_20d,
        pa.security_return,
        pa.contribution_to_return,
        pa.allocation_effect,
        pa.position_pnl,
        -- ISSUE: More date extractions
        extract(year from pa.position_date) as position_year,
        extract(month from pa.position_date) as position_month,
        date_trunc('month', pa.position_date) as position_month_start
    from position_attribution pa
    inner join portfolios p
        on pa.portfolio_id = p.portfolio_id
)

select * from final
