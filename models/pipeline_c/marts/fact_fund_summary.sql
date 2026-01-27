-- Pipeline C: Complex Portfolio Analytics
-- Model: fact_fund_summary
-- Description: Fund-level summary metrics
--
-- ISSUES FOR ARTEMIS TO OPTIMIZE:
-- 1. Based on already-aggregated data
-- 2. Could push more logic upstream

with fund_rollup as (
    select * from {{ ref('int_fund_rollup') }}
),

fund_hierarchy as (
    select * from {{ ref('stg_fund_hierarchy') }}
),

-- Get parent fund info
with_parent as (
    select
        fr.*,
        parent.entity_name as parent_fund_name
    from fund_rollup fr
    left join fund_hierarchy parent
        on fr.parent_entity_id = parent.entity_id
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['fund_id', 'valuation_date']) }} as fund_summary_key,
        fund_id,
        fund_name,
        parent_entity_id as parent_fund_id,
        parent_fund_name,
        hierarchy_level,
        valuation_date,
        portfolio_count,
        total_nav_usd,
        weighted_return_1y,
        weighted_volatility_1y,
        weighted_sharpe_ratio,
        worst_drawdown,
        total_var_95,
        -- ISSUE: Calculated fields
        case
            when weighted_return_1y >= 0.15 then 'HIGH'
            when weighted_return_1y >= 0.08 then 'MEDIUM'
            when weighted_return_1y >= 0 then 'LOW'
            else 'NEGATIVE'
        end as return_tier,
        case
            when weighted_sharpe_ratio >= 1.5 then 'EXCELLENT'
            when weighted_sharpe_ratio >= 1.0 then 'GOOD'
            when weighted_sharpe_ratio >= 0.5 then 'FAIR'
            else 'POOR'
        end as risk_adjusted_tier
    from with_parent
)

select * from final
