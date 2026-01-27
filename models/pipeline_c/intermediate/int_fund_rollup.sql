-- Pipeline C: Complex Portfolio Analytics
-- Model: int_fund_rollup
-- Description: Roll up portfolio metrics to fund level
--
-- ISSUES FOR ARTEMIS TO OPTIMIZE:
-- 1. Recursive-like hierarchy traversal
-- 2. Multiple aggregation levels
-- 3. Heavy joins

with fund_hierarchy as (
    select * from {{ ref('stg_fund_hierarchy') }}
),

portfolios as (
    select * from {{ ref('stg_portfolios') }}
),

risk_metrics as (
    select * from {{ ref('int_risk_metrics') }}
),

-- Get portfolio to fund mapping
portfolio_fund_map as (
    select
        p.portfolio_id,
        p.portfolio_name,
        p.fund_id,
        fh.entity_name as fund_name,
        fh.parent_entity_id,
        fh.hierarchy_level
    from portfolios p
    left join fund_hierarchy fh
        on p.fund_id = fh.entity_id
),

-- Get latest risk metrics per portfolio
latest_metrics as (
    select *
    from (
        select
            *,
            row_number() over (partition by portfolio_id order by valuation_date desc) as rn
        from risk_metrics
    )
    where rn = 1  -- ISSUE: Should use QUALIFY
),

-- ISSUE: Join and aggregate
fund_aggregated as (
    select
        pfm.fund_id,
        pfm.fund_name,
        pfm.parent_entity_id,
        pfm.hierarchy_level,
        lm.valuation_date,
        count(distinct pfm.portfolio_id) as portfolio_count,
        sum(lm.nav_usd) as total_nav_usd,
        -- Weighted average metrics
        sum(lm.nav_usd * lm.annualized_return_1y) / nullif(sum(lm.nav_usd), 0) as weighted_return_1y,
        sum(lm.nav_usd * lm.volatility_1y) / nullif(sum(lm.nav_usd), 0) as weighted_volatility_1y,
        sum(lm.nav_usd * lm.sharpe_ratio) / nullif(sum(lm.nav_usd), 0) as weighted_sharpe_ratio,
        min(lm.max_drawdown) as worst_drawdown,
        sum(lm.var_95_1d) as total_var_95
    from portfolio_fund_map pfm
    inner join latest_metrics lm
        on pfm.portfolio_id = lm.portfolio_id
    group by 1, 2, 3, 4, 5
)

select * from fund_aggregated
