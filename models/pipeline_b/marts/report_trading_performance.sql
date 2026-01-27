-- Pipeline B: Trade Analytics Pipeline
-- Model: report_trading_performance
-- Description: Trading performance report for IC dashboard
--
-- ISSUES FOR ARTEMIS TO OPTIMIZE:
-- 1. Re-aggregates fact data that could be pre-computed
-- 2. Complex window functions repeated from other models
-- 3. Multiple CTEs that could be consolidated

with trades as (
    select * from {{ ref('fact_trade_summary') }}
),

positions as (
    select * from {{ ref('fact_portfolio_positions') }}
),

-- ISSUE: Re-aggregating trade data by portfolio/month
trade_metrics as (
    select
        portfolio_id,
        portfolio_name,
        portfolio_type,
        fund_id,
        trade_year,
        trade_month,
        count(distinct trade_id) as trade_count,
        count(distinct security_id) as securities_traded,
        sum(case when trade_category = 'PURCHASE' then 1 else 0 end) as buy_count,
        sum(case when trade_category = 'SALE' then 1 else 0 end) as sell_count,
        sum(case when trade_category = 'PURCHASE' then net_amount else 0 end) as total_purchases,
        sum(case when trade_category = 'SALE' then abs(net_amount) else 0 end) as total_sales,
        sum(coalesce(realized_pnl, 0)) as total_realized_pnl,
        avg(case when realized_pnl is not null then realized_pnl_pct else null end) as avg_realized_return_pct
    from trades
    group by 1,2,3,4,5,6
),

-- ISSUE: Aggregating positions separately
position_metrics as (
    select
        portfolio_id,
        count(distinct security_id) as position_count,
        sum(market_value) as total_market_value,
        sum(cost_basis_value) as total_cost_basis,
        sum(unrealized_pnl) as total_unrealized_pnl,
        avg(unrealized_pnl_pct) as avg_unrealized_return_pct
    from positions
    group by 1
),

-- ISSUE: Window functions for running totals (repeated pattern)
with_running_totals as (
    select
        tm.*,
        sum(total_realized_pnl) over (
            partition by tm.portfolio_id
            order by tm.trade_year, tm.trade_month
            rows between unbounded preceding and current row
        ) as cumulative_realized_pnl,
        sum(total_purchases) over (
            partition by tm.portfolio_id
            order by tm.trade_year, tm.trade_month
            rows between unbounded preceding and current row
        ) as cumulative_invested,
        -- ISSUE: Multiple LAG functions
        lag(total_realized_pnl, 1) over (
            partition by tm.portfolio_id
            order by tm.trade_year, tm.trade_month
        ) as prior_month_pnl,
        lag(trade_count, 1) over (
            partition by tm.portfolio_id
            order by tm.trade_year, tm.trade_month
        ) as prior_month_trades
    from trade_metrics tm
),

-- ISSUE: Final join adds more complexity
final as (
    select
        wrt.*,
        pm.position_count,
        pm.total_market_value,
        pm.total_cost_basis,
        pm.total_unrealized_pnl,
        pm.avg_unrealized_return_pct,
        -- Combined metrics
        wrt.total_realized_pnl + coalesce(pm.total_unrealized_pnl, 0) as total_pnl,
        case
            when pm.total_cost_basis > 0
            then ((pm.total_market_value + wrt.cumulative_realized_pnl) - pm.total_cost_basis) / pm.total_cost_basis * 100
            else null
        end as total_return_pct
    from with_running_totals wrt
    left join position_metrics pm
        on wrt.portfolio_id = pm.portfolio_id
)

select * from final
order by portfolio_id, trade_year, trade_month
