-- Pipeline C: Complex Portfolio Analytics
-- Model: fact_portfolio_performance
-- Description: Main performance fact table
-- DEPENDENCIES:
--   - Pipeline A: fact_cashflow_summary (for cashflow context)
--   - Pipeline B: fact_trade_summary, fact_portfolio_positions (for trading/position context)
--
-- ISSUES FOR ARTEMIS TO OPTIMIZE:
-- 1. Multiple self-joins for period comparisons
-- 2. Repeated window functions with same partitions
-- 3. Complex CASE statements repeated multiple times
-- 4. Correlated subqueries
-- 5. Late filtering and unnecessary full table scans

with portfolio_vs_benchmark as (
    select * from {{ ref('int_portfolio_vs_benchmark') }}
),

risk_metrics as (
    select * from {{ ref('int_risk_metrics') }}
),

portfolios as (
    select * from {{ ref('stg_portfolios') }}
),

-- DEPENDENCY ON PIPELINE A: Cashflow summary for capital deployment context
cashflow_summary as (
    select * from {{ ref('fact_cashflow_summary') }}
),

-- Aggregate cashflows to portfolio level
portfolio_cashflow_totals as (
    select
        portfolio_id,
        sum(case when cashflow_type = 'CONTRIBUTION' then cumulative_total else 0 end) as total_contributions,
        sum(case when cashflow_type = 'DISTRIBUTION' then abs(cumulative_total) else 0 end) as total_distributions,
        sum(cumulative_total) as net_cashflow
    from cashflow_summary
    group by portfolio_id
),

-- DEPENDENCY ON PIPELINE B: Trade summary for trading activity context
trade_summary as (
    select * from {{ ref('fact_trade_summary') }}
),

-- Aggregate trades to portfolio/date level
portfolio_trade_activity as (
    select
        portfolio_id,
        trade_month_start as activity_month,
        count(*) as trade_count,
        sum(case when trade_category = 'PURCHASE' then abs(net_amount) else 0 end) as total_purchases,
        sum(case when trade_category = 'SALE' then abs(net_amount) else 0 end) as total_sales,
        sum(coalesce(realized_pnl, 0)) as realized_pnl
    from trade_summary
    group by portfolio_id, trade_month_start
),

-- DEPENDENCY ON PIPELINE B: Position snapshot for current holdings context
portfolio_positions as (
    select * from {{ ref('fact_portfolio_positions') }}
),

-- Aggregate positions to portfolio level
portfolio_position_totals as (
    select
        portfolio_id,
        sum(market_value) as total_market_value,
        sum(cost_basis_value) as total_cost_basis,
        sum(unrealized_pnl) as total_unrealized_pnl,
        count(*) as position_count
    from portfolio_positions
    group by portfolio_id
),

-- ISSUE: Another join when data could flow from upstream
combined as (
    select
        pvb.portfolio_id,
        pvb.valuation_date,
        pvb.nav,
        pvb.nav_usd,
        -- Portfolio returns
        pvb.portfolio_daily_return,
        pvb.portfolio_cumulative_return,
        pvb.portfolio_return_1m,
        pvb.portfolio_return_3m,
        pvb.portfolio_return_1y,
        pvb.portfolio_volatility,
        -- Benchmark comparison
        pvb.benchmark_id,
        pvb.benchmark_daily_return,
        pvb.benchmark_cumulative_return,
        pvb.benchmark_return_1m,
        pvb.benchmark_return_3m,
        pvb.benchmark_return_1y,
        pvb.benchmark_volatility,
        -- Relative performance
        pvb.daily_excess_return,
        pvb.cumulative_excess_return,
        pvb.excess_return_1m,
        pvb.excess_return_3m,
        pvb.excess_return_1y,
        pvb.tracking_error_1y,
        pvb.annualized_alpha,
        pvb.information_ratio,
        -- Risk metrics
        rm.drawdown,
        rm.max_drawdown,
        rm.downside_deviation_1y,
        rm.sharpe_ratio,
        rm.sortino_ratio,
        rm.var_95_1d,
        rm.var_99_1d
    from portfolio_vs_benchmark pvb
    inner join risk_metrics rm
        on pvb.portfolio_id = rm.portfolio_id
        and pvb.valuation_date = rm.valuation_date
),

-- ISSUE: Self-join for prior period comparisons (should use LAG)
with_prior_periods as (
    select
        c.*,
        -- ISSUE: Self-join for 1 day ago
        c1d.nav_usd as nav_1d_ago,
        c1d.portfolio_cumulative_return as return_1d_ago,
        -- ISSUE: Self-join for 1 week ago
        c1w.nav_usd as nav_1w_ago,
        c1w.portfolio_cumulative_return as return_1w_ago,
        -- ISSUE: Self-join for 1 month ago
        c1m.nav_usd as nav_1m_ago,
        c1m.portfolio_cumulative_return as return_1m_ago,
        -- ISSUE: Self-join for 3 months ago
        c3m.nav_usd as nav_3m_ago,
        c3m.portfolio_cumulative_return as return_3m_ago,
        -- ISSUE: Self-join for 1 year ago
        c1y.nav_usd as nav_1y_ago,
        c1y.portfolio_cumulative_return as return_1y_ago
    from combined c
    left join combined c1d
        on c.portfolio_id = c1d.portfolio_id
        and c1d.valuation_date = dateadd(day, -1, c.valuation_date)
    left join combined c1w
        on c.portfolio_id = c1w.portfolio_id
        and c1w.valuation_date = dateadd(day, -7, c.valuation_date)
    left join combined c1m
        on c.portfolio_id = c1m.portfolio_id
        and c1m.valuation_date = dateadd(month, -1, c.valuation_date)
    left join combined c3m
        on c.portfolio_id = c3m.portfolio_id
        and c3m.valuation_date = dateadd(month, -3, c.valuation_date)
    left join combined c1y
        on c.portfolio_id = c1y.portfolio_id
        and c1y.valuation_date = dateadd(year, -1, c.valuation_date)
),

-- ISSUE: Multiple window functions with repeated partitions
with_rankings as (
    select
        wpp.*,
        -- ISSUE: Multiple ROW_NUMBER with same partition
        row_number() over (
            partition by wpp.portfolio_id
            order by wpp.portfolio_cumulative_return desc
        ) as best_performance_rank,
        row_number() over (
            partition by wpp.portfolio_id
            order by wpp.portfolio_cumulative_return asc
        ) as worst_performance_rank,
        row_number() over (
            partition by wpp.portfolio_id
            order by wpp.sharpe_ratio desc nulls last
        ) as best_sharpe_rank,
        -- ISSUE: DENSE_RANK with same partition
        dense_rank() over (
            partition by wpp.portfolio_id
            order by wpp.nav_usd desc
        ) as nav_size_rank,
        -- ISSUE: More window calculations
        avg(wpp.portfolio_daily_return) over (
            partition by wpp.portfolio_id
            order by wpp.valuation_date
            rows between 20 preceding and current row
        ) as rolling_20d_avg_return,
        avg(wpp.portfolio_daily_return) over (
            partition by wpp.portfolio_id
            order by wpp.valuation_date
            rows between 60 preceding and current row
        ) as rolling_60d_avg_return,
        stddev(wpp.portfolio_daily_return) over (
            partition by wpp.portfolio_id
            order by wpp.valuation_date
            rows between 20 preceding and current row
        ) as rolling_20d_volatility,
        stddev(wpp.portfolio_daily_return) over (
            partition by wpp.portfolio_id
            order by wpp.valuation_date
            rows between 60 preceding and current row
        ) as rolling_60d_volatility
    from with_prior_periods wpp
),

-- ISSUE: Complex derived metrics with repeated CASE statements
with_derived_metrics as (
    select
        wr.*,
        -- ISSUE: Repeated complex CASE for performance classification
        case
            when wr.portfolio_cumulative_return >= 0.50 then 'EXCEPTIONAL'
            when wr.portfolio_cumulative_return >= 0.30 then 'EXCELLENT'
            when wr.portfolio_cumulative_return >= 0.15 then 'VERY_GOOD'
            when wr.portfolio_cumulative_return >= 0.05 then 'GOOD'
            when wr.portfolio_cumulative_return >= 0.00 then 'NEUTRAL'
            when wr.portfolio_cumulative_return >= -0.05 then 'POOR'
            when wr.portfolio_cumulative_return >= -0.15 then 'VERY_POOR'
            else 'UNACCEPTABLE'
        end as performance_rating,
        -- ISSUE: Same CASE for risk classification
        case
            when wr.sharpe_ratio >= 3.0 then 'EXCEPTIONAL'
            when wr.sharpe_ratio >= 2.0 then 'EXCELLENT'
            when wr.sharpe_ratio >= 1.5 then 'VERY_GOOD'
            when wr.sharpe_ratio >= 1.0 then 'GOOD'
            when wr.sharpe_ratio >= 0.5 then 'NEUTRAL'
            when wr.sharpe_ratio >= 0.0 then 'POOR'
            else 'VERY_POOR'
        end as risk_adjusted_rating,
        -- ISSUE: Complex calculation repeated
        case
            when wr.nav_1m_ago is not null and wr.nav_1m_ago > 0
            then (wr.nav_usd - wr.nav_1m_ago) / wr.nav_1m_ago
            else null
        end as nav_change_1m_pct,
        case
            when wr.nav_3m_ago is not null and wr.nav_3m_ago > 0
            then (wr.nav_usd - wr.nav_3m_ago) / wr.nav_3m_ago
            else null
        end as nav_change_3m_pct,
        case
            when wr.nav_1y_ago is not null and wr.nav_1y_ago > 0
            then (wr.nav_usd - wr.nav_1y_ago) / wr.nav_1y_ago
            else null
        end as nav_change_1y_pct,
        -- ISSUE: Momentum indicators with repeated logic
        case
            when wr.rolling_20d_avg_return > wr.rolling_60d_avg_return then 'POSITIVE_MOMENTUM'
            when wr.rolling_20d_avg_return < wr.rolling_60d_avg_return then 'NEGATIVE_MOMENTUM'
            else 'NEUTRAL_MOMENTUM'
        end as momentum_signal,
        -- ISSUE: Volatility regime classification
        case
            when wr.rolling_20d_volatility > wr.rolling_60d_volatility * 1.5 then 'HIGH_VOLATILITY'
            when wr.rolling_20d_volatility > wr.rolling_60d_volatility * 1.2 then 'ELEVATED_VOLATILITY'
            when wr.rolling_20d_volatility < wr.rolling_60d_volatility * 0.8 then 'LOW_VOLATILITY'
            else 'NORMAL_VOLATILITY'
        end as volatility_regime
    from with_rankings wr
),

-- ISSUE: Separate peer aggregation CTEs (should use window functions with partition by portfolio_type)
peer_return_aggs as (
    select
        p2.portfolio_type,
        pvb2.valuation_date,
        avg(pvb2.portfolio_cumulative_return) as peer_avg_return
    from portfolio_vs_benchmark pvb2
    inner join portfolios p2
        on pvb2.portfolio_id = p2.portfolio_id
    group by 1, 2
),

peer_sharpe_aggs as (
    select
        p2.portfolio_type,
        rm2.valuation_date,
        percentile_cont(0.5) within group (order by rm2.sharpe_ratio) as peer_median_sharpe
    from risk_metrics rm2
    inner join portfolios p2
        on rm2.portfolio_id = p2.portfolio_id
    group by 1, 2
),

with_peer_comparison as (
    select
        wdm.*,
        pra.peer_avg_return,
        psa.peer_median_sharpe
    from with_derived_metrics wdm
    inner join portfolios p_self
        on wdm.portfolio_id = p_self.portfolio_id
    left join peer_return_aggs pra
        on p_self.portfolio_type = pra.portfolio_type
        and wdm.valuation_date = pra.valuation_date
    left join peer_sharpe_aggs psa
        on p_self.portfolio_type = psa.portfolio_type
        and wdm.valuation_date = psa.valuation_date
),

-- ISSUE: Portfolio attributes added last
final as (
    select
        {{ dbt_utils.generate_surrogate_key(['wpc.portfolio_id', 'wpc.valuation_date']) }} as performance_key,
        p.portfolio_name,
        p.portfolio_type,
        p.fund_id,
        wpc.*,
        -- ISSUE: Date dimensions recalculated (should be in dim table)
        extract(year from wpc.valuation_date) as valuation_year,
        extract(month from wpc.valuation_date) as valuation_month,
        extract(quarter from wpc.valuation_date) as valuation_quarter,
        extract(dayofweek from wpc.valuation_date) as valuation_day_of_week,
        extract(dayofyear from wpc.valuation_date) as valuation_day_of_year,
        date_trunc('month', wpc.valuation_date) as valuation_month_start,
        date_trunc('quarter', wpc.valuation_date) as valuation_quarter_start,
        date_trunc('year', wpc.valuation_date) as valuation_year_start,
        -- ISSUE: String concatenations (slow)
        concat(p.portfolio_name, ' - ', wpc.valuation_date::varchar) as display_name,
        concat('Q', extract(quarter from wpc.valuation_date), ' ', extract(year from wpc.valuation_date)) as quarter_label,
        -- ISSUE: Complex derived field
        case
            when wpc.portfolio_cumulative_return > wpc.peer_avg_return then 'OUTPERFORMING'
            when wpc.portfolio_cumulative_return < wpc.peer_avg_return then 'UNDERPERFORMING'
            else 'AT_PEER'
        end as peer_relative_performance
    from with_peer_comparison wpc
    inner join portfolios p
        on wpc.portfolio_id = p.portfolio_id
)

select * from final
