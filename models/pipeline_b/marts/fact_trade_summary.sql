-- Pipeline B: Trade Analytics Pipeline
-- Model: fact_trade_summary
-- Description: Fact table for trade-level analysis
--
-- ISSUES FOR ARTEMIS TO OPTIMIZE:
-- 1. Self-joins for prior trade lookups (should use LAG)
-- 2. Repeated window functions with same partitions
-- 3. Correlated subqueries for security-level aggregations
-- 4. Complex CASE statements repeated multiple times

with trade_pnl as (
    select * from {{ ref('int_trade_pnl') }}
),

-- ISSUE: Getting portfolio data again (already available through joins upstream)
portfolios as (
    select * from {{ ref('stg_portfolios') }}
),

-- ISSUE: Join that adds overhead
enriched as (
    select
        t.trade_id,
        t.portfolio_id,
        p.portfolio_name,
        p.portfolio_type,
        p.fund_id,
        t.security_id,
        t.ticker,
        t.security_name,
        t.security_type,
        t.asset_class,
        t.sector,
        t.trade_date,
        t.trade_type,
        t.trade_category,
        t.quantity,
        t.execution_price,
        t.net_amount,
        t.commission,
        t.running_position,
        t.avg_cost_basis,
        t.realized_pnl,
        t.realized_pnl_pct,
        -- ISSUE: Redundant date extractions (already done upstream)
        extract(year from t.trade_date) as trade_year,
        extract(month from t.trade_date) as trade_month,
        extract(quarter from t.trade_date) as trade_quarter,
        extract(dayofweek from t.trade_date) as trade_day_of_week,
        date_trunc('week', t.trade_date) as trade_week,
        date_trunc('month', t.trade_date) as trade_month_start
    from trade_pnl t
    left join portfolios p
        on t.portfolio_id = p.portfolio_id
),

-- ISSUE: Self-joins for prior trade comparisons (should use LAG)
with_prior_trades as (
    select
        e.*,
        -- ISSUE: Self-join for prior trade same security
        pt1.execution_price as prior_trade_price,
        pt1.trade_date as prior_trade_date,
        pt1.quantity as prior_trade_quantity,
        -- ISSUE: Self-join for 5 trades ago
        pt5.execution_price as price_5_trades_ago,
        -- ISSUE: Self-join for 10 trades ago
        pt10.execution_price as price_10_trades_ago
    from enriched e
    left join enriched pt1
        on e.portfolio_id = pt1.portfolio_id
        and e.security_id = pt1.security_id
        and pt1.trade_date < e.trade_date
        and pt1.trade_date = (
            select max(pt_sub.trade_date)
            from enriched pt_sub
            where pt_sub.portfolio_id = e.portfolio_id
            and pt_sub.security_id = e.security_id
            and pt_sub.trade_date < e.trade_date
        )
    left join lateral (
        select execution_price
        from enriched pt_lateral
        where pt_lateral.portfolio_id = e.portfolio_id
        and pt_lateral.security_id = e.security_id
        and pt_lateral.trade_date < e.trade_date
        order by pt_lateral.trade_date desc
        limit 1 offset 4
    ) pt5 on true
    left join lateral (
        select execution_price
        from enriched pt_lateral
        where pt_lateral.portfolio_id = e.portfolio_id
        and pt_lateral.security_id = e.security_id
        and pt_lateral.trade_date < e.trade_date
        order by pt_lateral.trade_date desc
        limit 1 offset 9
    ) pt10 on true
),

-- ISSUE: Multiple window functions with repeated partitions
with_window_calcs as (
    select
        wpt.*,
        -- ISSUE: Running aggregations (repeated partition by portfolio_id, security_id)
        sum(quantity) over (
            partition by wpt.portfolio_id, wpt.security_id
            order by wpt.trade_date, wpt.trade_id
            rows between unbounded preceding and current row
        ) as cumulative_quantity,
        sum(abs(net_amount)) over (
            partition by wpt.portfolio_id, wpt.security_id
            order by wpt.trade_date, wpt.trade_id
            rows between unbounded preceding and current row
        ) as cumulative_trade_value,
        sum(coalesce(realized_pnl, 0)) over (
            partition by wpt.portfolio_id, wpt.security_id
            order by wpt.trade_date, wpt.trade_id
            rows between unbounded preceding and current row
        ) as cumulative_realized_pnl,
        -- ISSUE: Moving averages (same partition repeated)
        avg(execution_price) over (
            partition by wpt.portfolio_id, wpt.security_id
            order by wpt.trade_date, wpt.trade_id
            rows between 4 preceding and current row
        ) as rolling_5_trade_avg_price,
        avg(execution_price) over (
            partition by wpt.portfolio_id, wpt.security_id
            order by wpt.trade_date, wpt.trade_id
            rows between 9 preceding and current row
        ) as rolling_10_trade_avg_price,
        avg(execution_price) over (
            partition by wpt.portfolio_id, wpt.security_id
            order by wpt.trade_date, wpt.trade_id
            rows between 19 preceding and current row
        ) as rolling_20_trade_avg_price,
        -- ISSUE: More window calculations
        stddev(execution_price) over (
            partition by wpt.portfolio_id, wpt.security_id
            order by wpt.trade_date, wpt.trade_id
            rows between 9 preceding and current row
        ) as rolling_10_trade_price_stddev,
        count(*) over (
            partition by wpt.portfolio_id, wpt.security_id
            order by wpt.trade_date, wpt.trade_id
            rows between unbounded preceding and current row
        ) as trade_sequence_number,
        -- ISSUE: Rankings (same partition again)
        row_number() over (
            partition by wpt.portfolio_id, wpt.security_id, wpt.trade_category
            order by abs(wpt.net_amount) desc
        ) as size_rank_within_category
    from with_prior_trades wpt
),

-- ISSUE: Correlated subqueries for security-level context (very slow)
with_security_context as (
    select
        wwc.*,
        -- ISSUE: Correlated subquery for total portfolio trades on this security
        (
            select count(*)
            from enriched e2
            where e2.portfolio_id = wwc.portfolio_id
            and e2.security_id = wwc.security_id
            and e2.trade_date <= wwc.trade_date
        ) as total_portfolio_trades_this_security,
        -- ISSUE: Another correlated subquery for average portfolio price
        (
            select avg(e2.execution_price)
            from enriched e2
            where e2.portfolio_id = wwc.portfolio_id
            and e2.security_id = wwc.security_id
            and e2.trade_date <= wwc.trade_date
        ) as avg_portfolio_price_this_security,
        -- ISSUE: Fund-level aggregation (correlated)
        (
            select sum(abs(e2.net_amount))
            from enriched e2
            where e2.fund_id = wwc.fund_id
            and e2.security_id = wwc.security_id
            and e2.trade_date = wwc.trade_date
        ) as fund_total_volume_same_security_same_day
    from with_window_calcs wwc
),

-- ISSUE: Complex derived metrics with repeated CASE statements
final as (
    select
        {{ dbt_utils.generate_surrogate_key(['wsc.trade_id']) }} as trade_key,
        wsc.*,
        -- ISSUE: Price change calculations (repeated division logic)
        case
            when wsc.prior_trade_price is not null and wsc.prior_trade_price > 0
            then ((wsc.execution_price - wsc.prior_trade_price) / wsc.prior_trade_price) * 100
            else null
        end as price_change_from_prior_pct,
        case
            when wsc.price_5_trades_ago is not null and wsc.price_5_trades_ago > 0
            then ((wsc.execution_price - wsc.price_5_trades_ago) / wsc.price_5_trades_ago) * 100
            else null
        end as price_change_from_5_trades_ago_pct,
        case
            when wsc.rolling_20_trade_avg_price is not null and wsc.rolling_20_trade_avg_price > 0
            then ((wsc.execution_price - wsc.rolling_20_trade_avg_price) / wsc.rolling_20_trade_avg_price) * 100
            else null
        end as deviation_from_20_trade_avg_pct,
        -- ISSUE: Trade size classification (repeated CASE)
        case
            when abs(wsc.net_amount) >= 10000000 then 'BLOCK_TRADE'
            when abs(wsc.net_amount) >= 1000000 then 'LARGE'
            when abs(wsc.net_amount) >= 100000 then 'MEDIUM'
            when abs(wsc.net_amount) >= 10000 then 'SMALL'
            else 'MICRO'
        end as trade_size_category,
        -- ISSUE: Trade timing classification (complex nested CASE)
        case
            when wsc.execution_price > wsc.rolling_10_trade_avg_price * 1.1 then 'BOUGHT_HIGH'
            when wsc.execution_price > wsc.rolling_10_trade_avg_price * 1.03 then 'ABOVE_AVERAGE'
            when wsc.execution_price < wsc.rolling_10_trade_avg_price * 0.9 then 'BOUGHT_LOW'
            when wsc.execution_price < wsc.rolling_10_trade_avg_price * 0.97 then 'BELOW_AVERAGE'
            else 'AVERAGE'
        end as execution_quality,
        -- ISSUE: Momentum signal (repeated logic)
        case
            when wsc.rolling_5_trade_avg_price > wsc.rolling_20_trade_avg_price then 'UPTREND'
            when wsc.rolling_5_trade_avg_price < wsc.rolling_20_trade_avg_price then 'DOWNTREND'
            else 'NEUTRAL'
        end as price_momentum,
        -- ISSUE: Volatility classification
        case
            when wsc.rolling_10_trade_price_stddev < wsc.rolling_10_trade_avg_price * 0.02 then 'LOW_VOLATILITY'
            when wsc.rolling_10_trade_price_stddev < wsc.rolling_10_trade_avg_price * 0.05 then 'MODERATE_VOLATILITY'
            when wsc.rolling_10_trade_price_stddev < wsc.rolling_10_trade_avg_price * 0.10 then 'HIGH_VOLATILITY'
            else 'VERY_HIGH_VOLATILITY'
        end as price_volatility_regime,
        -- ISSUE: Trade frequency indicator
        case
            when wsc.trade_sequence_number >= 100 then 'VERY_ACTIVE'
            when wsc.trade_sequence_number >= 50 then 'ACTIVE'
            when wsc.trade_sequence_number >= 20 then 'MODERATE'
            when wsc.trade_sequence_number >= 5 then 'LIGHT'
            else 'FIRST_FEW_TRADES'
        end as trading_activity_level
    from with_security_context wsc
)

select * from final
