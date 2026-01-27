-- Pipeline C: Complex Portfolio Analytics
-- Model: report_lp_quarterly
-- Description: Quarterly LP reporting with period comparisons
--
-- ISSUES FOR ARTEMIS TO OPTIMIZE:
-- 1. Self-joins for multi-period comparisons (should use LAG)
-- 2. Repeated window functions with same partitions
-- 3. Correlated subqueries for fund-level aggregations
-- 4. Complex CASE statements repeated multiple times
-- 5. Late filtering after heavy computation

with portfolio_performance as (
    select * from {{ ref('fact_portfolio_performance') }}
),

cashflow_summary as (
    select * from {{ ref('fact_cashflow_summary') }}
),

-- ISSUE: Filter to quarter-end dates only (filtering late)
quarter_end_perf as (
    select *
    from portfolio_performance
    where valuation_date = last_day(valuation_date, 'quarter')
),

-- ISSUE: Aggregate cashflows to quarterly
quarterly_cashflows as (
    select
        portfolio_id,
        date_trunc('quarter', cashflow_month) as quarter_start,
        sum(case when cashflow_type = 'CONTRIBUTION' then total_amount else 0 end) as quarterly_contributions,
        sum(case when cashflow_type = 'DISTRIBUTION' then total_amount else 0 end) as quarterly_distributions,
        sum(case when cashflow_type = 'DIVIDEND' then total_amount else 0 end) as quarterly_dividends,
        sum(case when cashflow_type = 'FEE' then abs(total_amount) else 0 end) as quarterly_fees,
        sum(total_amount) as quarterly_net_cashflow,
        count(distinct transaction_count) as total_transactions
    from cashflow_summary
    group by 1, 2
),

-- ISSUE: Join performance and cashflows
combined as (
    select
        qep.portfolio_id,
        qep.portfolio_name,
        qep.portfolio_type,
        qep.fund_id,
        qep.valuation_date as quarter_end,
        qep.valuation_quarter_start as quarter_start,
        qep.valuation_year,
        qep.valuation_quarter,
        qep.nav_usd,
        qep.portfolio_return_3m as quarterly_return,
        qep.portfolio_return_1y,
        qep.portfolio_cumulative_return,
        qep.benchmark_return_3m as benchmark_quarterly_return,
        qep.excess_return_3m as quarterly_excess_return,
        qep.sharpe_ratio,
        qep.sortino_ratio,
        qep.max_drawdown,
        qep.performance_rating,
        qep.risk_adjusted_rating,
        qcf.quarterly_contributions,
        qcf.quarterly_distributions,
        qcf.quarterly_dividends,
        qcf.quarterly_fees,
        qcf.quarterly_net_cashflow,
        qcf.total_transactions
    from quarter_end_perf qep
    left join quarterly_cashflows qcf
        on qep.portfolio_id = qcf.portfolio_id
        and qep.valuation_quarter_start = qcf.quarter_start
),

-- ISSUE: Multiple self-joins for historical comparisons (should use LAG)
with_self_joins as (
    select
        c.*,
        -- ISSUE: Self-join for prior quarter
        c_q1.nav_usd as prior_1q_nav,
        c_q1.quarterly_return as prior_1q_return,
        c_q1.sharpe_ratio as prior_1q_sharpe,
        -- ISSUE: Self-join for 2 quarters ago
        c_q2.nav_usd as prior_2q_nav,
        c_q2.quarterly_return as prior_2q_return,
        -- ISSUE: Self-join for 3 quarters ago
        c_q3.nav_usd as prior_3q_nav,
        c_q3.quarterly_return as prior_3q_return,
        -- ISSUE: Self-join for 4 quarters ago (1 year)
        c_q4.nav_usd as prior_4q_nav,
        c_q4.quarterly_return as prior_4q_return,
        c_q4.sharpe_ratio as prior_4q_sharpe,
        -- ISSUE: Self-join for 8 quarters ago (2 years)
        c_q8.nav_usd as prior_8q_nav,
        c_q8.quarterly_return as prior_8q_return
    from combined c
    left join combined c_q1
        on c.portfolio_id = c_q1.portfolio_id
        and c_q1.quarter_end = dateadd(quarter, -1, c.quarter_end)
    left join combined c_q2
        on c.portfolio_id = c_q2.portfolio_id
        and c_q2.quarter_end = dateadd(quarter, -2, c.quarter_end)
    left join combined c_q3
        on c.portfolio_id = c_q3.portfolio_id
        and c_q3.quarter_end = dateadd(quarter, -3, c.quarter_end)
    left join combined c_q4
        on c.portfolio_id = c_q4.portfolio_id
        and c_q4.quarter_end = dateadd(quarter, -4, c.quarter_end)
    left join combined c_q8
        on c.portfolio_id = c_q8.portfolio_id
        and c_q8.quarter_end = dateadd(quarter, -8, c.quarter_end)
),

-- ISSUE: Multiple window functions with repeated partitions
with_window_calcs as (
    select
        wsj.*,
        -- ISSUE: Running totals (repeated partition)
        sum(quarterly_contributions) over (
            partition by wsj.portfolio_id
            order by wsj.quarter_end
            rows between unbounded preceding and current row
        ) as cumulative_contributions,
        sum(quarterly_distributions) over (
            partition by wsj.portfolio_id
            order by wsj.quarter_end
            rows between unbounded preceding and current row
        ) as cumulative_distributions,
        sum(quarterly_dividends) over (
            partition by wsj.portfolio_id
            order by wsj.quarter_end
            rows between unbounded preceding and current row
        ) as cumulative_dividends,
        sum(quarterly_fees) over (
            partition by wsj.portfolio_id
            order by wsj.quarter_end
            rows between unbounded preceding and current row
        ) as cumulative_fees,
        -- ISSUE: Moving averages (same partition repeated)
        avg(quarterly_return) over (
            partition by wsj.portfolio_id
            order by wsj.quarter_end
            rows between 3 preceding and current row
        ) as rolling_4q_avg_return,
        avg(quarterly_return) over (
            partition by wsj.portfolio_id
            order by wsj.quarter_end
            rows between 7 preceding and current row
        ) as rolling_8q_avg_return,
        -- ISSUE: More window calculations
        stddev(quarterly_return) over (
            partition by wsj.portfolio_id
            order by wsj.quarter_end
            rows between 3 preceding and current row
        ) as rolling_4q_volatility,
        min(quarterly_return) over (
            partition by wsj.portfolio_id
            order by wsj.quarter_end
            rows between 3 preceding and current row
        ) as rolling_4q_min_return,
        max(quarterly_return) over (
            partition by wsj.portfolio_id
            order by wsj.quarter_end
            rows between 3 preceding and current row
        ) as rolling_4q_max_return,
        -- ISSUE: Ranking (same partition again)
        row_number() over (
            partition by wsj.portfolio_id
            order by wsj.quarterly_return desc
        ) as best_quarter_rank,
        row_number() over (
            partition by wsj.portfolio_id
            order by wsj.quarterly_return asc
        ) as worst_quarter_rank
    from with_self_joins wsj
),

-- ISSUE: Correlated subqueries for fund-level aggregations (very slow)
with_fund_context as (
    select
        wwc.*,
        -- ISSUE: Correlated subquery for fund total NAV
        (
            select sum(qep2.nav_usd)
            from quarter_end_perf qep2
            where qep2.fund_id = wwc.fund_id
            and qep2.quarter_end = wwc.quarter_end
        ) as fund_total_nav,
        -- ISSUE: Another correlated subquery for fund average return
        (
            select avg(qep2.portfolio_return_3m)
            from quarter_end_perf qep2
            where qep2.fund_id = wwc.fund_id
            and qep2.quarter_end = wwc.quarter_end
        ) as fund_avg_quarterly_return,
        -- ISSUE: Count of portfolios in fund (correlated)
        (
            select count(distinct qep2.portfolio_id)
            from quarter_end_perf qep2
            where qep2.fund_id = wwc.fund_id
            and qep2.quarter_end = wwc.quarter_end
        ) as fund_portfolio_count
    from with_window_calcs wwc
),

-- ISSUE: Complex derived metrics with repeated CASE statements
with_derived_metrics as (
    select
        wfc.*,
        -- ISSUE: Portfolio weight in fund
        case
            when wfc.fund_total_nav > 0
            then (wfc.nav_usd / wfc.fund_total_nav) * 100
            else null
        end as portfolio_weight_in_fund,
        -- ISSUE: Complex QoQ calculations (repeated division logic)
        case
            when wfc.prior_1q_nav is not null and wfc.prior_1q_nav > 0
            then ((wfc.nav_usd - wfc.prior_1q_nav) / wfc.prior_1q_nav) * 100
            else null
        end as qoq_nav_growth_pct,
        case
            when wfc.prior_1q_return is not null
            then (wfc.quarterly_return - wfc.prior_1q_return)
            else null
        end as qoq_return_change,
        -- ISSUE: Complex YoY calculations
        case
            when wfc.prior_4q_nav is not null and wfc.prior_4q_nav > 0
            then ((wfc.nav_usd - wfc.prior_4q_nav) / wfc.prior_4q_nav) * 100
            else null
        end as yoy_nav_growth_pct,
        case
            when wfc.prior_4q_return is not null
            then (wfc.quarterly_return - wfc.prior_4q_return)
            else null
        end as yoy_return_change,
        -- ISSUE: 2-year growth
        case
            when wfc.prior_8q_nav is not null and wfc.prior_8q_nav > 0
            then ((wfc.nav_usd - wfc.prior_8q_nav) / wfc.prior_8q_nav) * 100
            else null
        end as two_year_nav_growth_pct,
        -- ISSUE: TVPI and DPI calculations (repeated division)
        case
            when wfc.cumulative_contributions > 0
            then (wfc.nav_usd + wfc.cumulative_distributions) / wfc.cumulative_contributions
            else null
        end as tvpi,
        case
            when wfc.cumulative_contributions > 0
            then wfc.cumulative_distributions / wfc.cumulative_contributions
            else null
        end as dpi,
        case
            when wfc.cumulative_contributions > 0
            then wfc.nav_usd / wfc.cumulative_contributions
            else null
        end as rvpi,
        -- ISSUE: Performance trend classification (complex nested CASE)
        case
            when wfc.rolling_4q_avg_return > wfc.rolling_8q_avg_return * 1.2 then 'ACCELERATING'
            when wfc.rolling_4q_avg_return > wfc.rolling_8q_avg_return * 1.05 then 'IMPROVING'
            when wfc.rolling_4q_avg_return < wfc.rolling_8q_avg_return * 0.8 then 'DECELERATING'
            when wfc.rolling_4q_avg_return < wfc.rolling_8q_avg_return * 0.95 then 'DECLINING'
            else 'STABLE'
        end as performance_trend,
        -- ISSUE: Consistency rating (complex nested CASE)
        case
            when wfc.rolling_4q_volatility < 0.02 then 'VERY_CONSISTENT'
            when wfc.rolling_4q_volatility < 0.05 then 'CONSISTENT'
            when wfc.rolling_4q_volatility < 0.10 then 'MODERATE'
            when wfc.rolling_4q_volatility < 0.15 then 'VARIABLE'
            else 'HIGHLY_VARIABLE'
        end as consistency_rating,
        -- ISSUE: Relative to fund performance (nested CASE)
        case
            when wfc.quarterly_return > wfc.fund_avg_quarterly_return + 0.05 then 'SIGNIFICANT_OUTPERFORM'
            when wfc.quarterly_return > wfc.fund_avg_quarterly_return + 0.02 then 'OUTPERFORM'
            when wfc.quarterly_return < wfc.fund_avg_quarterly_return - 0.05 then 'SIGNIFICANT_UNDERPERFORM'
            when wfc.quarterly_return < wfc.fund_avg_quarterly_return - 0.02 then 'UNDERPERFORM'
            else 'IN_LINE'
        end as relative_to_fund
    from with_fund_context wfc
),

-- ISSUE: More complex calculations and string operations
final as (
    select
        wdm.portfolio_id,
        wdm.portfolio_name,
        wdm.portfolio_type,
        wdm.fund_id,
        wdm.quarter_end,
        wdm.quarter_start,
        wdm.valuation_year,
        wdm.valuation_quarter,
        -- ISSUE: String concatenations (slow)
        concat('Q', wdm.valuation_quarter, ' ', wdm.valuation_year) as quarter_label,
        concat(wdm.valuation_year, '-Q', wdm.valuation_quarter) as quarter_code,
        concat(wdm.portfolio_name, ' (', wdm.portfolio_type, ')') as portfolio_display,
        -- Core metrics
        wdm.nav_usd,
        wdm.quarterly_return,
        wdm.benchmark_quarterly_return,
        wdm.quarterly_excess_return,
        wdm.portfolio_return_1y as trailing_1y_return,
        wdm.portfolio_cumulative_return as since_inception_return,
        wdm.sharpe_ratio,
        wdm.sortino_ratio,
        wdm.max_drawdown,
        wdm.performance_rating,
        wdm.risk_adjusted_rating,
        -- Cashflow metrics
        wdm.quarterly_contributions,
        wdm.quarterly_distributions,
        wdm.quarterly_dividends,
        wdm.quarterly_fees,
        wdm.quarterly_net_cashflow,
        wdm.cumulative_contributions,
        wdm.cumulative_distributions,
        wdm.total_transactions,
        -- Period comparisons
        wdm.qoq_nav_growth_pct,
        wdm.qoq_return_change,
        wdm.yoy_nav_growth_pct,
        wdm.yoy_return_change,
        wdm.two_year_nav_growth_pct,
        -- Performance ratios
        wdm.tvpi,
        wdm.dpi,
        wdm.rvpi,
        -- Rolling metrics
        wdm.rolling_4q_avg_return,
        wdm.rolling_8q_avg_return,
        wdm.rolling_4q_volatility,
        wdm.rolling_4q_min_return,
        wdm.rolling_4q_max_return,
        -- Fund context
        wdm.fund_total_nav,
        wdm.fund_avg_quarterly_return,
        wdm.fund_portfolio_count,
        wdm.portfolio_weight_in_fund,
        -- Classifications
        wdm.performance_trend,
        wdm.consistency_rating,
        wdm.relative_to_fund,
        wdm.best_quarter_rank,
        wdm.worst_quarter_rank,
        -- ISSUE: Additional derived fields (repeated calculations)
        case
            when wdm.cumulative_contributions > 0
            then wdm.cumulative_distributions / wdm.cumulative_contributions * 100
            else null
        end as distribution_yield_pct,
        case
            when wdm.cumulative_contributions > 0
            then wdm.cumulative_fees / wdm.cumulative_contributions * 100
            else null
        end as fee_burden_pct
    from with_derived_metrics wdm
)

select * from final
order by portfolio_id, quarter_end
