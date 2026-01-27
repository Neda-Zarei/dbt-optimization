-- Pipeline A: Simple Cashflow Pipeline
-- Model: fact_cashflow_summary
-- Description: Fact table summarizing cashflows by portfolio and month
--
-- ISSUES FOR ARTEMIS TO OPTIMIZE:
-- 1. Self-joins for prior period comparisons (should use LAG)
-- 2. Late aggregation (aggregates after full join)
-- 3. Repeated window functions with same partitions
-- 4. Redundant date calculations per row
-- 5. Correlated subqueries for fund-level totals

with cashflows as (
    select * from {{ ref('stg_cashflows') }}
),

portfolios as (
    select * from {{ ref('stg_portfolios') }}
),

-- ISSUE: Full join before aggregation (scans all rows)
joined as (
    select
        c.cashflow_id,
        c.portfolio_id,
        p.portfolio_name,
        p.portfolio_type,
        p.fund_id,
        c.cashflow_type,
        c.cashflow_date,
        c.amount,
        c.currency,
        -- ISSUE: Redundant date calculations done per row
        date_trunc('month', c.cashflow_date) as cashflow_month,
        date_trunc('quarter', c.cashflow_date) as cashflow_quarter,
        date_trunc('year', c.cashflow_date) as cashflow_year,
        extract(year from c.cashflow_date) as year_num,
        extract(month from c.cashflow_date) as month_num,
        extract(quarter from c.cashflow_date) as quarter_num,
        extract(dayofmonth from c.cashflow_date) as day_num
    from cashflows c
    inner join portfolios p
        on c.portfolio_id = p.portfolio_id
),

-- ISSUE: Aggregation happens after full row-level join
aggregated as (
    select
        portfolio_id,
        portfolio_name,
        portfolio_type,
        fund_id,
        cashflow_month,
        cashflow_quarter,
        cashflow_year,
        year_num,
        month_num,
        quarter_num,
        cashflow_type,
        currency,
        count(*) as transaction_count,
        count(distinct cashflow_id) as unique_transactions,
        sum(amount) as total_amount,
        avg(amount) as avg_amount,
        min(amount) as min_amount,
        max(amount) as max_amount,
        stddev(amount) as stddev_amount,
        -- ISSUE: Percentile calculations (slow)
        percentile_cont(0.25) within group (order by amount) as p25_amount,
        percentile_cont(0.50) within group (order by amount) as median_amount,
        percentile_cont(0.75) within group (order by amount) as p75_amount
    from joined
    group by 1,2,3,4,5,6,7,8,9,10,11,12  -- ISSUE: Non-descriptive GROUP BY
),

-- ISSUE: Self-join for prior month comparisons (should use LAG)
with_prior_months as (
    select
        agg.*,
        -- ISSUE: Self-join for prior month
        agg_m1.total_amount as prior_1m_total,
        agg_m1.transaction_count as prior_1m_count,
        -- ISSUE: Self-join for 3 months ago
        agg_m3.total_amount as prior_3m_total,
        -- ISSUE: Self-join for 6 months ago
        agg_m6.total_amount as prior_6m_total,
        -- ISSUE: Self-join for 12 months ago
        agg_m12.total_amount as prior_12m_total
    from aggregated agg
    left join aggregated agg_m1
        on agg.portfolio_id = agg_m1.portfolio_id
        and agg.cashflow_type = agg_m1.cashflow_type
        and agg.currency = agg_m1.currency
        and agg_m1.cashflow_month = dateadd(month, -1, agg.cashflow_month)
    left join aggregated agg_m3
        on agg.portfolio_id = agg_m3.portfolio_id
        and agg.cashflow_type = agg_m3.cashflow_type
        and agg.currency = agg_m3.currency
        and agg_m3.cashflow_month = dateadd(month, -3, agg.cashflow_month)
    left join aggregated agg_m6
        on agg.portfolio_id = agg_m6.portfolio_id
        and agg.cashflow_type = agg_m6.cashflow_type
        and agg.currency = agg_m6.currency
        and agg_m6.cashflow_month = dateadd(month, -6, agg.cashflow_month)
    left join aggregated agg_m12
        on agg.portfolio_id = agg_m12.portfolio_id
        and agg.cashflow_type = agg_m12.cashflow_type
        and agg.currency = agg_m12.currency
        and agg_m12.cashflow_month = dateadd(month, -12, agg.cashflow_month)
),

-- ISSUE: Multiple window functions with repeated partitions
with_window_calcs as (
    select
        wpm.*,
        -- ISSUE: Running totals (repeated partition)
        sum(total_amount) over (
            partition by wpm.portfolio_id, wpm.cashflow_type, wpm.currency
            order by wpm.cashflow_month
            rows between unbounded preceding and current row
        ) as cumulative_total,
        sum(transaction_count) over (
            partition by wpm.portfolio_id, wpm.cashflow_type, wpm.currency
            order by wpm.cashflow_month
            rows between unbounded preceding and current row
        ) as cumulative_count,
        -- ISSUE: Moving averages (same partition repeated)
        avg(total_amount) over (
            partition by wpm.portfolio_id, wpm.cashflow_type, wpm.currency
            order by wpm.cashflow_month
            rows between 2 preceding and current row
        ) as rolling_3m_avg,
        avg(total_amount) over (
            partition by wpm.portfolio_id, wpm.cashflow_type, wpm.currency
            order by wpm.cashflow_month
            rows between 5 preceding and current row
        ) as rolling_6m_avg,
        avg(total_amount) over (
            partition by wpm.portfolio_id, wpm.cashflow_type, wpm.currency
            order by wpm.cashflow_month
            rows between 11 preceding and current row
        ) as rolling_12m_avg,
        -- ISSUE: More window calculations
        stddev(total_amount) over (
            partition by wpm.portfolio_id, wpm.cashflow_type, wpm.currency
            order by wpm.cashflow_month
            rows between 11 preceding and current row
        ) as rolling_12m_stddev,
        min(total_amount) over (
            partition by wpm.portfolio_id, wpm.cashflow_type, wpm.currency
            order by wpm.cashflow_month
            rows between 11 preceding and current row
        ) as rolling_12m_min,
        max(total_amount) over (
            partition by wpm.portfolio_id, wpm.cashflow_type, wpm.currency
            order by wpm.cashflow_month
            rows between 11 preceding and current row
        ) as rolling_12m_max
    from with_prior_months wpm
),

-- ISSUE: Correlated subqueries for fund-level context (very slow)
with_fund_context as (
    select
        wwc.*,
        -- ISSUE: Correlated subquery for fund total
        (
            select sum(total_amount)
            from aggregated agg2
            inner join portfolios p2
                on agg2.portfolio_id = p2.portfolio_id
            where p2.fund_id = wwc.fund_id
            and agg2.cashflow_month = wwc.cashflow_month
            and agg2.cashflow_type = wwc.cashflow_type
        ) as fund_total_amount,
        -- ISSUE: Another correlated subquery for portfolio count
        (
            select count(distinct agg2.portfolio_id)
            from aggregated agg2
            inner join portfolios p2
                on agg2.portfolio_id = p2.portfolio_id
            where p2.fund_id = wwc.fund_id
            and agg2.cashflow_month = wwc.cashflow_month
            and agg2.cashflow_type = wwc.cashflow_type
        ) as fund_portfolio_count
    from with_window_calcs wwc
),

-- ISSUE: Complex derived metrics with repeated CASE statements
final as (
    select
        {{ dbt_utils.generate_surrogate_key(['wfc.portfolio_id', 'wfc.cashflow_month', 'wfc.cashflow_type', 'wfc.currency']) }} as cashflow_summary_key,
        wfc.*,
        -- ISSUE: Portfolio share of fund (repeated division)
        case
            when wfc.fund_total_amount > 0
            then (wfc.total_amount / wfc.fund_total_amount) * 100
            else null
        end as portfolio_share_of_fund_pct,
        -- ISSUE: Month-over-month growth calculations
        case
            when wfc.prior_1m_total is not null and wfc.prior_1m_total != 0
            then ((wfc.total_amount - wfc.prior_1m_total) / abs(wfc.prior_1m_total)) * 100
            else null
        end as mom_growth_pct,
        case
            when wfc.prior_3m_total is not null and wfc.prior_3m_total != 0
            then ((wfc.total_amount - wfc.prior_3m_total) / abs(wfc.prior_3m_total)) * 100
            else null
        end as growth_3m_pct,
        case
            when wfc.prior_12m_total is not null and wfc.prior_12m_total != 0
            then ((wfc.total_amount - wfc.prior_12m_total) / abs(wfc.prior_12m_total)) * 100
            else null
        end as yoy_growth_pct,
        -- ISSUE: Trend classification (complex nested CASE)
        case
            when wfc.rolling_3m_avg > wfc.rolling_12m_avg * 1.3 then 'ACCELERATING'
            when wfc.rolling_3m_avg > wfc.rolling_12m_avg * 1.1 then 'GROWING'
            when wfc.rolling_3m_avg < wfc.rolling_12m_avg * 0.7 then 'DECLINING_FAST'
            when wfc.rolling_3m_avg < wfc.rolling_12m_avg * 0.9 then 'DECLINING'
            else 'STABLE'
        end as trend_classification,
        -- ISSUE: Volatility classification
        case
            when wfc.rolling_12m_stddev < wfc.rolling_12m_avg * 0.1 then 'LOW_VOLATILITY'
            when wfc.rolling_12m_stddev < wfc.rolling_12m_avg * 0.3 then 'MODERATE_VOLATILITY'
            when wfc.rolling_12m_stddev < wfc.rolling_12m_avg * 0.5 then 'HIGH_VOLATILITY'
            else 'VERY_HIGH_VOLATILITY'
        end as volatility_classification,
        -- ISSUE: Size classification (repeated CASE)
        case
            when abs(wfc.total_amount) >= 10000000 then 'MEGA'
            when abs(wfc.total_amount) >= 5000000 then 'LARGE'
            when abs(wfc.total_amount) >= 1000000 then 'MEDIUM'
            when abs(wfc.total_amount) >= 100000 then 'SMALL'
            else 'MICRO'
        end as transaction_size_category
    from with_fund_context wfc
)

select * from final
