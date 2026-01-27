-- Pipeline A: Simple Cashflow Pipeline
-- Model: report_monthly_cashflows
-- Description: LP reporting view for monthly cashflow analysis
--
-- ISSUES FOR ARTEMIS TO OPTIMIZE:
-- 1. Re-aggregates data that's already in fact table
-- 2. Repeated window functions
-- 3. Suboptimal pivot pattern

with fact_data as (
    select * from {{ ref('fact_cashflow_summary') }}
),

-- ISSUE: Re-aggregating already aggregated data
monthly_totals as (
    select
        portfolio_id,
        portfolio_name,
        portfolio_type,
        fund_id,
        cashflow_month,
        year_num,
        month_num,
        sum(case when cashflow_type = 'CONTRIBUTION' then total_amount else 0 end) as contributions,
        sum(case when cashflow_type = 'DISTRIBUTION' then total_amount else 0 end) as distributions,
        sum(case when cashflow_type = 'DIVIDEND' then total_amount else 0 end) as dividends,
        sum(case when cashflow_type = 'FEE' then total_amount else 0 end) as fees,
        sum(total_amount) as total_cashflow,
        sum(transaction_count) as total_transactions
    from fact_data
    group by 1,2,3,4,5,6,7
),

-- ISSUE: Window functions recalculated multiple times
with_running_totals as (
    select
        *,
        -- Running totals (repeated pattern)
        sum(contributions) over (
            partition by portfolio_id
            order by cashflow_month
            rows between unbounded preceding and current row
        ) as cumulative_contributions,
        sum(distributions) over (
            partition by portfolio_id
            order by cashflow_month
            rows between unbounded preceding and current row
        ) as cumulative_distributions,
        sum(total_cashflow) over (
            partition by portfolio_id
            order by cashflow_month
            rows between unbounded preceding and current row
        ) as cumulative_net_cashflow,
        -- Prior period comparisons (another repeated pattern)
        lag(contributions, 1) over (partition by portfolio_id order by cashflow_month) as prior_month_contributions,
        lag(distributions, 1) over (partition by portfolio_id order by cashflow_month) as prior_month_distributions,
        lag(total_cashflow, 1) over (partition by portfolio_id order by cashflow_month) as prior_month_total,
        -- YoY comparison
        lag(contributions, 12) over (partition by portfolio_id order by cashflow_month) as prior_year_contributions,
        lag(distributions, 12) over (partition by portfolio_id order by cashflow_month) as prior_year_distributions
    from monthly_totals
),

-- ISSUE: Calculated columns that could be simplified
final as (
    select
        *,
        contributions - coalesce(prior_month_contributions, 0) as mom_contribution_change,
        distributions - coalesce(prior_month_distributions, 0) as mom_distribution_change,
        case
            when prior_year_contributions > 0
            then (contributions - prior_year_contributions) / prior_year_contributions * 100
            else null
        end as yoy_contribution_pct_change,
        contributions - distributions as net_inflow
    from with_running_totals
)

select * from final
order by portfolio_id, cashflow_month
