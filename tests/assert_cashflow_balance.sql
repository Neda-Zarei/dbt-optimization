-- Test: Cashflow totals should balance at portfolio level
-- Validates that contributions - distributions = net cashflow

with cashflow_totals as (
    select
        portfolio_id,
        sum(contributions) as total_contributions,
        sum(distributions) as total_distributions,
        sum(net_inflow) as total_net_inflow
    from {{ ref('report_monthly_cashflows') }}
    group by portfolio_id
)

select *
from cashflow_totals
where abs(total_contributions - total_distributions - total_net_inflow) > 0.01
