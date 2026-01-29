-- Pipeline C: Complex Portfolio Analytics
-- Model: report_ic_dashboard
-- Description: Investment Committee dashboard report
--
-- ISSUES FOR ARTEMIS TO OPTIMIZE:
-- 1. Combines multiple fact tables
-- 2. Re-aggregates aggregated data
-- 3. Complex pivoting logic
-- 4. Multiple CTEs that could be simplified

with portfolio_performance as (
    select * from {{ ref('fact_portfolio_performance') }}
),

fund_summary as (
    select * from {{ ref('fact_fund_summary') }}
),

position_snapshot as (
    select * from {{ ref('fact_position_snapshot') }}
),

-- ISSUE: Get latest performance per portfolio
latest_portfolio_perf as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by portfolio_id
                order by valuation_date desc
            ) as rn
        from portfolio_performance
    )
    where rn = 1  -- ISSUE: Should use QUALIFY
),

-- ISSUE: Get latest positions per portfolio
latest_positions as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by portfolio_id, security_id
                order by position_date desc
            ) as rn
        from position_snapshot
    )
    where rn = 1
),

-- ISSUE: Re-aggregate positions for portfolio summary
position_summary as (
    select
        portfolio_id,
        count(distinct security_id) as total_positions,
        count(distinct sector) as sector_count,
        sum(market_value_usd) as total_market_value,
        max(weight_pct) as max_position_weight,
        -- Concentration metrics
        sum(case when weight_pct >= 0.05 then 1 else 0 end) as positions_over_5pct
    from latest_positions
    group by 1
),

-- ISSUE: Sector concentration
sector_concentration as (
    select
        portfolio_id,
        listagg(sector, ', ') within group (order by sector_weight desc) as top_sectors
    from (
        select
            portfolio_id,
            sector,
            sum(weight_pct) as sector_weight,
            row_number() over (partition by portfolio_id order by sum(weight_pct) desc) as sector_rank
        from latest_positions
        group by 1, 2
    )
    where sector_rank <= 3
    group by 1
),

-- ISSUE: Combine all metrics
dashboard_data as (
    select
        lpp.portfolio_id,
        lpp.portfolio_name,
        lpp.portfolio_type,
        lpp.fund_id,
        fs.fund_name,
        lpp.valuation_date as as_of_date,
        lpp.nav_usd,
        -- Performance
        lpp.portfolio_return_1m,
        lpp.portfolio_return_3m,
        lpp.portfolio_return_1y,
        lpp.portfolio_cumulative_return as inception_return,
        -- Benchmark comparison
        lpp.benchmark_id,
        lpp.excess_return_1m,
        lpp.excess_return_1y,
        lpp.information_ratio,
        -- Risk
        lpp.portfolio_volatility,
        lpp.sharpe_ratio,
        lpp.sortino_ratio,
        lpp.max_drawdown,
        lpp.var_95_1d,
        -- Positions
        ps.total_positions,
        ps.sector_count,
        ps.max_position_weight,
        ps.positions_over_5pct,
        sc.top_sectors,
        -- Fund level
        fs.total_nav_usd as fund_total_nav,
        fs.portfolio_count as fund_portfolio_count,
        fs.weighted_sharpe_ratio as fund_sharpe,
        -- Portfolio share of fund
        case
            when fs.total_nav_usd > 0
            then lpp.nav_usd / fs.total_nav_usd * 100
            else null
        end as pct_of_fund
    from latest_portfolio_perf lpp
    left join fund_summary fs
        on lpp.fund_id = fs.fund_id
        and lpp.valuation_date = fs.valuation_date
    left join position_summary ps
        on lpp.portfolio_id = ps.portfolio_id
    left join sector_concentration sc
        on lpp.portfolio_id = sc.portfolio_id
),

-- ISSUE: Final scoring/ranking
final as (
    select
        *,
        -- Performance score (simplified)
        (coalesce(portfolio_return_1y, 0) * 0.4 +
         coalesce(sharpe_ratio, 0) * 0.3 +
         coalesce(information_ratio, 0) * 0.3) as composite_score,
        rank() over (order by portfolio_return_1y desc nulls last) as return_rank,
        rank() over (order by sharpe_ratio desc nulls last) as sharpe_rank
    from dashboard_data
)

select * from final
order by composite_score desc
