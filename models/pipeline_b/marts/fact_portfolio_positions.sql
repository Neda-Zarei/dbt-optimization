-- Pipeline B: Trade Analytics Pipeline
-- Model: fact_portfolio_positions
-- Description: Current position snapshot by portfolio and security
-- DEPENDENCY: Uses fact_cashflow_summary from Pipeline A for portfolio cash context
--
-- ISSUES FOR ARTEMIS TO OPTIMIZE:
-- 1. Gets latest position via subquery (should use QUALIFY)
-- 2. Self-joins for historical position lookups
-- 3. Correlated subqueries for portfolio-level aggregations
-- 4. Repeated window functions

with trade_pnl as (
    select * from {{ ref('int_trade_pnl') }}
),

-- DEPENDENCY ON PIPELINE A: Get cashflow context for each portfolio
cashflow_summary as (
    select * from {{ ref('fact_cashflow_summary') }}
),

-- Aggregate cashflows by portfolio to get total contributions/distributions
portfolio_cashflows as (
    select
        portfolio_id,
        sum(case when cashflow_type = 'CONTRIBUTION' then cumulative_total else 0 end) as total_contributions,
        sum(case when cashflow_type = 'DISTRIBUTION' then abs(cumulative_total) else 0 end) as total_distributions,
        max(cashflow_month) as last_cashflow_date
    from cashflow_summary
    group by portfolio_id
),

latest_positions as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by portfolio_id, security_id
                order by trade_date desc, trade_id desc
            ) as rn
        from trade_pnl
    )
    where rn = 1
),

-- ISSUE: Self-join to get position 30 days ago
positions_30d_ago as (
    select
        portfolio_id,
        security_id,
        running_position as position_30d_ago,
        avg_cost_basis as cost_basis_30d_ago
    from (
        select
            *,
            row_number() over (
                partition by portfolio_id, security_id
                order by trade_date desc, trade_id desc
            ) as rn
        from trade_pnl
        where trade_date <= dateadd(day, -30, current_date())
    )
    where rn = 1
),

-- ISSUE: Self-join to get position 90 days ago
positions_90d_ago as (
    select
        portfolio_id,
        security_id,
        running_position as position_90d_ago,
        avg_cost_basis as cost_basis_90d_ago
    from (
        select
            *,
            row_number() over (
                partition by portfolio_id, security_id
                order by trade_date desc, trade_id desc
            ) as rn
        from trade_pnl
        where trade_date <= dateadd(day, -90, current_date())
    )
    where rn = 1
),

market_prices as (
    select
        security_id,
        close_price as current_price,
        price_date
    from (
        select
            security_id,
            close_price,
            price_date,
            row_number() over (partition by security_id order by price_date desc) as rn
        from {{ ref('stg_market_prices') }}
    )
    where rn = 1  -- ISSUE: Again, should use QUALIFY
),

-- ISSUE: Get historical prices for comparison
market_prices_30d_ago as (
    select
        security_id,
        close_price as price_30d_ago
    from (
        select
            security_id,
            close_price,
            row_number() over (partition by security_id order by price_date desc) as rn
        from {{ ref('stg_market_prices') }}
        where price_date <= dateadd(day, -30, current_date())
    )
    where rn = 1
),

-- ISSUE: Join all the position snapshots together
enriched_positions as (
    select
        lp.*,
        mp.current_price,
        mp.price_date as price_as_of_date,
        p30.position_30d_ago,
        p30.cost_basis_30d_ago,
        p90.position_90d_ago,
        p90.cost_basis_90d_ago,
        mp30.price_30d_ago,
        pcf.total_contributions,
        pcf.total_distributions,
        pcf.last_cashflow_date
    from latest_positions lp
    left join market_prices mp
        on lp.security_id = mp.security_id
    left join positions_30d_ago p30
        on lp.portfolio_id = p30.portfolio_id
        and lp.security_id = p30.security_id
    left join positions_90d_ago p90
        on lp.portfolio_id = p90.portfolio_id
        and lp.security_id = p90.security_id
    left join market_prices_30d_ago mp30
        on lp.security_id = mp30.security_id
    left join portfolio_cashflows pcf
        on lp.portfolio_id = pcf.portfolio_id
    where lp.running_position != 0
),

-- ISSUE: Window functions for portfolio-level context
with_portfolio_context as (
    select
        ep.*,
        -- ISSUE: Repeated partition by portfolio_id
        sum(ep.running_position * ep.current_price) over (
            partition by ep.portfolio_id
        ) as portfolio_total_market_value,
        sum(ep.running_position * ep.avg_cost_basis) over (
            partition by ep.portfolio_id
        ) as portfolio_total_cost_basis,
        count(*) over (
            partition by ep.portfolio_id
        ) as portfolio_position_count,
        -- ISSUE: Rankings
        row_number() over (
            partition by ep.portfolio_id
            order by (ep.running_position * ep.current_price) desc
        ) as position_size_rank,
        row_number() over (
            partition by ep.portfolio_id
            order by ((ep.current_price - ep.avg_cost_basis) / nullif(ep.avg_cost_basis, 0)) desc
        ) as position_return_rank
    from enriched_positions ep
),

-- ISSUE: Separate aggregation for sector context (should use window functions)
sector_aggs as (
    select
        portfolio_id,
        sector,
        sum(running_position * current_price) as sector_market_value,
        count(*) as sector_position_count
    from enriched_positions
    group by 1, 2
),

with_sector_context as (
    select
        wpc.*,
        sa.sector_market_value,
        sa.sector_position_count
    from with_portfolio_context wpc
    left join sector_aggs sa
        on wpc.portfolio_id = sa.portfolio_id
        and wpc.sector = sa.sector
),

-- ISSUE: Complex calculations in final select
final as (
    select
        {{ dbt_utils.generate_surrogate_key(['wsc.portfolio_id', 'wsc.security_id']) }} as position_key,
        wsc.portfolio_id,
        wsc.security_id,
        wsc.ticker,
        wsc.security_name,
        wsc.sector,
        wsc.asset_class,
        wsc.running_position as current_quantity,
        wsc.avg_cost_basis,
        wsc.current_price,
        wsc.price_as_of_date,
        -- Core calculations
        wsc.running_position * wsc.avg_cost_basis as cost_basis_value,
        wsc.running_position * wsc.current_price as market_value,
        (wsc.running_position * wsc.current_price) - (wsc.running_position * wsc.avg_cost_basis) as unrealized_pnl,
        -- ISSUE: Repeated division logic
        case
            when wsc.avg_cost_basis > 0
            then ((wsc.current_price - wsc.avg_cost_basis) / wsc.avg_cost_basis) * 100
            else null
        end as unrealized_pnl_pct,
        -- Portfolio context
        wsc.portfolio_total_market_value,
        wsc.portfolio_total_cost_basis,
        wsc.portfolio_position_count,
        -- ISSUE: Weight calculation (repeated division)
        case
            when wsc.portfolio_total_market_value > 0
            then ((wsc.running_position * wsc.current_price) / wsc.portfolio_total_market_value) * 100
            else null
        end as portfolio_weight_pct,
        -- Sector context
        wsc.sector_market_value,
        wsc.sector_position_count,
        case
            when wsc.sector_market_value > 0
            then ((wsc.running_position * wsc.current_price) / wsc.sector_market_value) * 100
            else null
        end as sector_weight_pct,
        -- Historical comparison
        wsc.position_30d_ago,
        wsc.position_90d_ago,
        wsc.position_30d_ago - wsc.running_position as position_change_30d,
        wsc.position_90d_ago - wsc.running_position as position_change_90d,
        -- Price momentum
        wsc.price_30d_ago,
        case
            when wsc.price_30d_ago > 0
            then ((wsc.current_price - wsc.price_30d_ago) / wsc.price_30d_ago) * 100
            else null
        end as price_change_30d_pct,
        -- Cashflow context from Pipeline A
        wsc.total_contributions,
        wsc.total_distributions,
        wsc.last_cashflow_date,
        -- Rankings
        wsc.position_size_rank,
        wsc.position_return_rank,
        -- ISSUE: Complex classification
        case
            when ((wsc.running_position * wsc.current_price) / nullif(wsc.portfolio_total_market_value, 0)) > 0.10 then 'CONCENTRATED'
            when ((wsc.running_position * wsc.current_price) / nullif(wsc.portfolio_total_market_value, 0)) > 0.05 then 'SIGNIFICANT'
            when ((wsc.running_position * wsc.current_price) / nullif(wsc.portfolio_total_market_value, 0)) > 0.02 then 'MODERATE'
            else 'SMALL'
        end as position_size_category,
        wsc.cumulative_purchase_cost as total_invested,
        current_timestamp() as snapshot_timestamp
    from with_sector_context wsc
)

select * from final
