-- Pipeline B: Trade Analytics Pipeline
-- Model: int_trade_pnl
-- Description: Calculate P&L for each trade
--

with trades as (
    select * from {{ ref('int_trades_enriched') }}
),

positions as (
    select
        trade_id,
        portfolio_id,
        security_id,
        ticker,
        security_name,
        security_type,
        asset_class,
        sector,
        industry,
        trade_date,
        trade_type,
        trade_category,
        quantity,
        execution_price,
        net_amount,
        commission,
        sum(case
            when trade_category = 'PURCHASE' then quantity
            when trade_category = 'SALE' then -quantity
            else 0
        end) over (
            partition by portfolio_id, security_id
            order by trade_date, trade_id
            rows between unbounded preceding and current row
        ) as running_position,
        sum(case
            when trade_category = 'PURCHASE' then net_amount
            when trade_category = 'SALE' then -net_amount
            else 0
        end) over (
            partition by portfolio_id, security_id
            order by trade_date, trade_id
            rows between unbounded preceding and current row
        ) as cumulative_cost,
        sum(case when trade_category = 'PURCHASE' then quantity else 0 end) over (
            partition by portfolio_id, security_id
            order by trade_date, trade_id
            rows between unbounded preceding and current row
        ) as cumulative_purchased_qty,
        sum(case when trade_category = 'PURCHASE' then net_amount else 0 end) over (
            partition by portfolio_id, security_id
            order by trade_date, trade_id
            rows between unbounded preceding and current row
        ) as cumulative_purchase_cost
    from trades
),

with_cost_basis as (
    select
        *,
        case
            when cumulative_purchased_qty > 0
            then cumulative_purchase_cost / cumulative_purchased_qty
            else null
        end as avg_cost_basis
    from positions
),

with_pnl as (
    select
        *,
        case
            when trade_category = 'SALE' and avg_cost_basis is not null
            then (execution_price - avg_cost_basis) * quantity
            else null
        end as realized_pnl,
        case
            when trade_category = 'SALE' and avg_cost_basis is not null and avg_cost_basis > 0
            then (execution_price - avg_cost_basis) / avg_cost_basis * 100
            else null
        end as realized_pnl_pct
    from with_cost_basis
)

select * from with_pnl
