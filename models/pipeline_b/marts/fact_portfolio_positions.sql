-- Pipeline B: Trade Analytics Pipeline
-- Model: fact_portfolio_positions
-- Description: Current position snapshot by portfolio and security
--
-- ISSUES FOR ARTEMIS TO OPTIMIZE:
-- 1. Gets latest position via subquery (should use QUALIFY)
-- 2. Redundant calculations
-- 3. Multiple passes for final aggregation

with trade_pnl as (
    select * from {{ ref('int_trade_pnl') }}
),

-- ISSUE: Using subquery + WHERE instead of QUALIFY for latest
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

-- ISSUE: Final join and calculations that could be simplified
final as (
    select
        {{ dbt_utils.generate_surrogate_key(['lp.portfolio_id', 'lp.security_id']) }} as position_key,
        lp.portfolio_id,
        lp.security_id,
        lp.ticker,
        lp.running_position as current_quantity,
        lp.avg_cost_basis,
        mp.current_price,
        mp.price_date as price_as_of_date,
        lp.running_position * lp.avg_cost_basis as cost_basis_value,
        lp.running_position * mp.current_price as market_value,
        (lp.running_position * mp.current_price) - (lp.running_position * lp.avg_cost_basis) as unrealized_pnl,
        case
            when lp.avg_cost_basis > 0
            then ((mp.current_price - lp.avg_cost_basis) / lp.avg_cost_basis) * 100
            else null
        end as unrealized_pnl_pct,
        lp.cumulative_purchase_cost as total_invested,
        current_timestamp() as snapshot_timestamp
    from latest_positions lp
    left join market_prices mp
        on lp.security_id = mp.security_id
    where lp.running_position != 0
)

select * from final
