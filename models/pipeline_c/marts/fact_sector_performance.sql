-- Pipeline C: Complex Portfolio Analytics
-- Model: fact_sector_performance
-- Description: Sector-level performance aggregation
--

with sector_attribution as (
    select * from {{ ref('int_sector_attribution') }}
),

portfolios as (
    select * from {{ ref('stg_portfolios') }}
),

with_portfolio_info as (
    select
        sa.*,
        p.portfolio_name,
        p.portfolio_type,
        p.fund_id
    from sector_attribution sa
    inner join portfolios p
        on sa.portfolio_id = p.portfolio_id
),

with_rankings as (
    select
        *,
        rank() over (
            partition by portfolio_id, position_date
            order by sector_weight desc
        ) as sector_weight_rank,
        rank() over (
            partition by portfolio_id, position_date
            order by sector_contribution desc
        ) as sector_contribution_rank
    from with_portfolio_info
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['portfolio_id', 'sector', 'position_date']) }} as sector_performance_key,
        *,
        case
            when sector_weight_rank <= 3 then 'TOP_3'
            when sector_weight_rank <= 5 then 'TOP_5'
            else 'OTHER'
        end as sector_weight_tier
    from with_rankings
)

select * from final
