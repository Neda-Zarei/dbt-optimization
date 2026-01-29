-- Pipeline B: Trade Analytics Pipeline
-- Model: stg_trades
-- Description: Staging model for trade transactions
--

with source as (
    select
        trade_id,
        portfolio_id,
        security_id,
        broker_id,
        trade_date,
        settlement_date,
        trade_type,
        quantity,
        price,
        gross_amount,
        commission,
        fees,
        net_amount,
        currency,
        created_at,
        updated_at
    from {{ source('raw', 'trades') }}
),

categorized as (
    select
        *,
        case
            when trade_type in ('BUY', 'COVER') then 'PURCHASE'
            when trade_type in ('SELL', 'SHORT') then 'SALE'
            when trade_type in ('DIVIDEND', 'INTEREST') then 'INCOME'
            else 'OTHER'
        end as trade_category,
        case
            when abs(net_amount) >= 10000000 then 'LARGE'
            when abs(net_amount) >= 1000000 then 'MEDIUM'
            when abs(net_amount) >= 100000 then 'SMALL'
            else 'MICRO'
        end as trade_size_bucket,
        upper(trim(trade_type)) as trade_type_clean,
        upper(trim(currency)) as currency_clean
    from source
),

with_dates as (
    select
        *,
        datediff('day', trade_date, settlement_date) as settlement_days,
        date_trunc('month', trade_date) as trade_month,
        date_trunc('quarter', trade_date) as trade_quarter,
        extract(year from trade_date) as trade_year,
        extract(month from trade_date) as trade_month_num,
        dayofweek(trade_date) as trade_day_of_week
    from categorized
),

filtered as (
    select *
    from with_dates
    where trade_date >= '{{ var("start_date") }}'
)

select * from filtered
