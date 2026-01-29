-- Pipeline A: Simple Cashflow Pipeline
-- Model: stg_cashflows
-- Description: Staging model for raw cashflow data
--

with source as (
    select distinct
        cashflow_id,
        portfolio_id,
        cashflow_type,
        cashflow_date,
        amount,
        currency,
        created_at,
        updated_at
    from {{ source('raw', 'cashflows') }}
),

converted as (
    select
        cashflow_id,
        portfolio_id,
        upper(cashflow_type) as cashflow_type,
        cast(cashflow_date as date) as cashflow_date,
        cast(amount as decimal(18,2)) as amount,
        upper(currency) as currency,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at
    from source
),

filtered as (
    select *
    from converted
    where cashflow_date >= '{{ var("start_date") }}'
      and cashflow_date <= '{{ var("end_date") }}'
)

select * from filtered
