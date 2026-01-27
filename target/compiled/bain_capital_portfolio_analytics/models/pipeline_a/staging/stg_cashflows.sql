-- Pipeline A: Simple Cashflow Pipeline
-- Model: stg_cashflows
-- Description: Staging model for raw cashflow data
--
-- ISSUES FOR ARTEMIS TO OPTIMIZE:
-- 1. Unnecessary DISTINCT (source already unique)
-- 2. Late filtering (should push date filter upstream)
-- 3. Non-optimal date casting

with source as (
    select distinct  -- ISSUE: Unnecessary DISTINCT, source has unique constraint
        cashflow_id,
        portfolio_id,
        cashflow_type,
        cashflow_date,
        amount,
        currency,
        created_at,
        updated_at
    from DBT_DEMO.DEV.cashflows
),

-- ISSUE: Heavy transformation before filtering
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

-- ISSUE: Filter applied after transformation, should be earlier
filtered as (
    select *
    from converted
    where cashflow_date >= '2020-01-01'
      and cashflow_date <= '2024-12-31'
)

select * from filtered