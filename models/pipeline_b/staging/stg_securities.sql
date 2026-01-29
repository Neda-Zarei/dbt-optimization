-- Pipeline B: Trade Analytics Pipeline
-- Model: stg_securities
-- Description: Staging model for security master data
--

with source as (
    select
        security_id,
        ticker,
        security_name,
        security_type,
        asset_class,
        sector,
        industry,
        currency,
        exchange,
        is_active,
        created_at,
        updated_at
    from {{ source('raw', 'securities') }}
),

deduplicated as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by security_id
                order by updated_at desc
            ) as rn
        from source
    ) sub
    where rn = 1
),

standardized as (
    select
        security_id,
        upper(trim(ticker)) as ticker,
        trim(security_name) as security_name,
        case
            when security_type in ('STOCK', 'EQUITY', 'COMMON') then 'EQUITY'
            when security_type in ('BOND', 'NOTE', 'DEBENTURE') then 'FIXED_INCOME'
            when security_type in ('OPTION', 'FUTURE', 'SWAP') then 'DERIVATIVE'
            when security_type in ('ETF', 'MUTUAL_FUND') then 'FUND'
            else 'OTHER'
        end as security_type_standardized,
        security_type as security_type_original,
        upper(asset_class) as asset_class,
        sector,
        industry,
        upper(currency) as currency,
        exchange,
        is_active,
        created_at,
        updated_at
    from deduplicated
)

select * from standardized
where is_active = true
