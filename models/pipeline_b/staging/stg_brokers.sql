-- Pipeline B: Trade Analytics Pipeline
-- Model: stg_brokers
-- Description: Staging model for broker information

with source as (
    select
        broker_id,
        broker_name,
        broker_type,
        region,
        is_active,
        commission_rate,
        created_at,
        updated_at
    from {{ source('raw', 'brokers') }}
),

deduplicated as (
    select *
    from (
        select
            *,
            row_number() over (partition by broker_id order by updated_at desc) as rn
        from source
    )
    where rn = 1
)

select
    broker_id,
    trim(broker_name) as broker_name,
    upper(broker_type) as broker_type,
    upper(region) as region,
    is_active,
    commission_rate,
    created_at,
    updated_at
from deduplicated
where is_active = true
