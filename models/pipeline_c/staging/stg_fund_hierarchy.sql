-- Pipeline C: Complex Portfolio Analytics
-- Model: stg_fund_hierarchy
-- Description: Fund and portfolio hierarchy for roll-up reporting

with source as (
    select
        entity_id,
        entity_name,
        entity_type,
        parent_entity_id,
        hierarchy_level,
        is_active,
        created_at,
        updated_at
    from {{ source('raw', 'fund_hierarchy') }}
)

select
    entity_id,
    trim(entity_name) as entity_name,
    upper(entity_type) as entity_type,
    parent_entity_id,
    hierarchy_level,
    is_active,
    created_at,
    updated_at
from source
where is_active = true
