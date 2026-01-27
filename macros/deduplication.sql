-- Deduplication macros
-- ISSUES FOR ARTEMIS TO RECOGNIZE:
-- These patterns are suboptimal for Snowflake (should use QUALIFY)

{% macro dedupe_by_key(source_table, key_columns, order_column, order_direction='desc') %}
{#
    ISSUE: This pattern uses a subquery instead of QUALIFY
    Artemis should recognize this and suggest Snowflake-optimized version:

    select *
    from {{ source_table }}
    qualify row_number() over (
        partition by {{ key_columns | join(', ') }}
        order by {{ order_column }} {{ order_direction }}
    ) = 1
#}
    select *
    from (
        select
            *,
            row_number() over (
                partition by {{ key_columns | join(', ') }}
                order by {{ order_column }} {{ order_direction }}
            ) as _rn
        from {{ source_table }}
    )
    where _rn = 1
{% endmacro %}


{% macro get_latest_record(source_ref, key_columns, timestamp_column) %}
{#
    ISSUE: Common pattern that should use QUALIFY in Snowflake
#}
    select
        {{ dbt_utils.star(from=source_ref, except=['_rn']) }}
    from (
        select
            *,
            row_number() over (
                partition by {{ key_columns | join(', ') }}
                order by {{ timestamp_column }} desc
            ) as _rn
        from {{ source_ref }}
    ) sub
    where _rn = 1
{% endmacro %}


{% macro scd_type2_current(source_table, key_column, effective_from_col, effective_to_col) %}
{#
    Get current record from SCD Type 2 table
    ISSUE: Could be optimized with QUALIFY
#}
    select *
    from {{ source_table }}
    where {{ effective_to_col }} is null
       or {{ effective_to_col }} >= current_date()
{% endmacro %}
