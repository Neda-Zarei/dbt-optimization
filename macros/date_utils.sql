-- Date utility macros
-- ISSUES FOR ARTEMIS TO RECOGNIZE:
-- These macros contain patterns that could be optimized when used

{% macro get_date_spine(start_date, end_date) %}
{# Generates a date spine - useful for gap filling #}
    select
        dateadd(
            day,
            seq4(),
            '{{ start_date }}'::date
        ) as date_day
    from table(generator(rowcount => datediff(day, '{{ start_date }}'::date, '{{ end_date }}'::date) + 1))
{% endmacro %}


{% macro fiscal_year_start(date_col, fiscal_month_start=1) %}
{# Calculate fiscal year start based on date #}
    case
        when extract(month from {{ date_col }}) >= {{ fiscal_month_start }}
        then date_trunc('year', {{ date_col }})
        else dateadd('year', -1, date_trunc('year', {{ date_col }}))
    end
{% endmacro %}


{% macro get_period_dates(period_type) %}
{# Get standard period boundaries #}
{% if period_type == 'mtd' %}
    date_trunc('month', current_date())
{% elif period_type == 'qtd' %}
    date_trunc('quarter', current_date())
{% elif period_type == 'ytd' %}
    date_trunc('year', current_date())
{% elif period_type == 'trailing_12m' %}
    dateadd('month', -12, current_date())
{% else %}
    dateadd('day', -{{ var('default_lookback_days', 90) }}, current_date())
{% endif %}
{% endmacro %}


{% macro business_days_between(start_date, end_date) %}
{# Calculate business days between two dates - ISSUE: Could be more efficient #}
    (
        select count(*)
        from ({{ get_date_spine(start_date, end_date) }}) ds
        where dayofweek(ds.date_day) not in (0, 6)  -- Exclude weekends
    )
{% endmacro %}
