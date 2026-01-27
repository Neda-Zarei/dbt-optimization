-- Aggregation macros
-- ISSUES FOR ARTEMIS TO RECOGNIZE:
-- Patterns that could be optimized for Snowflake

{% macro safe_divide(numerator, denominator, default_value=0) %}
{# Safe division avoiding divide by zero #}
    case
        when {{ denominator }} != 0 and {{ denominator }} is not null
        then {{ numerator }} / {{ denominator }}
        else {{ default_value }}
    end
{% endmacro %}


{% macro weighted_average(value_col, weight_col) %}
{# Calculate weighted average
   ISSUE: Commonly repeated pattern #}
    {{ safe_divide(
        'sum(' ~ value_col ~ ' * ' ~ weight_col ~ ')',
        'sum(' ~ weight_col ~ ')'
    ) }}
{% endmacro %}


{% macro running_total(value_col, partition_cols, order_col) %}
{# Running total calculation #}
    sum({{ value_col }}) over (
        partition by {{ partition_cols | join(', ') }}
        order by {{ order_col }}
        rows between unbounded preceding and current row
    )
{% endmacro %}


{% macro lag_compare(value_col, partition_cols, order_col, lag_periods=1) %}
{# Compare to lagged value
   ISSUE: Often called multiple times with different lag periods #}
    {{ value_col }} - coalesce(
        lag({{ value_col }}, {{ lag_periods }}) over (
            partition by {{ partition_cols | join(', ') }}
            order by {{ order_col }}
        ),
        0
    )
{% endmacro %}


{% macro pivot_cashflow_types(cashflow_col, amount_col, types=['CONTRIBUTION', 'DISTRIBUTION', 'DIVIDEND', 'FEE']) %}
{# Pivot cashflow types into columns
   ISSUE: Could use Snowflake PIVOT for better performance #}
    {% for cf_type in types %}
    sum(case when {{ cashflow_col }} = '{{ cf_type }}' then {{ amount_col }} else 0 end) as {{ cf_type | lower }}s
    {%- if not loop.last %},{% endif %}
    {% endfor %}
{% endmacro %}


{% macro categorize_amount(amount_col, thresholds) %}
{# Categorize amounts into buckets
   ISSUE: Repeated CASE pattern
   thresholds should be a list of (threshold, label) tuples #}
    case
        {% for threshold, label in thresholds %}
        when abs({{ amount_col }}) >= {{ threshold }} then '{{ label }}'
        {% endfor %}
        else 'MICRO'
    end
{% endmacro %}
