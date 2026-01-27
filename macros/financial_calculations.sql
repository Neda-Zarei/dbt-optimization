-- Financial calculation macros
-- ISSUES FOR ARTEMIS TO RECOGNIZE:
-- These contain calculation patterns that may be duplicated across models

{% macro calculate_returns(price_col, partition_cols, order_col) %}
{# Calculate various return metrics
   ISSUE: This creates multiple window functions that could be consolidated #}
    -- Daily return
    ({{ price_col }} - lag({{ price_col }}, 1) over (
        partition by {{ partition_cols | join(', ') }}
        order by {{ order_col }}
    )) / nullif(lag({{ price_col }}, 1) over (
        partition by {{ partition_cols | join(', ') }}
        order by {{ order_col }}
    ), 0) as daily_return,

    -- Log return (for compounding)
    ln({{ price_col }} / nullif(lag({{ price_col }}, 1) over (
        partition by {{ partition_cols | join(', ') }}
        order by {{ order_col }}
    ), 0)) as log_return
{% endmacro %}


{% macro rolling_return(return_col, partition_cols, order_col, periods) %}
{# Calculate rolling compounded return
   ISSUE: Called multiple times with different periods, could optimize #}
    exp(sum(ln(1 + coalesce({{ return_col }}, 0))) over (
        partition by {{ partition_cols | join(', ') }}
        order by {{ order_col }}
        rows between {{ periods - 1 }} preceding and current row
    )) - 1
{% endmacro %}


{% macro rolling_volatility(return_col, partition_cols, order_col, periods, annualization_factor=252) %}
{# Calculate rolling volatility
   ISSUE: Often called alongside rolling_return, could combine #}
    stddev({{ return_col }}) over (
        partition by {{ partition_cols | join(', ') }}
        order by {{ order_col }}
        rows between {{ periods - 1 }} preceding and current row
    ) * sqrt({{ annualization_factor }})
{% endmacro %}


{% macro sharpe_ratio(return_col, partition_cols, order_col, periods, risk_free_rate=0) %}
{# Calculate Sharpe ratio
   ISSUE: Requires both return and volatility calculations #}
    case
        when {{ rolling_volatility(return_col, partition_cols, order_col, periods) }} > 0
        then (
            avg({{ return_col }}) over (
                partition by {{ partition_cols | join(', ') }}
                order by {{ order_col }}
                rows between {{ periods - 1 }} preceding and current row
            ) * 252 - {{ risk_free_rate }}
        ) / {{ rolling_volatility(return_col, partition_cols, order_col, periods) }}
        else null
    end
{% endmacro %}


{% macro calculate_drawdown(nav_col, partition_cols, order_col) %}
{# Calculate running drawdown
   ISSUE: Multiple window functions for related metrics #}
    -- Running max
    max({{ nav_col }}) over (
        partition by {{ partition_cols | join(', ') }}
        order by {{ order_col }}
        rows between unbounded preceding and current row
    ) as running_max,

    -- Current drawdown
    ({{ nav_col }} - max({{ nav_col }}) over (
        partition by {{ partition_cols | join(', ') }}
        order by {{ order_col }}
        rows between unbounded preceding and current row
    )) / nullif(max({{ nav_col }}) over (
        partition by {{ partition_cols | join(', ') }}
        order by {{ order_col }}
        rows between unbounded preceding and current row
    ), 0) as drawdown
{% endmacro %}


{% macro modified_dietz_return(end_nav, begin_nav, contributions, distributions, weight_factor=0.5) %}
{# Modified Dietz return calculation
   ISSUE: This formula is repeated in multiple models #}
    case
        when ({{ begin_nav }} + {{ contributions }} * {{ weight_factor }} - {{ distributions }} * {{ weight_factor }}) > 0
        then (
            {{ end_nav }} - {{ begin_nav }} - {{ contributions }} + {{ distributions }}
        ) / (
            {{ begin_nav }} + {{ contributions }} * {{ weight_factor }} - {{ distributions }} * {{ weight_factor }}
        )
        else null
    end
{% endmacro %}


{% macro irr_approximation(total_distributions, total_contributions, nav, years) %}
{# Simple IRR approximation (Newton-Raphson would require iteration)
   ISSUE: This is a rough approximation #}
    case
        when {{ total_contributions }} > 0 and {{ years }} > 0
        then power(
            ({{ total_distributions }} + {{ nav }}) / {{ total_contributions }},
            1.0 / {{ years }}
        ) - 1
        else null
    end
{% endmacro %}
