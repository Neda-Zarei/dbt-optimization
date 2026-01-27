-- Generic test for financial data accuracy
-- This test validates that key financial metrics are within expected ranges

{% test valid_nav(model, column_name) %}

select
    {{ column_name }}
from {{ model }}
where {{ column_name }} < 0
   or {{ column_name }} > 100000000000  -- NAV > 100B is suspicious

{% endtest %}


{% test valid_return(model, column_name) %}

select
    {{ column_name }}
from {{ model }}
where {{ column_name }} < -1  -- Can't lose more than 100%
   or {{ column_name }} > 10   -- 1000% return is suspicious

{% endtest %}


{% test valid_weight(model, column_name) %}

select
    {{ column_name }}
from {{ model }}
where {{ column_name }} < 0
   or {{ column_name }} > 1

{% endtest %}


{% test valid_ratio(model, column_name, min_val=-10, max_val=10) %}

select
    {{ column_name }}
from {{ model }}
where {{ column_name }} < {{ min_val }}
   or {{ column_name }} > {{ max_val }}

{% endtest %}
