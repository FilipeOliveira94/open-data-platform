{% test accepted_range(model, column_name, min_value=none, max_value=none) %}
select {{ column_name }} as value
from {{ model }}
where {{ column_name }} is not null
  and (
    {% if min_value is not none and max_value is not none %}
    {{ column_name }} < {{ min_value }} or {{ column_name }} > {{ max_value }}
    {% elif min_value is not none %}
    {{ column_name }} < {{ min_value }}
    {% elif max_value is not none %}
    {{ column_name }} > {{ max_value }}
    {% else %}
    false
    {% endif %}
  )
{% endtest %}
