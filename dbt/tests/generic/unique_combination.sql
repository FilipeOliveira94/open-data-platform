{% test unique_combination(model, combination) %}
select
  {% for column in combination %}
  {{ column }}{% if not loop.last %},{% endif %}
  {% endfor %},
  count(*) as row_count
from {{ model }}
group by
  {% for column in combination %}
  {{ column }}{% if not loop.last %},{% endif %}
  {% endfor %}
having count(*) > 1
{% endtest %}
