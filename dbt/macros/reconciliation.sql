{% macro scalars_differ(check_name, left_scalar, right_scalar) %}
select
  '{{ check_name }}' as check_name,
  cast(left_value as varchar) as left_value,
  cast(right_value as varchar) as right_value
from (
  select
    ({{ left_scalar }}) as left_value,
    ({{ right_scalar }}) as right_value
) compared
where left_value is distinct from right_value
{% endmacro %}

{% macro scalars_near(check_name, left_scalar, right_scalar, tolerance) %}
select
  '{{ check_name }}' as check_name,
  cast(left_value as varchar) as left_value,
  cast(right_value as varchar) as right_value
from (
  select
    cast(({{ left_scalar }}) as double) as left_value,
    cast(({{ right_scalar }}) as double) as right_value
) compared
where abs(left_value - right_value) > {{ tolerance }}
   or (left_value is null) <> (right_value is null)
{% endmacro %}

{% macro keys_differ(check_name, left_sql, right_sql) %}
select
  '{{ check_name }}' as check_name,
  cast(key_value as varchar) as key_value,
  side
from (
  select l.key_value, 'left_only' as side
  from ({{ left_sql }}) l
  where not exists (
    select 1
    from ({{ right_sql }}) r
    where r.key_value is not distinct from l.key_value
  )
  union all
  select r.key_value, 'right_only' as side
  from ({{ right_sql }}) r
  where not exists (
    select 1
    from ({{ left_sql }}) l
    where l.key_value is not distinct from r.key_value
  )
) drifted
{% endmacro %}
