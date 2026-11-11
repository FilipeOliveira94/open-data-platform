{% test column_types(model, expected) %}
{# Compare information_schema type families to the model column list.
   A prefix match accepts varchar(n) and timestamp(p). #}
with expected as (
  {% for name, type_family in expected.items() %}
  select '{{ name }}' as column_name, '{{ type_family }}' as type_family
  {% if not loop.last %}union all{% endif %}
  {% endfor %}
),
actual as (
  select
    lower(column_name) as column_name,
    lower(data_type) as data_type
  from {{ model.database }}.information_schema.columns
  where lower(table_schema) = lower('{{ model.schema }}')
    and lower(table_name) = lower('{{ model.identifier }}')
)
select
  e.column_name,
  e.type_family,
  a.data_type
from expected e
left join actual a
  on a.column_name = e.column_name
where a.data_type is null
   or a.data_type not like e.type_family || '%'
{% endtest %}
