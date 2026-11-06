{% macro gold_location(schema_name, table_name) -%}
s3a://{{ var('warehouse_bucket') }}/{{ var('gold_location_prefix') }}/{{ schema_name }}/{{ table_name }}
{%- endmacro %}

{% macro gold_table_config(schema_name, table_name) -%}
{{ config(
    schema=schema_name,
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location(schema_name, table_name) ~ "'"
    }
) }}
{%- endmacro %}
