{# City + UF match: lowercase, strip accents, hyphen to space (gold-model-design). #}
{% macro normalize_place(expr) -%}
regexp_replace(
  replace(
    regexp_replace(
      normalize(lower(trim(cast({{ expr }} as varchar))), NFD),
      '\p{M}',
      ''
    ),
    '-',
    ' '
  ),
  '\s+',
  ' '
)
{%- endmacro %}
