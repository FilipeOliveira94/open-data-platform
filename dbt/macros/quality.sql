{% macro finding_id(parts) -%}
to_hex(sha256(to_utf8(concat_ws('|', {{ parts }}))))
{%- endmacro %}
