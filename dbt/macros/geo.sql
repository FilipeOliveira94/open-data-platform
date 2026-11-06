{% macro haversine_km(lat1, lon1, lat2, lon2) -%}
great_circle_distance({{ lat1 }}, {{ lon1 }}, {{ lat2 }}, {{ lon2 }})
{%- endmacro %}

{% macro distance_band(km_expr) -%}
case
  when {{ km_expr }} is null then 'unknown'
  when {{ km_expr }} <= 50 then '0_50km'
  when {{ km_expr }} <= 200 then '50_200km'
  when {{ km_expr }} <= 500 then '200_500km'
  else '500km_plus'
end
{%- endmacro %}

{% macro pad_zip5(expr) -%}
lpad(cast({{ expr }} as varchar), 5, '0')
{%- endmacro %}
