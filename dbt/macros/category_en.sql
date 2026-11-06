{% macro category_english(portuguese_name, published_english) -%}
coalesce(
  nullif(trim({{ published_english }}), ''),
  case trim({{ portuguese_name }})
    when 'pc_gamer' then 'gaming_pc'
    when 'portateis_cozinha_e_preparadores_de_alimentos'
      then 'portable_kitchen_appliances_and_food_processors'
    else null
  end
)
{%- endmacro %}
