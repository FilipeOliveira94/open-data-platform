{{ keys_differ(
  'dim_products',
  "select product_id as key_value from " ~ ref('dim_products'),
  "select product_id as key_value from " ~ source('silver', 'olist_brazilian_ecommerce__products')
) }}
union all
select
  'dim_products_category' as check_name,
  product_id as key_value,
  category_portuguese as side
from {{ ref('dim_products') }}
where (category_portuguese = 'pc_gamer' and category_english is distinct from 'gaming_pc')
   or (
     category_portuguese = 'portateis_cozinha_e_preparadores_de_alimentos'
     and category_english is distinct from 'portable_kitchen_appliances_and_food_processors'
   )
