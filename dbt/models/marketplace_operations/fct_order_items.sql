{{ config(
    schema='marketplace_operations',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('marketplace_operations', 'fct_order_items') ~ "'"
    }
) }}

select
  i.order_id,
  i.order_item_id,
  i.product_id,
  i.seller_id,
  cast(i.shipping_limit_date as timestamp) as shipping_limit_date,
  i.price,
  i.freight_value,
  i.price + i.freight_value as item_gmv,
  s.seller_city,
  s.seller_state as seller_state_code,
  s.seller_zip_code_prefix as seller_zip_prefix,
  m.municipality_id as seller_municipality_id,
  m.municipality_name as seller_municipality_name
from {{ source('silver', 'olist_brazilian_ecommerce__order_items') }} i
left join {{ source('silver', 'olist_brazilian_ecommerce__sellers') }} s
  on s.seller_id = i.seller_id
left join {{ ref('dim_municipality') }} m
  on m.municipality_name_norm = {{ normalize_place('s.seller_city') }}
 and m.state_code = s.seller_state

