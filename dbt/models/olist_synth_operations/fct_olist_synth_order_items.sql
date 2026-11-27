{{ config(
    schema='olist_synth_operations',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('olist_synth_operations', 'fct_olist_synth_order_items') ~ "'"
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
  {{ pad_zip5('s.seller_zip_code_prefix') }} as seller_zip_prefix,
  s.seller_city,
  s.seller_state as seller_state_code,
  cast(i.updated_at as timestamp) as updated_at
from {{ source('silver', 'olist_synth_cdc__order_items') }} i
left join {{ source('silver', 'olist_synth_cdc__sellers') }} s
  on s.seller_id = i.seller_id

