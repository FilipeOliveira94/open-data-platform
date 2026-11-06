{{ config(
    schema='marketplace_operations',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('marketplace_operations', 'mart_seller_buyer_geography') ~ "'"
    }
) }}

select
  i.seller_id,
  o.municipality_id,
  o.municipality_name,
  count(distinct o.order_id) as order_count,
  sum(i.item_gmv) as item_gmv
from {{ ref('fct_order_items') }} i
join {{ ref('fct_orders') }} o on o.order_id = i.order_id
group by 1, 2, 3

