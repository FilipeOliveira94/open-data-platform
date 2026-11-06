{{ config(
    schema='oltp_operations',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('oltp_operations', 'mart_oltp_buyer_geography') ~ "'"
    }
) }}

select
  i.seller_id,
  o.municipality_id,
  o.municipality_name,
  count(distinct o.order_id) as order_count,
  sum(i.item_gmv) as item_gmv
from {{ ref('fct_oltp_order_items') }} i
join {{ ref('fct_oltp_orders') }} o on o.order_id = i.order_id
group by 1, 2, 3

