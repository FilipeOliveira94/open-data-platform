{{ config(
    schema='oltp_operations',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('oltp_operations', 'mart_oltp_platform_sales') ~ "'"
    }
) }}

with items as (
  select order_id, sum(item_gmv) as item_gmv
  from {{ ref('fct_oltp_order_items') }}
  group by 1
)
select
  o.purchase_date,
  count(*) as order_count,
  count_if(o.order_status = 'delivered') as delivered_order_count,
  coalesce(sum(i.item_gmv), 0) as item_gmv
from {{ ref('fct_oltp_orders') }} o
left join items i on i.order_id = o.order_id
group by 1

