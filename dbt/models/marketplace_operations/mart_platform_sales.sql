{{ config(
    schema='marketplace_operations',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('marketplace_operations', 'mart_platform_sales') ~ "'"
    }
) }}

with items as (
  select order_id, sum(item_gmv) as item_gmv
  from {{ ref('fct_order_items') }}
  group by 1
),
payments as (
  select order_id, sum(payment_value) as amount_collected
  from {{ ref('fct_payments') }}
  group by 1
)
select
  o.purchase_date,
  count(*) as order_count,
  count_if(o.order_status = 'delivered') as delivered_order_count,
  coalesce(sum(i.item_gmv), 0) as item_gmv,
  coalesce(sum(p.amount_collected), 0) as amount_collected
from {{ ref('fct_orders') }} o
left join items i on i.order_id = o.order_id
left join payments p on p.order_id = o.order_id
group by 1

