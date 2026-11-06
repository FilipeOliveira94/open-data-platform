{{ config(
    schema='marketplace_operations',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('marketplace_operations', 'mart_order_cancellations') ~ "'"
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
  date_trunc('month', o.order_purchase_timestamp) as purchase_month,
  o.order_status,
  coalesce(pp.primary_payment_method, 'unknown') as primary_payment_method,
  count(*) as order_count,
  coalesce(sum(i.item_gmv), 0) as item_gmv,
  coalesce(sum(p.amount_collected), 0) as amount_collected,
  count_if(o.order_approved_at is not null) as approved_before_cancel_count
from {{ ref('fct_orders') }} o
left join {{ ref('int_order_primary_payment') }} pp on pp.order_id = o.order_id
left join items i on i.order_id = o.order_id
left join payments p on p.order_id = o.order_id
where o.order_status in ('canceled', 'unavailable')
group by 1, 2, 3

