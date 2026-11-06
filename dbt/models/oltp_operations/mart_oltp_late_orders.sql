{{ config(
    schema='oltp_operations',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('oltp_operations', 'mart_oltp_late_orders') ~ "'"
    }
) }}

with item_agg as (
  select
    order_id,
    max(shipping_limit_date) as shipping_limit_date,
    min_by(seller_id, order_item_id) as seller_id
  from {{ ref('fct_oltp_order_items') }}
  group by 1
)
select
  o.order_id,
  o.purchase_date,
  o.purchase_state as customer_state,
  ia.seller_id,
  date_diff(
    'day',
    cast(o.order_estimated_delivery_date as date),
    cast(o.order_delivered_customer_date as date)
  ) as days_late,
  case
    when o.order_delivered_carrier_date is not null
     and ia.shipping_limit_date is not null
     and o.order_delivered_carrier_date > ia.shipping_limit_date
    then true else false
  end as carrier_handoff_missed_deadline
from {{ ref('fct_oltp_orders') }} o
join item_agg ia on ia.order_id = o.order_id
where o.order_status = 'delivered'
  and o.order_delivered_customer_date is not null
  and o.order_estimated_delivery_date is not null
  and cast(o.order_delivered_customer_date as date) > cast(o.order_estimated_delivery_date as date)

