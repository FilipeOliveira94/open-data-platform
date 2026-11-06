{{ config(
    schema='oltp_operations',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('oltp_operations', 'mart_oltp_seller_performance') ~ "'"
    }
) }}

with item_orders as (
  select
    i.seller_id,
    i.order_id,
    sum(i.item_gmv) as item_gmv,
    sum(i.price) as price_sum,
    sum(i.freight_value) as freight_sum,
    max(i.shipping_limit_date) as shipping_limit_date
  from {{ ref('fct_oltp_order_items') }} i
  group by 1, 2
)
select
  io.seller_id,
  sum(io.item_gmv) as item_gmv,
  count(distinct io.order_id) as order_count,
  sum(io.freight_sum) / nullif(sum(io.price_sum), 0) as freight_to_price_ratio,
  avg(
    case
      when o.order_delivered_carrier_date is not null
       and io.shipping_limit_date is not null
       and o.order_delivered_carrier_date <= io.shipping_limit_date
      then 1.0 else 0.0
    end
  ) as on_time_handoff_rate
from item_orders io
join {{ ref('fct_oltp_orders') }} o on o.order_id = io.order_id
group by 1

