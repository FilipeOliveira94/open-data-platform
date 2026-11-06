{{ config(
    schema='marketplace_operations',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('marketplace_operations', 'mart_seller_performance') ~ "'"
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
  from {{ ref('fct_order_items') }} i
  group by 1, 2
),
joined as (
  select
    io.seller_id,
    io.order_id,
    io.item_gmv,
    io.price_sum,
    io.freight_sum,
    o.order_delivered_carrier_date,
    o.municipality_id as buyer_municipality_id,
    case
      when o.order_delivered_carrier_date is not null
       and io.shipping_limit_date is not null
       and o.order_delivered_carrier_date <= io.shipping_limit_date
      then 1 else 0
    end as on_time_handoff
  from item_orders io
  join {{ ref('fct_orders') }} o on o.order_id = io.order_id
)
select
  seller_id,
  sum(item_gmv) as item_gmv,
  count(distinct order_id) as order_count,
  avg(cast(on_time_handoff as double)) as on_time_handoff_rate,
  sum(freight_sum) / nullif(sum(price_sum), 0) as freight_to_price_ratio,
  count(distinct buyer_municipality_id) as distinct_buyer_municipalities
from joined
group by 1

