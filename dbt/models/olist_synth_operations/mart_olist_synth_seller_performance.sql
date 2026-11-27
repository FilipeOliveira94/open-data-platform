{{ config(
    schema='olist_synth_operations',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('olist_synth_operations', 'mart_olist_synth_seller_performance') ~ "'"
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
  from {{ ref('fct_olist_synth_order_items') }} i
  group by 1, 2
)
select
  io.seller_id,
  sum(io.item_gmv) as item_gmv,
  count(distinct io.order_id) as order_count,
  -- Money sums stay decimal; the ratio is a double (a decimal quotient is rounded at six places).
  cast(sum(io.freight_sum) as double) / nullif(cast(sum(io.price_sum) as double), 0) as freight_to_price_ratio,
  -- Only (seller, order) pairs with a carrier date and a shipping limit count.
  count_if(
    o.order_delivered_carrier_date is not null
    and io.shipping_limit_date is not null
  ) as handoff_eligible_count,
  count_if(o.order_delivered_carrier_date <= io.shipping_limit_date) * 1.0
    / nullif(
      count_if(
        o.order_delivered_carrier_date is not null
        and io.shipping_limit_date is not null
      ),
      0
    ) as on_time_handoff_rate
from item_orders io
join {{ ref('fct_olist_synth_orders') }} o on o.order_id = io.order_id
group by 1
