{{ config(
    schema='marketplace_operations',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('marketplace_operations', 'mart_late_orders') ~ "'"
    }
) }}

with item_agg as (
  select
    order_id,
    max(shipping_limit_date) as shipping_limit_date,
    sum(price) as price_sum,
    sum(freight_value) as freight_sum,
    min_by(seller_id, order_item_id) as seller_id
  from {{ ref('fct_order_items') }}
  group by 1
),
reviews as (
  select
    order_id,
    avg(cast(review_score as double)) as mean_review_score
  from {{ source('silver', 'olist_brazilian_ecommerce__order_reviews') }}
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
  r.mean_review_score as review_score,
  ia.freight_sum / nullif(ia.price_sum, 0) as freight_to_price_ratio,
  pp.primary_payment_method,
  case
    when o.order_delivered_carrier_date is not null
     and ia.shipping_limit_date is not null
     and o.order_delivered_carrier_date > ia.shipping_limit_date
    then true else false
  end as carrier_handoff_missed_deadline
from {{ ref('fct_orders') }} o
join item_agg ia on ia.order_id = o.order_id
left join {{ ref('int_order_primary_payment') }} pp on pp.order_id = o.order_id
left join reviews r on r.order_id = o.order_id
where o.order_status = 'delivered'
  and o.order_delivered_customer_date is not null
  and o.order_estimated_delivery_date is not null
  and cast(o.order_delivered_customer_date as date) > cast(o.order_estimated_delivery_date as date)

