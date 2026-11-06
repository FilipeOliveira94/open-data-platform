{{ config(
    schema='oltp_operations',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('oltp_operations', 'mart_oltp_delivery_performance') ~ "'"
    }
) }}

with item_limits as (
  select order_id, max(shipping_limit_date) as shipping_limit_date
  from {{ ref('fct_oltp_order_items') }}
  group by 1
),
base as (
  select
    date_trunc('month', o.order_purchase_timestamp) as purchase_month,
    case
      when o.order_status = 'delivered'
       and o.order_delivered_customer_date is not null
       and o.order_estimated_delivery_date is not null
       and cast(o.order_delivered_customer_date as date) <= cast(o.order_estimated_delivery_date as date)
      then 1 else 0
    end as on_time_flag,
    case
      when o.order_status = 'delivered'
       and o.order_delivered_customer_date is not null
       and o.order_estimated_delivery_date is not null
       and cast(o.order_delivered_customer_date as date) > cast(o.order_estimated_delivery_date as date)
      then 1 else 0
    end as late_flag,
    case
      when o.order_status = 'delivered'
       and o.order_delivered_customer_date is not null
       and o.order_estimated_delivery_date is not null
      then 1 else 0
    end as rate_eligible,
    case
      when o.order_delivered_carrier_date is not null
       and il.shipping_limit_date is not null
       and o.order_delivered_carrier_date <= il.shipping_limit_date
      then 1 else 0
    end as on_time_handoff_flag
  from {{ ref('fct_oltp_orders') }} o
  left join item_limits il on il.order_id = o.order_id
)
select
  purchase_month,
  sum(rate_eligible) as delivered_with_both_dates,
  sum(on_time_flag) * 1.0 / nullif(sum(rate_eligible), 0) as on_time_rate,
  sum(late_flag) * 1.0 / nullif(sum(rate_eligible), 0) as late_rate,
  avg(cast(on_time_handoff_flag as double)) as on_time_handoff_rate
from base
group by 1

