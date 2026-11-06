{{ config(
    schema='marketplace_operations',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('marketplace_operations', 'mart_order_milestones') ~ "'"
    }
) }}

select
  date_trunc('month', o.order_purchase_timestamp) as purchase_month,
  coalesce(pp.primary_payment_method, 'unknown') as primary_payment_method,
  count(*) as order_count,
  count_if(o.order_approved_at is not null) as approved_count,
  count_if(o.order_delivered_carrier_date is not null) as carrier_handoff_count,
  count_if(o.order_delivered_customer_date is not null) as customer_delivery_count,
  count_if(o.order_status = 'canceled' and o.order_approved_at is not null) as canceled_after_approval_count,
  count_if(o.order_status = 'unavailable') as unavailable_count
from {{ ref('fct_orders') }} o
left join {{ ref('int_order_primary_payment') }} pp on pp.order_id = o.order_id
group by 1, 2

