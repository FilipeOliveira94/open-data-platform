{{ config(
    schema='olist_synth_operations',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('olist_synth_operations', 'mart_olist_synth_order_milestones') ~ "'"
    }
) }}

select
  date_trunc('month', o.order_purchase_timestamp) as purchase_month,
  count(*) as order_count,
  count_if(o.order_approved_at is not null) as approved_count,
  count_if(o.order_delivered_carrier_date is not null) as carrier_handoff_count,
  count_if(o.order_delivered_customer_date is not null) as customer_delivery_count
from {{ ref('fct_olist_synth_orders') }} o
group by 1

