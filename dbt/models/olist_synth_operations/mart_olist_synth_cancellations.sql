{{ config(
    schema='olist_synth_operations',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('olist_synth_operations', 'mart_olist_synth_cancellations') ~ "'"
    }
) }}

select
  date_trunc('month', o.order_purchase_timestamp) as purchase_month,
  o.order_status,
  count(*) as order_count,
  count_if(o.order_approved_at is not null) as approved_before_cancel_count
from {{ ref('fct_olist_synth_orders') }} o
where o.order_status in ('canceled', 'unavailable')
group by 1, 2

