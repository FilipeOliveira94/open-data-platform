{{ config(
    schema='customer_crm',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('customer_crm', 'mart_customer_retention') ~ "'"
    }
) }}

with delivered as (
  select
    customer_unique_id,
    order_id,
    order_purchase_timestamp,
    purchase_date
  from {{ ref('fct_orders') }}
  where order_status = 'delivered'
)
select
  customer_unique_id,
  min(purchase_date) as first_delivered_purchase_date,
  max(purchase_date) as last_delivered_purchase_date,
  count(*) as delivered_order_count,
  count(*) > 1 as is_repeat_buyer
from delivered
group by 1

