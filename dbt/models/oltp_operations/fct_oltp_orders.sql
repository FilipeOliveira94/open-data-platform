{{ config(
    schema='oltp_operations',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('oltp_operations', 'fct_oltp_orders') ~ "'"
    }
) }}

select
  o.order_id,
  o.customer_id,
  c.customer_unique_id,
  o.order_status,
  cast(o.order_purchase_timestamp as timestamp) as order_purchase_timestamp,
  cast(o.order_approved_at as timestamp) as order_approved_at,
  cast(o.order_delivered_carrier_date as timestamp) as order_delivered_carrier_date,
  cast(o.order_delivered_customer_date as timestamp) as order_delivered_customer_date,
  cast(o.order_estimated_delivery_date as timestamp) as order_estimated_delivery_date,
  cast(o.order_purchase_timestamp as date) as purchase_date,
  c.customer_city as purchase_city,
  c.customer_state as purchase_state,
  {{ pad_zip5('c.customer_zip_code_prefix') }} as purchase_zip_prefix,
  m.municipality_id,
  m.municipality_name,
  cast(o.updated_at as timestamp) as updated_at
from {{ source('silver', 'postgres_oltp__orders') }} o
left join {{ source('silver', 'postgres_oltp__customers') }} c
  on c.customer_id = o.customer_id
left join {{ ref('dim_municipality') }} m
  on m.municipality_name_norm = {{ normalize_place('c.customer_city') }}
 and m.state_code = c.customer_state

