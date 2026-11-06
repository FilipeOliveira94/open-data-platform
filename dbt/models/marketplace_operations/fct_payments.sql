{{ config(
    schema='marketplace_operations',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('marketplace_operations', 'fct_payments') ~ "'"
    }
) }}

select
  order_id,
  payment_sequential,
  payment_type,
  payment_installments,
  payment_value
from {{ source('silver', 'olist_brazilian_ecommerce__order_payments') }}

