{{ config(materialized='ephemeral') }}

select
  order_id,
  payment_type as primary_payment_method,
  payment_sequential as primary_payment_sequential,
  payment_value as primary_payment_value
from (
  select
    order_id,
    payment_type,
    payment_sequential,
    payment_value,
    row_number() over (
      partition by order_id
      order by payment_value desc, payment_sequential asc
    ) as rn
  from {{ ref('fct_payments') }}
) p
where rn = 1

