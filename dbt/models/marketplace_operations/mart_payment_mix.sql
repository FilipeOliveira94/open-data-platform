{{ config(
    schema='marketplace_operations',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('marketplace_operations', 'mart_payment_mix') ~ "'"
    }
) }}

select
  payment_type as payment_method,
  payment_installments as installment_count,
  count(*) as payment_row_count,
  count(distinct order_id) as order_count,
  sum(payment_value) as amount_collected
from {{ ref('fct_payments') }}
group by 1, 2

