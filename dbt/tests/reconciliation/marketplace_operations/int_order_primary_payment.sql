-- One method per paying order. A tie keeps the smaller payment_sequential.
with ranked as (
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
)
select
  'int_order_primary_payment' as check_name,
  cast(coalesce(e.order_id, g.order_id) as varchar) as key_value,
  'drift' as side
from ranked e
full outer join {{ ref('int_order_primary_payment') }} g
  on g.order_id = e.order_id
where coalesce(e.rn, 1) = 1
  and (
    g.order_id is null
    or e.order_id is null
    or g.primary_payment_method is distinct from e.payment_type
    or g.primary_payment_sequential is distinct from e.payment_sequential
    or g.primary_payment_value is distinct from e.payment_value
  )
