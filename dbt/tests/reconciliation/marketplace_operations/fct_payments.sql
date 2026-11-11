{{ keys_differ(
  'fct_payments',
  "select order_id || ':' || cast(payment_sequential as varchar) as key_value from " ~ ref('fct_payments'),
  "select order_id || ':' || cast(payment_sequential as varchar) as key_value from " ~ source('silver', 'olist_brazilian_ecommerce__order_payments')
) }}
union all
{{ scalars_near(
  'fct_payments_collected',
  "select coalesce(sum(payment_value), 0) from " ~ ref('fct_payments'),
  "select coalesce(sum(payment_value), 0) from " ~ source('silver', 'olist_brazilian_ecommerce__order_payments'),
  0.01
) }}
