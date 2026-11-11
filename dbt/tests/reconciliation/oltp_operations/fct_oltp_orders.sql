{{ keys_differ(
  'fct_oltp_orders',
  "select order_id as key_value from " ~ ref('fct_oltp_orders'),
  "select order_id as key_value from " ~ source('silver', 'postgres_oltp__orders')
) }}
