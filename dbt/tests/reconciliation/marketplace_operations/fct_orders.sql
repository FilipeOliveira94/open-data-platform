{{ keys_differ(
  'fct_orders',
  "select order_id as key_value from " ~ ref('fct_orders'),
  "select order_id as key_value from " ~ source('silver', 'olist_brazilian_ecommerce__orders')
) }}
