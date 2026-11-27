{{ keys_differ(
  'fct_olist_synth_orders',
  "select order_id as key_value from " ~ ref('fct_olist_synth_orders'),
  "select order_id as key_value from " ~ source('silver', 'olist_synth_cdc__orders')
) }}
