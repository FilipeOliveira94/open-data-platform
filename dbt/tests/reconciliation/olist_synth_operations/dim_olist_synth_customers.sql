{{ keys_differ(
  'dim_olist_synth_customers',
  "select customer_id as key_value from " ~ ref('dim_olist_synth_customers'),
  "select customer_id as key_value from " ~ source('silver', 'olist_synth_cdc__customers')
) }}
