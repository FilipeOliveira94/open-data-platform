{{ keys_differ(
  'fct_storefront_events',
  "select event_id as key_value from " ~ ref('fct_storefront_events'),
  "select event_id as key_value from " ~ source('silver', 'demo_storefront__clickstream')
) }}
