{{ keys_differ(
  'dim_storefront_users',
  "select user_id as key_value from " ~ ref('dim_storefront_users'),
  "select distinct user_id as key_value from " ~ source('silver', 'demo_storefront__clickstream')
) }}
