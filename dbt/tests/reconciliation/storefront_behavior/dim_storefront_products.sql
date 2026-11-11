{{ keys_differ(
  'dim_storefront_products',
  "select product_id as key_value from " ~ ref('dim_storefront_products'),
  "select distinct product_id as key_value from " ~ source('silver', 'demo_storefront__clickstream') ~ " where product_id is not null"
) }}
