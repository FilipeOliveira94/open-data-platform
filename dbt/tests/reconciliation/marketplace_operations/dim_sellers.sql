{{ keys_differ(
  'dim_sellers',
  "select seller_id as key_value from " ~ ref('dim_sellers'),
  "select seller_id as key_value from " ~ source('silver', 'olist_brazilian_ecommerce__sellers')
) }}
