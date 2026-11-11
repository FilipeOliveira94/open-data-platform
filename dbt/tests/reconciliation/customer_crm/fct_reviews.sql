{{ keys_differ(
  'fct_reviews',
  "select review_id || ':' || order_id as key_value from " ~ ref('fct_reviews'),
  "select review_id || ':' || order_id as key_value from " ~ source('silver', 'olist_brazilian_ecommerce__order_reviews')
) }}
