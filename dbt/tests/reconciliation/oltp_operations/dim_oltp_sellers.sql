{{ keys_differ(
  'dim_oltp_sellers',
  "select seller_id as key_value from " ~ ref('dim_oltp_sellers'),
  "select seller_id as key_value from " ~ source('silver', 'postgres_oltp__sellers')
) }}
