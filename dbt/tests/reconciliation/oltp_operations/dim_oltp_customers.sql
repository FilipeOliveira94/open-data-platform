{{ keys_differ(
  'dim_oltp_customers',
  "select customer_id as key_value from " ~ ref('dim_oltp_customers'),
  "select customer_id as key_value from " ~ source('silver', 'postgres_oltp__customers')
) }}
