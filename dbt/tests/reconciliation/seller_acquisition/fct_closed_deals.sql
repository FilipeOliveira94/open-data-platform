{{ keys_differ(
  'fct_closed_deals',
  "select mql_id as key_value from " ~ ref('fct_closed_deals'),
  "select mql_id as key_value from " ~ source('silver', 'olist_marketing_funnel__closed_deals')
) }}
