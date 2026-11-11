{{ keys_differ(
  'fct_leads',
  "select mql_id as key_value from " ~ ref('fct_leads'),
  "select mql_id as key_value from " ~ source('silver', 'olist_marketing_funnel__qualified_leads')
) }}
