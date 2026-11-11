{{ scalars_differ(
  'storefront_contract_violation',
  "select count(*) from " ~ ref('fct_storefront_quality_findings') ~ " where issue_class = 'contract_violation'",
  "select count(*) from " ~ source('gold_staging', 'quarantine_findings') ~ " where source_dataset = 'demo_storefront.clickstream'"
) }}
union all
{{ scalars_differ(
  'cart_or_purchase_without_product',
  "select count(*) from " ~ ref('fct_storefront_quality_findings') ~ " where reason = 'cart_or_purchase_without_product'",
  "select count(*) from " ~ ref('fct_storefront_events') ~ " where event_type in ('add_to_cart', 'purchase') and product_id is null"
) }}
