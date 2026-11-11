{{ scalars_differ(
  'crm_contract_violation',
  "select count(*) from " ~ ref('fct_crm_quality_findings') ~ " where issue_class = 'contract_violation'",
  "select count(*) from " ~ source('gold_staging', 'quarantine_findings') ~ " where source_dataset in ('olist_brazilian_ecommerce.customers', 'olist_brazilian_ecommerce.order_reviews')"
) }}
union all
{{ scalars_differ(
  'address_without_municipality',
  "select count(*) from " ~ ref('fct_crm_quality_findings') ~ " where reason = 'address_without_municipality'",
  "select count(*) from " ~ ref('fct_orders') ~ " where municipality_id is null"
) }}
union all
{{ scalars_differ(
  'zip_prefix_unknown',
  "select count(*) from " ~ ref('fct_crm_quality_findings') ~ " where reason = 'zip_prefix_unknown'",
  "select count(*) from " ~ ref('fct_orders') ~ " o where not exists (select 1 from " ~ ref('dim_zip_prefix') ~ " z where z.zip_prefix = o.purchase_zip_prefix)"
) }}
union all
{{ scalars_differ(
  'review_without_order',
  "select count(*) from " ~ ref('fct_crm_quality_findings') ~ " where reason = 'review_without_order'",
  "select count(*) from " ~ ref('fct_reviews') ~ " r where not exists (select 1 from " ~ ref('fct_orders') ~ " o where o.order_id = r.order_id)"
) }}
