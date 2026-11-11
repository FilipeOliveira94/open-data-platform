{{ scalars_differ(
  'catalog_contract_violation',
  "select count(*) from " ~ ref('fct_catalog_quality_findings') ~ " where issue_class = 'contract_violation'",
  "select count(*) from " ~ source('gold_staging', 'quarantine_findings') ~ " where source_dataset = 'dummyjson_products.products'"
) }}
union all
{{ scalars_differ(
  'repeated_reviewer_email',
  "select count(*) from " ~ ref('fct_catalog_quality_findings') ~ " where reason = 'repeated_reviewer_email'",
  "select count(*) from (select product_id, reviewer_email from " ~ ref('fct_catalog_reviews') ~ " group by 1, 2 having count(*) > 1) repeated"
) }}
