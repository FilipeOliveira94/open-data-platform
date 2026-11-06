{{ config(
    schema='catalog_commerce',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('catalog_commerce', 'fct_catalog_quality_findings') ~ "'"
    }
) }}

with contract_violations as (
  select
    {{ finding_id("array[source_dataset, reason, coalesce(field_name, ''), business_key, cast(record_index as varchar), 'contract_violation']") }} as finding_id,
    'contract_violation' as issue_class,
    reason,
    source_dataset,
    business_key,
    field_name as field,
    detail
  from {{ source('gold_staging', 'quarantine_findings') }}
  where source_dataset = 'dummyjson_products.products'
),
business as (
  select
    {{ finding_id("array['catalog_commerce', 'repeated_reviewer_email', cast(product_id as varchar), reviewer_email]") }} as finding_id,
    'business_anomaly' as issue_class,
    'repeated_reviewer_email' as reason,
    'dummyjson_products.products' as source_dataset,
    cast(product_id as varchar) || ':' || reviewer_email as business_key,
    'reviewer_email' as field,
    'Reviewer email appears more than once on the same product' as detail
  from (
    select product_id, reviewer_email
    from {{ ref('fct_catalog_reviews') }}
    group by 1, 2
    having count(*) > 1
  ) d
)
select * from contract_violations
union all
select * from business

