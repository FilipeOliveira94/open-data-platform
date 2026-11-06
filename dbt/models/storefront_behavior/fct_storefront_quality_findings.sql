{{ config(
    schema='storefront_behavior',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('storefront_behavior', 'fct_storefront_quality_findings') ~ "'"
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
  where source_dataset = 'demo_storefront.clickstream'
),
business as (
  select
    {{ finding_id("array['storefront_behavior', 'cart_or_purchase_without_product', event_id]") }} as finding_id,
    'business_anomaly' as issue_class,
    'cart_or_purchase_without_product' as reason,
    'demo_storefront.clickstream' as source_dataset,
    event_id as business_key,
    'product_id' as field,
    'Cart or purchase event has no product' as detail
  from {{ ref('fct_storefront_events') }}
  where event_type in ('add_to_cart', 'purchase') and product_id is null
)
select * from contract_violations
union all
select * from business

