{{ config(
    schema='customer_crm',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('customer_crm', 'fct_crm_quality_findings') ~ "'"
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
  where source_dataset in (
    'olist_brazilian_ecommerce.customers',
    'olist_brazilian_ecommerce.order_reviews'
  )
),
business as (
  select
    {{ finding_id("array['customer_crm', 'address_without_municipality', order_id]") }} as finding_id,
    'business_anomaly' as issue_class,
    'address_without_municipality' as reason,
    'olist_brazilian_ecommerce.orders' as source_dataset,
    order_id as business_key,
    'purchase_city' as field,
    'Purchase address did not match a municipality' as detail
  from {{ ref('fct_orders') }}
  where municipality_id is null

  union all

  select
    {{ finding_id("array['customer_crm', 'zip_prefix_unknown', order_id]") }},
    'business_anomaly',
    'zip_prefix_unknown',
    'olist_brazilian_ecommerce.orders',
    order_id,
    'purchase_zip_prefix',
    'Purchase ZIP prefix missing from dim_zip_prefix'
  from {{ ref('fct_orders') }} o
  where not exists (
    select 1 from {{ ref('dim_zip_prefix') }} z where z.zip_prefix = o.purchase_zip_prefix
  )

  union all

  select
    {{ finding_id("array['customer_crm', 'review_without_order', review_id || ':' || order_id]") }},
    'business_anomaly',
    'review_without_order',
    'olist_brazilian_ecommerce.order_reviews',
    review_id || ':' || order_id,
    cast(null as varchar),
    'Review references an order absent from fct_orders'
  from {{ ref('fct_reviews') }} r
  where not exists (select 1 from {{ ref('fct_orders') }} o where o.order_id = r.order_id)
)
select * from contract_violations
union all
select * from business

