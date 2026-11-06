{{ config(
    schema='oltp_operations',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('oltp_operations', 'fct_oltp_quality_findings') ~ "'"
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
  where source_id = 'postgres_oltp'
    and dataset in ('customers', 'orders', 'order_items', 'sellers')
),
business as (
  select
    {{ finding_id("array['oltp_operations', 'order_without_items', order_id]") }} as finding_id,
    'business_anomaly' as issue_class,
    'order_without_items' as reason,
    'postgres_oltp.orders' as source_dataset,
    order_id as business_key,
    cast(null as varchar) as field,
    'OLTP order has no items' as detail
  from {{ ref('fct_oltp_orders') }} o
  where not exists (select 1 from {{ ref('fct_oltp_order_items') }} i where i.order_id = o.order_id)

  union all

  select
    {{ finding_id("array['oltp_operations', 'delivered_without_timestamp', order_id]") }},
    'business_anomaly',
    'delivered_without_timestamp',
    'postgres_oltp.orders',
    order_id,
    'order_delivered_customer_date',
    'Delivered without customer delivery timestamp'
  from {{ ref('fct_oltp_orders') }}
  where order_status = 'delivered' and order_delivered_customer_date is null

  union all

  select
    {{ finding_id("array['oltp_operations', 'address_without_municipality', order_id]") }},
    'business_anomaly',
    'address_without_municipality',
    'postgres_oltp.orders',
    order_id,
    'purchase_city',
    'Purchase address did not match a municipality'
  from {{ ref('fct_oltp_orders') }}
  where municipality_id is null

  union all

  select
    {{ finding_id("array['oltp_operations', 'zip_prefix_unknown', order_id]") }},
    'business_anomaly',
    'zip_prefix_unknown',
    'postgres_oltp.orders',
    order_id,
    'purchase_zip_prefix',
    'Padded ZIP prefix missing from dim_zip_prefix'
  from {{ ref('fct_oltp_orders') }} o
  where not exists (
    select 1 from {{ ref('dim_zip_prefix') }} z where z.zip_prefix = o.purchase_zip_prefix
  )
)
select * from contract_violations
union all
select * from business

