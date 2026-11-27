{{ config(
    schema='olist_synth_operations',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('olist_synth_operations', 'fct_olist_synth_quality_findings') ~ "'"
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
    detail,
    silver_action
  from {{ source('gold_staging', 'quarantine_findings') }}
  where source_id = 'olist_synth_cdc'
    and dataset in ('customers', 'orders', 'order_items', 'sellers')
),
business as (
  select
    {{ finding_id("array['olist_synth_operations', 'order_without_items', order_id]") }} as finding_id,
    'business_anomaly' as issue_class,
    'order_without_items' as reason,
    'olist_synth_cdc.orders' as source_dataset,
    order_id as business_key,
    cast(null as varchar) as field,
    'olist-synth order has no items' as detail
  from {{ ref('fct_olist_synth_orders') }} o
  where not exists (select 1 from {{ ref('fct_olist_synth_order_items') }} i where i.order_id = o.order_id)

  union all

  select
    {{ finding_id("array['olist_synth_operations', 'delivered_without_timestamp', order_id]") }},
    'business_anomaly',
    'delivered_without_timestamp',
    'olist_synth_cdc.orders',
    order_id,
    'order_delivered_customer_date',
    'Delivered without customer delivery timestamp'
  from {{ ref('fct_olist_synth_orders') }}
  where order_status = 'delivered' and order_delivered_customer_date is null

  union all

  select
    {{ finding_id("array['olist_synth_operations', 'address_without_municipality', order_id]") }},
    'business_anomaly',
    'address_without_municipality',
    'olist_synth_cdc.orders',
    order_id,
    'purchase_city',
    'Purchase address did not match a municipality'
  from {{ ref('fct_olist_synth_orders') }}
  where municipality_id is null

  union all

  select
    {{ finding_id("array['olist_synth_operations', 'zip_prefix_unknown', order_id]") }},
    'business_anomaly',
    'zip_prefix_unknown',
    'olist_synth_cdc.orders',
    order_id,
    'purchase_zip_prefix',
    'Padded ZIP prefix missing from dim_zip_prefix'
  from {{ ref('fct_olist_synth_orders') }} o
  where not exists (
    select 1 from {{ ref('dim_zip_prefix') }} z where z.zip_prefix = o.purchase_zip_prefix
  )
)
select * from contract_violations
union all
-- A business anomaly is computed from Silver; no record was quarantined.
select b.*, cast(null as varchar) as silver_action from business b

