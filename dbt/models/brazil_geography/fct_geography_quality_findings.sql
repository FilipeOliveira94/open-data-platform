{{ config(
    schema='brazil_geography',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('brazil_geography', 'fct_geography_quality_findings') ~ "'"
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
    'ibge_localidades.municipios',
    'olist_brazilian_ecommerce.geolocation'
  )
),
business as (
  select
    {{ finding_id("array['brazil_geography', 'zip_prefix_multi_state', zip_prefix]") }} as finding_id,
    'business_anomaly' as issue_class,
    'zip_prefix_multi_state' as reason,
    'olist_brazilian_ecommerce.geolocation' as source_dataset,
    zip_prefix as business_key,
    cast(null as varchar) as field,
    'ZIP prefix spans more than one state' as detail
  from {{ ref('dim_zip_prefix') }}
  where multi_state_flag
)
select * from contract_violations
union all
select * from business
