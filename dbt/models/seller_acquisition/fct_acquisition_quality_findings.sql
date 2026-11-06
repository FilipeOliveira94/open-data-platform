{{ config(
    schema='seller_acquisition',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('seller_acquisition', 'fct_acquisition_quality_findings') ~ "'"
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
    'olist_marketing_funnel.qualified_leads',
    'olist_marketing_funnel.closed_deals'
  )
),
business as (
  select
    {{ finding_id("array['seller_acquisition', 'won_seller_not_in_orders', mql_id]") }} as finding_id,
    'business_anomaly' as issue_class,
    'won_seller_not_in_orders' as reason,
    'olist_marketing_funnel.closed_deals' as source_dataset,
    mql_id as business_key,
    'seller_id' as field,
    'Won deal seller id not present on dim_sellers' as detail
  from {{ ref('mart_lead_outcome') }}
  where is_won and not seller_matched_in_orders

  union all

  select
    {{ finding_id("array['seller_acquisition', 'won_before_first_contact', mql_id]") }},
    'business_anomaly',
    'won_before_first_contact',
    'olist_marketing_funnel.closed_deals',
    mql_id,
    'won_date',
    'Won date is before first contact date'
  from {{ ref('mart_lead_outcome') }}
  where is_won and days_to_win is not null and days_to_win < 0

  union all

  select
    {{ finding_id("array['seller_acquisition', 'lead_without_origin', mql_id]") }},
    'business_anomaly',
    'lead_without_origin',
    'olist_marketing_funnel.qualified_leads',
    mql_id,
    'origin',
    'Lead has null or empty origin'
  from {{ ref('fct_leads') }}
  where origin is null or trim(origin) = ''

  union all

  select
    {{ finding_id("array['seller_acquisition', 'deal_without_lead', mql_id]") }},
    'business_anomaly',
    'deal_without_lead',
    'olist_marketing_funnel.closed_deals',
    mql_id,
    cast(null as varchar),
    'Closed deal has no matching qualified lead'
  from {{ ref('fct_closed_deals') }} d
  where not exists (select 1 from {{ ref('fct_leads') }} l where l.mql_id = d.mql_id)
)
select * from contract_violations
union all
select * from business

