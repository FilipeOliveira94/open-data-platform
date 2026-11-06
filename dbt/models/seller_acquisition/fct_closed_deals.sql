{{ config(
    schema='seller_acquisition',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('seller_acquisition', 'fct_closed_deals') ~ "'"
    }
) }}

select
  mql_id,
  seller_id,
  sdr_id,
  sr_id,
  cast(won_date as timestamp) as won_date,
  cast(won_date as date) as won_day,
  business_segment,
  lead_type,
  lead_behaviour_profile,
  has_company,
  has_gtin,
  average_stock,
  business_type,
  declared_product_catalog_size,
  declared_monthly_revenue
from {{ source('silver', 'olist_marketing_funnel__closed_deals') }}

