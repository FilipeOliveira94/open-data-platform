{{ config(
    schema='seller_acquisition',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('seller_acquisition', 'fct_leads') ~ "'"
    }
) }}

select
  mql_id,
  cast(first_contact_date as timestamp) as first_contact_date,
  cast(first_contact_date as date) as first_contact_day,
  origin,
  landing_page_id
from {{ source('silver', 'olist_marketing_funnel__qualified_leads') }}

