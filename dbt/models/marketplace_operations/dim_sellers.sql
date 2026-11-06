{{ config(
    schema='marketplace_operations',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('marketplace_operations', 'dim_sellers') ~ "'"
    }
) }}

select
  s.seller_id,
  s.seller_zip_code_prefix as zip_prefix,
  s.seller_city as city,
  s.seller_state as state_code,
  m.municipality_id,
  m.municipality_name
from {{ source('silver', 'olist_brazilian_ecommerce__sellers') }} s
left join {{ ref('dim_municipality') }} m
  on m.municipality_name_norm = {{ normalize_place('s.seller_city') }}
 and m.state_code = s.seller_state

