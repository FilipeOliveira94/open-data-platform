{{ config(
    schema='oltp_operations',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('oltp_operations', 'dim_oltp_sellers') ~ "'"
    }
) }}

select
  seller_id,
  {{ pad_zip5('seller_zip_code_prefix') }} as zip_prefix,
  seller_city as city,
  seller_state as state_code,
  cast(updated_at as timestamp) as updated_at,
  m.municipality_id,
  m.municipality_name
from {{ source('silver', 'postgres_oltp__sellers') }} s
left join {{ ref('dim_municipality') }} m
  on m.municipality_name_norm = {{ normalize_place('s.seller_city') }}
 and m.state_code = s.seller_state

