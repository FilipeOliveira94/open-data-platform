{{ config(
    schema='oltp_operations',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('oltp_operations', 'dim_oltp_customers') ~ "'"
    }
) }}

select
  customer_id,
  customer_unique_id,
  {{ pad_zip5('customer_zip_code_prefix') }} as zip_prefix,
  customer_city as city,
  customer_state as state_code,
  cast(updated_at as timestamp) as updated_at,
  m.municipality_id,
  m.municipality_name
from {{ source('silver', 'postgres_oltp__customers') }} c
left join {{ ref('dim_municipality') }} m
  on m.municipality_name_norm = {{ normalize_place('c.customer_city') }}
 and m.state_code = c.customer_state

