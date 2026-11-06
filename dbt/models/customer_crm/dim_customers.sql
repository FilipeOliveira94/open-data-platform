{{ config(
    schema='customer_crm',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('customer_crm', 'dim_customers') ~ "'"
    }
) }}

with ranked as (
  select
    c.customer_unique_id,
    c.customer_city as city,
    c.customer_state as state_code,
    c.customer_zip_code_prefix as zip_prefix,
    o.order_id,
    o.order_purchase_timestamp,
    row_number() over (
      partition by c.customer_unique_id
      order by o.order_purchase_timestamp desc, o.order_id desc
    ) as rn
  from {{ source('silver', 'olist_brazilian_ecommerce__customers') }} c
  join {{ source('silver', 'olist_brazilian_ecommerce__orders') }} o
    on o.customer_id = c.customer_id
)
select
  r.customer_unique_id,
  r.city,
  r.state_code,
  r.zip_prefix,
  m.municipality_id,
  m.municipality_name
from ranked r
left join {{ ref('dim_municipality') }} m
  on m.municipality_name_norm = {{ normalize_place('r.city') }}
 and m.state_code = r.state_code
where r.rn = 1
