with ranked as (
  select
    c.customer_unique_id,
    c.customer_city as city,
    c.customer_state as state_code,
    c.customer_zip_code_prefix as zip_prefix,
    row_number() over (
      partition by c.customer_unique_id
      order by o.order_purchase_timestamp desc, o.order_id desc
    ) as rn
  from {{ source('silver', 'olist_brazilian_ecommerce__customers') }} c
  join {{ source('silver', 'olist_brazilian_ecommerce__orders') }} o
    on o.customer_id = c.customer_id
)
select
  'dim_customers_attribute' as check_name,
  e.customer_unique_id as key_value,
  'attribute' as side
from ranked e
join {{ ref('dim_customers') }} g
  on g.customer_unique_id = e.customer_unique_id
where e.rn = 1
  and (
    g.city is distinct from e.city
    or g.state_code is distinct from e.state_code
    or g.zip_prefix is distinct from e.zip_prefix
  )
union all
{{ keys_differ(
  'dim_customers',
  "select customer_unique_id as key_value from " ~ ref('dim_customers'),
  "select distinct c.customer_unique_id as key_value from " ~ source('silver', 'olist_brazilian_ecommerce__customers') ~ " c join " ~ source('silver', 'olist_brazilian_ecommerce__orders') ~ " o on o.customer_id = c.customer_id"
) }}
