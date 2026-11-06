{{ config(
    schema='customer_crm',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('customer_crm', 'mart_customer_geography') ~ "'"
    }
) }}

with buyers as (
  select
    o.municipality_id,
    o.municipality_name,
    o.customer_unique_id,
    o.order_id
  from {{ ref('fct_orders') }} o
),
retention as (
  select customer_unique_id, is_repeat_buyer
  from {{ ref('mart_customer_retention') }}
),
scores as (
  select
    o.municipality_id,
    avg(cast(r.review_score as double)) as mean_review_score
  from {{ ref('fct_reviews') }} r
  join {{ ref('fct_orders') }} o on o.order_id = r.order_id
  group by 1
)
select
  b.municipality_id,
  b.municipality_name,
  count(distinct b.customer_unique_id) as buyer_count,
  count(distinct case when ret.is_repeat_buyer then b.customer_unique_id end) as repeat_buyer_count,
  s.mean_review_score
from buyers b
left join retention ret on ret.customer_unique_id = b.customer_unique_id
left join scores s on s.municipality_id is not distinct from b.municipality_id
group by 1, 2, 5

