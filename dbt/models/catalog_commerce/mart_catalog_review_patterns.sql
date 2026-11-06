{{ config(
    schema='catalog_commerce',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('catalog_commerce', 'mart_catalog_review_patterns') ~ "'"
    }
) }}

with email_counts as (
  select
    product_id,
    reviewer_email,
    count(*) as email_count
  from {{ ref('fct_catalog_reviews') }}
  group by 1, 2
),
flagged as (
  select
    r.*,
    case when e.email_count > 1 then 1.0 else 0.0 end as repeated_email_flag
  from {{ ref('fct_catalog_reviews') }} r
  join email_counts e
    on e.product_id = r.product_id
   and e.reviewer_email = r.reviewer_email
)
select
  category,
  rating,
  count(*) as review_count,
  avg(cast(comment_length as double)) as mean_comment_length,
  avg(repeated_email_flag) as repeated_reviewer_email_share
from flagged
group by 1, 2

