{{ scalars_differ(
  'mart_customer_geography_buyers',
  "select coalesce(sum(buyer_count), 0) from " ~ ref('mart_customer_geography'),
  "select count(*) from (select municipality_id, municipality_name, customer_unique_id from " ~ ref('fct_orders') ~ " group by 1, 2, 3) buyers"
) }}
union all
select
  'mart_customer_geography_score' as check_name,
  cast(m.municipality_id as varchar) as left_value,
  cast(m.mean_review_score as varchar) as right_value
from {{ ref('mart_customer_geography') }} m
join (
  select
    o.municipality_id,
    avg(cast(r.review_score as double)) as mean_review_score
  from {{ ref('fct_reviews') }} r
  join {{ ref('fct_orders') }} o
    on o.order_id = r.order_id
  group by 1
) expected
  on expected.municipality_id is not distinct from m.municipality_id
where m.mean_review_score is distinct from expected.mean_review_score
