{{ config(
    schema='seller_acquisition',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('seller_acquisition', 'mart_marketing_acquisition') ~ "'"
    }
) }}

with outcomes as (
  select * from {{ ref('mart_lead_outcome') }}
),
segment as (
  select origin, business_segment
  from (
    select
      origin,
      business_segment,
      row_number() over (
        partition by origin
        order by count(*) desc, business_segment
      ) as rn
    from outcomes
    where is_won and business_segment is not null
    group by 1, 2
  ) x
  where rn = 1
)
select
  o.origin,
  count(*) as lead_count,
  count_if(o.is_won) as win_count,
  count_if(o.is_won) * 1.0 / nullif(count(*), 0) as win_rate,
  approx_percentile(o.days_to_win, 0.5) as median_days_to_win,
  sum(case when o.seller_matched_in_orders then o.matched_seller_item_gmv else 0 end) as matched_seller_item_gmv,
  s.business_segment as top_declared_segment
from outcomes o
left join segment s on s.origin = o.origin
group by 1, 7

