{{ config(
    schema='seller_acquisition',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('seller_acquisition', 'mart_lead_outcome') ~ "'"
    }
) }}

with seller_gmv as (
  select seller_id, sum(item_gmv) as item_gmv
  from {{ ref('fct_order_items') }}
  group by 1
)
select
  l.mql_id,
  l.origin,
  l.first_contact_date,
  d.won_date,
  d.mql_id is not null as is_won,
  date_diff('day', cast(l.first_contact_date as date), cast(d.won_date as date)) as days_to_win,
  d.seller_id,
  s.seller_id is not null as seller_matched_in_orders,
  g.item_gmv as matched_seller_item_gmv,
  d.business_segment
from {{ ref('fct_leads') }} l
left join {{ ref('fct_closed_deals') }} d on d.mql_id = l.mql_id
left join {{ ref('dim_sellers') }} s on s.seller_id = d.seller_id
left join seller_gmv g on g.seller_id = d.seller_id

