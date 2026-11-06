{{ config(
    schema='storefront_behavior',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('storefront_behavior', 'mart_storefront_user_behavior') ~ "'"
    }
) }}

with sessions as (
  select * from {{ ref('mart_storefront_session') }}
),
events as (
  select * from {{ ref('fct_storefront_events') }}
)
select
  u.user_id,
  count(distinct s.session_id) as session_count,
  count_if(s.has_purchase) as purchase_session_count,
  count(distinct e.product_id) as products_touched,
  count_if(s.has_cart and not s.has_purchase) as cart_no_purchase_sessions,
  max(e.event_time) as last_session_time
from {{ ref('dim_storefront_users') }} u
left join sessions s on s.user_id = u.user_id
left join events e on e.user_id = u.user_id
group by 1

