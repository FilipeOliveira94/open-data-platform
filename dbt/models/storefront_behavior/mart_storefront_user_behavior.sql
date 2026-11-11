{{ config(
    schema='storefront_behavior',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('storefront_behavior', 'mart_storefront_user_behavior') ~ "'"
    }
) }}

with session_agg as (
  select
    user_id,
    count(*) as session_count,
    count_if(has_purchase) as purchase_session_count,
    count_if(has_cart and not has_purchase) as cart_no_purchase_sessions
  from {{ ref('mart_storefront_session') }}
  group by 1
),
event_agg as (
  select
    user_id,
    count(distinct product_id) as products_touched,
    max(event_time) as last_session_time
  from {{ ref('fct_storefront_events') }}
  group by 1
)
select
  u.user_id,
  coalesce(s.session_count, 0) as session_count,
  coalesce(s.purchase_session_count, 0) as purchase_session_count,
  coalesce(e.products_touched, 0) as products_touched,
  coalesce(s.cart_no_purchase_sessions, 0) as cart_no_purchase_sessions,
  e.last_session_time
from {{ ref('dim_storefront_users') }} u
left join session_agg s on s.user_id = u.user_id
left join event_agg e on e.user_id = u.user_id

