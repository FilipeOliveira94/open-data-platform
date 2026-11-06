{{ config(
    schema='storefront_behavior',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('storefront_behavior', 'mart_storefront_product_behavior') ~ "'"
    }
) }}

with events as (
  select *
  from {{ ref('fct_storefront_events') }}
  where product_id is not null
),
session_ends as (
  select
    session_id,
    last_product_id
  from {{ ref('mart_storefront_session') }}
)
select
  e.product_id,
  count_if(e.event_type = 'page_view') as view_count,
  count_if(e.event_type = 'add_to_cart') as cart_count,
  count_if(e.event_type = 'purchase') as purchase_count,
  count(distinct case when e.event_type = 'page_view' then e.session_id end) as sessions_viewed,
  count(distinct case when e.event_type = 'purchase' then e.session_id end) as sessions_purchased,
  count(distinct case when se.last_product_id = e.product_id then e.session_id end)
    as sessions_ended_on_product
from events e
left join session_ends se on se.session_id = e.session_id
group by 1
