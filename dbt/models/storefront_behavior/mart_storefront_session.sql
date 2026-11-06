{{ config(
    schema='storefront_behavior',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('storefront_behavior', 'mart_storefront_session') ~ "'"
    }
) }}

with ordered as (
  select
    *,
    row_number() over (
      partition by session_id
      order by event_time, event_id
    ) as event_seq,
    row_number() over (
      partition by session_id
      order by event_time desc, event_id desc
    ) as event_seq_desc,
    min(case when event_type = 'purchase' then event_time end) over (partition by session_id) as first_purchase_time
  from {{ ref('fct_storefront_events') }}
),
agg as (
  select
    session_id,
    min(user_id) as user_id,
    count(*) as event_count,
    count_if(event_type = 'add_to_cart') > 0 as has_cart,
    count_if(event_type = 'purchase') > 0 as has_purchase,
    count_if(
      event_type = 'add_to_cart'
      and (first_purchase_time is null or event_time < first_purchase_time)
    ) as cart_events_before_first_purchase,
    max(case when event_seq_desc = 1 then event_type end) as last_event_type,
    max(case when event_seq_desc = 1 then product_id end) as last_product_id
  from ordered
  group by 1
)
select * from agg

