{{ config(
    schema='storefront_behavior',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('storefront_behavior', 'fct_storefront_events') ~ "'"
    }
) }}

select
  event_id,
  cast(event_time as timestamp) as event_time,
  event_type,
  session_id,
  user_id,
  product_id
from {{ source('silver', 'demo_storefront__clickstream') }}

