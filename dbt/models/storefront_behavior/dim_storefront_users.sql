{{ config(
    schema='storefront_behavior',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('storefront_behavior', 'dim_storefront_users') ~ "'"
    }
) }}

select distinct user_id
from {{ source('silver', 'demo_storefront__clickstream') }}

