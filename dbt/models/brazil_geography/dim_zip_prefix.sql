{{ config(
    schema='brazil_geography',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('brazil_geography', 'dim_zip_prefix') ~ "'"
    }
) }}

with base as (
  select
    geolocation_zip_code_prefix as zip_prefix,
    geolocation_state as state_code,
    geolocation_city as city_name,
    geolocation_lat as latitude,
    geolocation_lng as longitude
  from {{ source('silver', 'olist_brazilian_ecommerce__geolocation') }}
),
agg as (
  select
    zip_prefix,
    count(distinct state_code) as state_count,
    avg(latitude) as mean_latitude,
    avg(longitude) as mean_longitude
  from base
  group by 1
),
state_mode as (
  select zip_prefix, state_code
  from (
    select
      zip_prefix,
      state_code,
      row_number() over (
        partition by zip_prefix
        order by count(*) desc, state_code
      ) as rn
    from base
    group by 1, 2
  ) s
  where rn = 1
),
city_mode as (
  select zip_prefix, city_name
  from (
    select
      zip_prefix,
      city_name,
      row_number() over (
        partition by zip_prefix
        order by count(*) desc, city_name
      ) as rn
    from base
    group by 1, 2
  ) c
  where rn = 1
)
select
  a.zip_prefix,
  s.state_code as modal_state,
  c.city_name as modal_city,
  a.mean_latitude,
  a.mean_longitude,
  a.state_count > 1 as multi_state_flag
from agg a
join state_mode s on s.zip_prefix = a.zip_prefix
join city_mode c on c.zip_prefix = a.zip_prefix
