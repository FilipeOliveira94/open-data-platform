{{ config(
    schema='marketplace_operations',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('marketplace_operations', 'mart_freight') ~ "'"
    }
) }}

with item_geo as (
  select
    i.order_id,
    i.order_item_id,
    i.price,
    i.freight_value,
    o.purchase_state as customer_state,
    o.purchase_zip_prefix as customer_zip,
    i.seller_zip_prefix as seller_zip,
    cz.mean_latitude as customer_lat,
    cz.mean_longitude as customer_lon,
    sz.mean_latitude as seller_lat,
    sz.mean_longitude as seller_lon
  from {{ ref('fct_order_items') }} i
  join {{ ref('fct_orders') }} o on o.order_id = i.order_id
  left join {{ ref('dim_zip_prefix') }} cz on cz.zip_prefix = o.purchase_zip_prefix
  left join {{ ref('dim_zip_prefix') }} sz on sz.zip_prefix = i.seller_zip_prefix
),
scored as (
  select
    customer_state,
    {{ haversine_km('customer_lat', 'customer_lon', 'seller_lat', 'seller_lon') }} as distance_km,
    price,
    freight_value
  from item_geo
)
select
  customer_state,
  {{ distance_band('distance_km') }} as distance_band,
  count(*) as item_count,
  avg(freight_value) as avg_freight_per_item,
  sum(freight_value) / nullif(sum(price), 0) as freight_to_price_ratio
from scored
group by 1, 2

