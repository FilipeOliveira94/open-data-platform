{{ config(
    schema='oltp_operations',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('oltp_operations', 'mart_oltp_freight') ~ "'"
    }
) }}

with item_geo as (
  select
    i.price,
    i.freight_value,
    o.purchase_state as customer_state,
    {{ haversine_km('cz.mean_latitude', 'cz.mean_longitude', 'sz.mean_latitude', 'sz.mean_longitude') }} as distance_km
  from {{ ref('fct_oltp_order_items') }} i
  join {{ ref('fct_oltp_orders') }} o on o.order_id = i.order_id
  left join {{ ref('dim_zip_prefix') }} cz on cz.zip_prefix = o.purchase_zip_prefix
  left join {{ ref('dim_zip_prefix') }} sz on sz.zip_prefix = i.seller_zip_prefix
)
select
  customer_state,
  {{ distance_band('distance_km') }} as distance_band,
  count(*) as item_count,
  -- Money sums stay decimal; the mean and the ratio are doubles: a Trino decimal
  -- mean keeps the input scale and a decimal quotient is rounded at six places.
  avg(cast(freight_value as double)) as avg_freight_per_item,
  cast(sum(freight_value) as double) / nullif(cast(sum(price) as double), 0) as freight_to_price_ratio
from item_geo
group by 1, 2

