{{ config(
    schema='marketplace_operations',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('marketplace_operations', 'dim_products') ~ "'"
    }
) }}

with translation as (
  select
    product_category_name,
    product_category_name_english,
    row_number() over (
      partition by product_category_name
      order by _ingestion_ts desc, execution_id desc, raw_object_key desc, cast(record_index as bigint) desc
    ) as rn
  from {{ source('silver', 'olist_brazilian_ecommerce__product_category_name_translation') }}
)
select
  p.product_id,
  p.product_category_name as category_portuguese,
  {{ category_english('p.product_category_name', 't.product_category_name_english') }} as category_english,
  p.product_name_lenght as product_name_length,
  p.product_description_lenght as product_description_length,
  p.product_photos_qty,
  p.product_weight_g,
  p.product_length_cm,
  p.product_height_cm,
  p.product_width_cm
from {{ source('silver', 'olist_brazilian_ecommerce__products') }} p
left join translation t
  on t.product_category_name = p.product_category_name
 and t.rn = 1

