{{ config(
    schema='catalog_commerce',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('catalog_commerce', 'fct_catalog_reviews') ~ "'"
    }
) }}

select
  p.id as product_id,
  p.category,
  cast(r.ordinality as integer) as review_position,
  cast(r.rating as integer) as rating,
  length(coalesce(r.comment, '')) as comment_length,
  r."reviewerEmail" as reviewer_email,
  cast(r.date as timestamp) as review_time
from {{ source('silver', 'dummyjson_products__products') }} p
cross join unnest(p.reviews) with ordinality as r(rating, comment, date, "reviewerName", "reviewerEmail", ordinality)

