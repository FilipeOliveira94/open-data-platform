{{ scalars_differ(
  'fct_catalog_reviews',
  "select count(*) from " ~ ref('fct_catalog_reviews'),
  "select coalesce(sum(cardinality(reviews)), 0) from " ~ source('silver', 'dummyjson_products__products')
) }}
