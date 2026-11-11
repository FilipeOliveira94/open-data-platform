{{ scalars_differ(
  'mart_catalog_review_patterns',
  "select coalesce(sum(review_count), 0) from " ~ ref('mart_catalog_review_patterns'),
  "select count(*) from " ~ ref('fct_catalog_reviews')
) }}
