-- A review counts once per (review_id, order_id). The category is resolved
-- per order (`mixed` when the items span categories), so items do not fan out.
{{ scalars_differ(
  'mart_review_drivers_rows',
  "select coalesce(sum(review_count), 0) from " ~ ref('mart_review_drivers'),
  "select count(*) from (select distinct review_id, order_id from " ~ ref('fct_reviews') ~ ") pairs"
) }}
