-- A review is counted once per item so the English category can split.
-- Orders with no items still keep the review.
{{ scalars_differ(
  'mart_review_drivers_rows',
  "select coalesce(sum(review_count), 0) from " ~ ref('mart_review_drivers'),
  "select count(*) from (select r.review_id, r.order_id, i.order_item_id from " ~ ref('fct_reviews') ~ " r left join " ~ ref('fct_order_items') ~ " i on i.order_id = r.order_id) exploded"
) }}
