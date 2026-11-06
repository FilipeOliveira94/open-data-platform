{{ config(
    schema='customer_crm',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('customer_crm', 'fct_reviews') ~ "'"
    }
) }}

select
  review_id,
  order_id,
  cast(review_score as integer) as review_score,
  review_comment_title,
  review_comment_message,
  (review_comment_message is not null and trim(review_comment_message) <> '') as has_comment,
  cast(review_creation_date as timestamp) as review_creation_date,
  cast(review_answer_timestamp as timestamp) as review_answer_timestamp,
  date_diff(
    'second',
    cast(review_creation_date as timestamp),
    cast(review_answer_timestamp as timestamp)
  ) / 86400.0 as answer_lag_days
from {{ source('silver', 'olist_brazilian_ecommerce__order_reviews') }}

