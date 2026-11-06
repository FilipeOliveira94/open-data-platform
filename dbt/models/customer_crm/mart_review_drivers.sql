{{ config(
    schema='customer_crm',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('customer_crm', 'mart_review_drivers') ~ "'"
    }
) }}

with base as (
  select
    r.review_score,
    r.has_comment,
    r.answer_lag_days,
    case
      when o.order_delivered_customer_date is null or o.order_estimated_delivery_date is null then 'unknown'
      when cast(o.order_delivered_customer_date as date) > cast(o.order_estimated_delivery_date as date) then 'late'
      when cast(o.order_delivered_customer_date as date) < cast(o.order_estimated_delivery_date as date) then 'early'
      else 'on_time'
    end as delay_bucket,
    p.category_english
  from {{ ref('fct_reviews') }} r
  left join {{ ref('fct_orders') }} o on o.order_id = r.order_id
  left join {{ ref('fct_order_items') }} i on i.order_id = r.order_id
  left join {{ ref('dim_products') }} p on p.product_id = i.product_id
)
select
  delay_bucket,
  category_english,
  count(*) as review_count,
  avg(cast(review_score as double)) as mean_score,
  avg(case when has_comment then 1.0 else 0.0 end) as comment_share,
  approx_percentile(answer_lag_days, 0.5) as median_answer_lag_days
from base
group by 1, 2
