{{ scalars_differ(
  'mart_oltp_delivery_performance_eligible',
  "select coalesce(sum(delivered_with_both_dates), 0) from " ~ ref('mart_oltp_delivery_performance'),
  "select count(*) from " ~ ref('fct_oltp_orders') ~ " where order_status = 'delivered' and order_delivered_customer_date is not null and order_estimated_delivery_date is not null"
) }}
union all
select
  'mart_oltp_delivery_performance_identity' as check_name,
  cast(purchase_month as varchar) as left_value,
  cast(on_time_rate + late_rate as varchar) as right_value
from {{ ref('mart_oltp_delivery_performance') }}
where delivered_with_both_dates > 0
  and abs(on_time_rate + late_rate - 1) > 1e-9
union all
{{ scalars_differ(
  'mart_oltp_delivery_performance_late',
  "select count(*) from " ~ ref('mart_oltp_late_orders'),
  "select count(*) from " ~ ref('fct_oltp_orders') ~ " o where o.order_status = 'delivered' and o.order_delivered_customer_date is not null and o.order_estimated_delivery_date is not null and cast(o.order_delivered_customer_date as date) > cast(o.order_estimated_delivery_date as date) and exists (select 1 from " ~ ref('fct_oltp_order_items') ~ " i where i.order_id = o.order_id)"
) }}
