{{ scalars_differ(
  'mart_delivery_performance_identity',
  "select coalesce(sum(on_time_count), 0) + coalesce(sum(late_count), 0) from " ~ ref('mart_delivery_performance'),
  "select coalesce(sum(delivered_with_both_dates), 0) from " ~ ref('mart_delivery_performance')
) }}
union all
{{ scalars_differ(
  'mart_delivery_performance_eligible',
  "select coalesce(sum(delivered_with_both_dates), 0) from " ~ ref('mart_delivery_performance'),
  "select count(*) from " ~ ref('fct_orders') ~ " where order_status = 'delivered' and order_delivered_customer_date is not null and order_estimated_delivery_date is not null"
) }}
union all
{{ scalars_differ(
  'mart_delivery_performance_late',
  "select coalesce(sum(late_count), 0) from " ~ ref('mart_delivery_performance'),
  "select count(*) from " ~ ref('mart_late_orders')
) }}
union all
-- Handoff eligibility: a carrier date and an item shipping limit.
{{ scalars_differ(
  'mart_delivery_performance_handoff_eligible',
  "select coalesce(sum(handoff_eligible_count), 0) from " ~ ref('mart_delivery_performance'),
  "select count(*) from " ~ ref('fct_orders') ~ " o where o.order_delivered_carrier_date is not null and exists (select 1 from " ~ ref('fct_order_items') ~ " i where i.order_id = o.order_id and i.shipping_limit_date is not null)"
) }}
