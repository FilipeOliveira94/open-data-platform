{{ scalars_differ(
  'mart_order_milestones_orders',
  "select coalesce(sum(order_count), 0) from " ~ ref('mart_order_milestones'),
  "select count(*) from " ~ ref('fct_orders')
) }}
union all
{{ scalars_differ(
  'mart_order_milestones_approved',
  "select coalesce(sum(approved_count), 0) from " ~ ref('mart_order_milestones'),
  "select count(*) from " ~ ref('fct_orders') ~ " where order_approved_at is not null"
) }}
union all
{{ scalars_differ(
  'mart_order_milestones_carrier',
  "select coalesce(sum(carrier_handoff_count), 0) from " ~ ref('mart_order_milestones'),
  "select count(*) from " ~ ref('fct_orders') ~ " where order_delivered_carrier_date is not null"
) }}
union all
{{ scalars_differ(
  'mart_order_milestones_delivery',
  "select coalesce(sum(customer_delivery_count), 0) from " ~ ref('mart_order_milestones'),
  "select count(*) from " ~ ref('fct_orders') ~ " where order_delivered_customer_date is not null"
) }}
union all
{{ scalars_differ(
  'mart_order_milestones_canceled_after_approval',
  "select coalesce(sum(canceled_after_approval_count), 0) from " ~ ref('mart_order_milestones'),
  "select count(*) from " ~ ref('fct_orders') ~ " where order_status = 'canceled' and order_approved_at is not null"
) }}
union all
{{ scalars_differ(
  'mart_order_milestones_unavailable',
  "select coalesce(sum(unavailable_count), 0) from " ~ ref('mart_order_milestones'),
  "select count(*) from " ~ ref('fct_orders') ~ " where order_status = 'unavailable'"
) }}
