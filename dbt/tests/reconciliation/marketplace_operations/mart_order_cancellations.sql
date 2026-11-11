{{ scalars_differ(
  'mart_order_cancellations_orders',
  "select coalesce(sum(order_count), 0) from " ~ ref('mart_order_cancellations'),
  "select count(*) from " ~ ref('fct_orders') ~ " where order_status in ('canceled', 'unavailable')"
) }}
union all
{{ scalars_near(
  'mart_order_cancellations_gmv',
  "select coalesce(sum(item_gmv), 0) from " ~ ref('mart_order_cancellations'),
  "select coalesce(sum(i.item_gmv), 0) from " ~ ref('fct_order_items') ~ " i join " ~ ref('fct_orders') ~ " o on o.order_id = i.order_id where o.order_status in ('canceled', 'unavailable')",
  0.01
) }}
union all
{{ scalars_near(
  'mart_order_cancellations_collected',
  "select coalesce(sum(amount_collected), 0) from " ~ ref('mart_order_cancellations'),
  "select coalesce(sum(p.payment_value), 0) from " ~ ref('fct_payments') ~ " p join " ~ ref('fct_orders') ~ " o on o.order_id = p.order_id where o.order_status in ('canceled', 'unavailable')",
  0.01
) }}
union all
{{ scalars_differ(
  'mart_order_cancellations_approved',
  "select coalesce(sum(approved_before_cancel_count), 0) from " ~ ref('mart_order_cancellations'),
  "select count(*) from " ~ ref('fct_orders') ~ " where order_status in ('canceled', 'unavailable') and order_approved_at is not null"
) }}
