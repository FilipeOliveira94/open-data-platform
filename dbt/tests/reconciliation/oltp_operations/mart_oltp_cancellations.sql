{{ scalars_differ(
  'mart_oltp_cancellations_orders',
  "select coalesce(sum(order_count), 0) from " ~ ref('mart_oltp_cancellations'),
  "select count(*) from " ~ ref('fct_oltp_orders') ~ " where order_status in ('canceled', 'unavailable')"
) }}
union all
{{ scalars_differ(
  'mart_oltp_cancellations_approved',
  "select coalesce(sum(approved_before_cancel_count), 0) from " ~ ref('mart_oltp_cancellations'),
  "select count(*) from " ~ ref('fct_oltp_orders') ~ " where order_status in ('canceled', 'unavailable') and order_approved_at is not null"
) }}
