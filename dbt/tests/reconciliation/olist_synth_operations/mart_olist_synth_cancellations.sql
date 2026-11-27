{{ scalars_differ(
  'mart_olist_synth_cancellations_orders',
  "select coalesce(sum(order_count), 0) from " ~ ref('mart_olist_synth_cancellations'),
  "select count(*) from " ~ ref('fct_olist_synth_orders') ~ " where order_status in ('canceled', 'unavailable')"
) }}
union all
{{ scalars_differ(
  'mart_olist_synth_cancellations_approved',
  "select coalesce(sum(approved_before_cancel_count), 0) from " ~ ref('mart_olist_synth_cancellations'),
  "select count(*) from " ~ ref('fct_olist_synth_orders') ~ " where order_status in ('canceled', 'unavailable') and order_approved_at is not null"
) }}
