{{ scalars_differ(
  'mart_oltp_freight_items',
  "select coalesce(sum(item_count), 0) from " ~ ref('mart_oltp_freight'),
  "select count(*) from " ~ ref('fct_oltp_order_items') ~ " i join " ~ ref('fct_oltp_orders') ~ " o on o.order_id = i.order_id"
) }}
