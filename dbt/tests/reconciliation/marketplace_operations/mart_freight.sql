{{ scalars_differ(
  'mart_freight_items',
  "select coalesce(sum(item_count), 0) from " ~ ref('mart_freight'),
  "select count(*) from " ~ ref('fct_order_items') ~ " i join " ~ ref('fct_orders') ~ " o on o.order_id = i.order_id"
) }}
