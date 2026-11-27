{{ scalars_differ(
  'mart_olist_synth_freight_items',
  "select coalesce(sum(item_count), 0) from " ~ ref('mart_olist_synth_freight'),
  "select count(*) from " ~ ref('fct_olist_synth_order_items') ~ " i join " ~ ref('fct_olist_synth_orders') ~ " o on o.order_id = i.order_id"
) }}
