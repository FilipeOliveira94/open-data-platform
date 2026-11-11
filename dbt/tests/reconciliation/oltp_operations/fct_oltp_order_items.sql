{{ keys_differ(
  'fct_oltp_order_items',
  "select order_id || ':' || cast(order_item_id as varchar) as key_value from " ~ ref('fct_oltp_order_items'),
  "select order_id || ':' || cast(order_item_id as varchar) as key_value from " ~ source('silver', 'postgres_oltp__order_items')
) }}
union all
{{ scalars_differ(
  'fct_oltp_order_items_gmv',
  "select coalesce(sum(item_gmv), 0) from " ~ ref('fct_oltp_order_items'),
  "select coalesce(sum(price + freight_value), 0) from " ~ source('silver', 'postgres_oltp__order_items')
) }}
