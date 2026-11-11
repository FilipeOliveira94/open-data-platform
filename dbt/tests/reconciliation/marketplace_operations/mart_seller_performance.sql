{{ scalars_near(
  'mart_seller_performance_gmv',
  "select coalesce(sum(item_gmv), 0) from " ~ ref('mart_seller_performance'),
  "select coalesce(sum(i.item_gmv), 0) from " ~ ref('fct_order_items') ~ " i join " ~ ref('fct_orders') ~ " o on o.order_id = i.order_id",
  0.01
) }}
union all
{{ scalars_differ(
  'mart_seller_performance_orders',
  "select coalesce(sum(order_count), 0) from " ~ ref('mart_seller_performance'),
  "select count(*) from (select i.seller_id, i.order_id from " ~ ref('fct_order_items') ~ " i join " ~ ref('fct_orders') ~ " o on o.order_id = i.order_id group by 1, 2) pairs"
) }}
