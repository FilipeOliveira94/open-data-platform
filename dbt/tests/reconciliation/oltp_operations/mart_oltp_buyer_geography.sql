{{ scalars_differ(
  'mart_oltp_buyer_geography_gmv',
  "select coalesce(sum(item_gmv), 0) from " ~ ref('mart_oltp_buyer_geography'),
  "select coalesce(sum(i.item_gmv), 0) from " ~ ref('fct_oltp_order_items') ~ " i join " ~ ref('fct_oltp_orders') ~ " o on o.order_id = i.order_id"
) }}
union all
{{ scalars_differ(
  'mart_oltp_buyer_geography_orders',
  "select coalesce(sum(order_count), 0) from " ~ ref('mart_oltp_buyer_geography'),
  "select count(*) from (select i.seller_id, i.order_id from " ~ ref('fct_oltp_order_items') ~ " i join " ~ ref('fct_oltp_orders') ~ " o on o.order_id = i.order_id group by 1, 2) pairs"
) }}
