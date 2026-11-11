{{ scalars_differ(
  'mart_oltp_platform_sales_orders',
  "select coalesce(sum(order_count), 0) from " ~ ref('mart_oltp_platform_sales'),
  "select count(*) from " ~ ref('fct_oltp_orders')
) }}
union all
{{ scalars_differ(
  'mart_oltp_platform_sales_delivered',
  "select coalesce(sum(delivered_order_count), 0) from " ~ ref('mart_oltp_platform_sales'),
  "select count(*) from " ~ ref('fct_oltp_orders') ~ " where order_status = 'delivered'"
) }}
union all
{{ scalars_differ(
  'mart_oltp_platform_sales_gmv',
  "select coalesce(sum(item_gmv), 0) from " ~ ref('mart_oltp_platform_sales'),
  "select coalesce(sum(i.item_gmv), 0) from " ~ ref('fct_oltp_order_items') ~ " i where exists (select 1 from " ~ ref('fct_oltp_orders') ~ " o where o.order_id = i.order_id)"
) }}
