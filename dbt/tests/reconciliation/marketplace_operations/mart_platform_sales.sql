{{ scalars_differ(
  'mart_platform_sales_orders',
  "select coalesce(sum(order_count), 0) from " ~ ref('mart_platform_sales'),
  "select count(*) from " ~ ref('fct_orders')
) }}
union all
{{ scalars_differ(
  'mart_platform_sales_delivered',
  "select coalesce(sum(delivered_order_count), 0) from " ~ ref('mart_platform_sales'),
  "select count(*) from " ~ ref('fct_orders') ~ " where order_status = 'delivered'"
) }}
union all
{{ scalars_near(
  'mart_platform_sales_gmv',
  "select coalesce(sum(item_gmv), 0) from " ~ ref('mart_platform_sales'),
  "select coalesce(sum(i.item_gmv), 0) from " ~ ref('fct_order_items') ~ " i where exists (select 1 from " ~ ref('fct_orders') ~ " o where o.order_id = i.order_id)",
  0.01
) }}
union all
{{ scalars_near(
  'mart_platform_sales_collected',
  "select coalesce(sum(amount_collected), 0) from " ~ ref('mart_platform_sales'),
  "select coalesce(sum(p.payment_value), 0) from " ~ ref('fct_payments') ~ " p where exists (select 1 from " ~ ref('fct_orders') ~ " o where o.order_id = p.order_id)",
  0.01
) }}
