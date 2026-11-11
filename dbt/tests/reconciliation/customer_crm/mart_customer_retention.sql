{{ scalars_differ(
  'mart_customer_retention_people',
  "select count(*) from " ~ ref('mart_customer_retention'),
  "select count(*) from (select customer_unique_id from " ~ ref('fct_orders') ~ " where order_status = 'delivered' group by 1) people"
) }}
union all
{{ scalars_differ(
  'mart_customer_retention_orders',
  "select coalesce(sum(delivered_order_count), 0) from " ~ ref('mart_customer_retention'),
  "select count(*) from " ~ ref('fct_orders') ~ " where order_status = 'delivered'"
) }}
