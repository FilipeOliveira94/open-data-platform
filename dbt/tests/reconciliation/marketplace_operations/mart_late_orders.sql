{{ scalars_differ(
  'mart_late_orders',
  "select count(*) from " ~ ref('mart_late_orders'),
  "select count(*) from " ~ ref('fct_orders') ~ " o where o.order_status = 'delivered' and o.order_delivered_customer_date is not null and o.order_estimated_delivery_date is not null and cast(o.order_delivered_customer_date as date) > cast(o.order_estimated_delivery_date as date) and exists (select 1 from " ~ ref('fct_order_items') ~ " i where i.order_id = o.order_id)"
) }}
