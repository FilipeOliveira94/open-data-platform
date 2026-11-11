{{ scalars_differ(
  'operations_contract_violation',
  "select count(*) from " ~ ref('fct_operations_quality_findings') ~ " where issue_class = 'contract_violation'",
  "select count(*) from " ~ source('gold_staging', 'quarantine_findings') ~ " where source_id = 'olist_brazilian_ecommerce' and dataset in ('orders', 'order_items', 'order_payments', 'products', 'sellers', 'product_category_name_translation')"
) }}
union all
{{ scalars_differ(
  'order_without_items',
  "select count(*) from " ~ ref('fct_operations_quality_findings') ~ " where reason = 'order_without_items'",
  "select count(*) from " ~ ref('fct_orders') ~ " o where not exists (select 1 from " ~ ref('fct_order_items') ~ " i where i.order_id = o.order_id)"
) }}
union all
{{ scalars_differ(
  'order_without_payment',
  "select count(*) from " ~ ref('fct_operations_quality_findings') ~ " where reason = 'order_without_payment'",
  "select count(*) from " ~ ref('fct_orders') ~ " o where not exists (select 1 from " ~ ref('fct_payments') ~ " p where p.order_id = o.order_id)"
) }}
union all
{{ scalars_differ(
  'payment_amount_mismatch',
  "select count(*) from " ~ ref('fct_operations_quality_findings') ~ " where reason = 'payment_amount_mismatch'",
  "select count(*) from (select o.order_id from " ~ ref('fct_orders') ~ " o join (select order_id, sum(item_gmv) as item_gmv from " ~ ref('fct_order_items') ~ " group by 1) i on i.order_id = o.order_id join (select order_id, sum(payment_value) as amount_collected from " ~ ref('fct_payments') ~ " group by 1) p on p.order_id = o.order_id where abs(i.item_gmv - p.amount_collected) > 0.01) mismatched"
) }}
union all
{{ scalars_differ(
  'delivered_without_timestamp',
  "select count(*) from " ~ ref('fct_operations_quality_findings') ~ " where reason = 'delivered_without_timestamp'",
  "select count(*) from " ~ ref('fct_orders') ~ " where order_status = 'delivered' and order_delivered_customer_date is null"
) }}
union all
{{ scalars_differ(
  'product_without_category',
  "select count(*) from " ~ ref('fct_operations_quality_findings') ~ " where reason = 'product_without_category'",
  "select count(*) from " ~ ref('dim_products') ~ " where category_portuguese is null or trim(category_portuguese) = ''"
) }}
