{{ scalars_differ(
  'oltp_contract_violation',
  "select count(*) from " ~ ref('fct_oltp_quality_findings') ~ " where issue_class = 'contract_violation'",
  "select count(*) from " ~ source('gold_staging', 'quarantine_findings') ~ " where source_id = 'postgres_oltp' and dataset in ('customers', 'orders', 'order_items', 'sellers')"
) }}
union all
{{ scalars_differ(
  'order_without_items',
  "select count(*) from " ~ ref('fct_oltp_quality_findings') ~ " where reason = 'order_without_items'",
  "select count(*) from " ~ ref('fct_oltp_orders') ~ " o where not exists (select 1 from " ~ ref('fct_oltp_order_items') ~ " i where i.order_id = o.order_id)"
) }}
union all
{{ scalars_differ(
  'delivered_without_timestamp',
  "select count(*) from " ~ ref('fct_oltp_quality_findings') ~ " where reason = 'delivered_without_timestamp'",
  "select count(*) from " ~ ref('fct_oltp_orders') ~ " where order_status = 'delivered' and order_delivered_customer_date is null"
) }}
union all
{{ scalars_differ(
  'address_without_municipality',
  "select count(*) from " ~ ref('fct_oltp_quality_findings') ~ " where reason = 'address_without_municipality'",
  "select count(*) from " ~ ref('fct_oltp_orders') ~ " where municipality_id is null"
) }}
union all
{{ scalars_differ(
  'zip_prefix_unknown',
  "select count(*) from " ~ ref('fct_oltp_quality_findings') ~ " where reason = 'zip_prefix_unknown'",
  "select count(*) from " ~ ref('fct_oltp_orders') ~ " o where not exists (select 1 from " ~ ref('dim_zip_prefix') ~ " z where z.zip_prefix = o.purchase_zip_prefix)"
) }}
