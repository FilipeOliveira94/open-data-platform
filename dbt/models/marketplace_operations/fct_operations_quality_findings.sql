{{ config(
    schema='marketplace_operations',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('marketplace_operations', 'fct_operations_quality_findings') ~ "'"
    }
) }}

with contract_violations as (
  select
    {{ finding_id("array[source_dataset, reason, coalesce(field_name, ''), business_key, cast(record_index as varchar), 'contract_violation']") }} as finding_id,
    'contract_violation' as issue_class,
    reason,
    source_dataset,
    business_key,
    field_name as field,
    detail
  from {{ source('gold_staging', 'quarantine_findings') }}
  where source_id = 'olist_brazilian_ecommerce'
    and dataset in (
      'orders', 'order_items', 'order_payments', 'products', 'sellers',
      'product_category_name_translation'
    )
),
items as (
  select order_id, sum(item_gmv) as item_gmv from {{ ref('fct_order_items') }} group by 1
),
pays as (
  select order_id, sum(payment_value) as amount_collected from {{ ref('fct_payments') }} group by 1
),
business as (
  select
    {{ finding_id("array['marketplace_operations', 'order_without_items', order_id]") }} as finding_id,
    'business_anomaly' as issue_class,
    'order_without_items' as reason,
    'olist_brazilian_ecommerce.orders' as source_dataset,
    order_id as business_key,
    cast(null as varchar) as field,
    'Order has no items' as detail
  from {{ ref('fct_orders') }} o
  where not exists (select 1 from {{ ref('fct_order_items') }} i where i.order_id = o.order_id)

  union all

  select
    {{ finding_id("array['marketplace_operations', 'order_without_payment', order_id]") }},
    'business_anomaly',
    'order_without_payment',
    'olist_brazilian_ecommerce.orders',
    order_id,
    cast(null as varchar),
    'Order has no payment rows'
  from {{ ref('fct_orders') }} o
  where not exists (select 1 from {{ ref('fct_payments') }} p where p.order_id = o.order_id)

  union all

  select
    {{ finding_id("array['marketplace_operations', 'payment_amount_mismatch', o.order_id]") }},
    'business_anomaly',
    'payment_amount_mismatch',
    'olist_brazilian_ecommerce.orders',
    o.order_id,
    cast(null as varchar),
    'Item GMV and collected amount differ by more than 0.01'
  from {{ ref('fct_orders') }} o
  join items i on i.order_id = o.order_id
  join pays p on p.order_id = o.order_id
  where abs(i.item_gmv - p.amount_collected) > 0.01

  union all

  select
    {{ finding_id("array['marketplace_operations', 'delivered_without_timestamp', order_id]") }},
    'business_anomaly',
    'delivered_without_timestamp',
    'olist_brazilian_ecommerce.orders',
    order_id,
    'order_delivered_customer_date',
    'Delivered status without customer delivery timestamp'
  from {{ ref('fct_orders') }}
  where order_status = 'delivered' and order_delivered_customer_date is null

  union all

  select
    {{ finding_id("array['marketplace_operations', 'product_without_category', product_id]") }},
    'business_anomaly',
    'product_without_category',
    'olist_brazilian_ecommerce.products',
    product_id,
    'product_category_name',
    'Product has no category'
  from {{ ref('dim_products') }}
  where category_portuguese is null or trim(category_portuguese) = ''
)
select * from contract_violations
union all
select * from business

