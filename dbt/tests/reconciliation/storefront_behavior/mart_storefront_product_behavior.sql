{{ scalars_differ(
  'mart_storefront_product_behavior_rows',
  "select count(*) from " ~ ref('mart_storefront_product_behavior'),
  "select count(*) from " ~ ref('dim_storefront_products')
) }}
union all
{{ scalars_differ(
  'mart_storefront_product_behavior_views',
  "select coalesce(sum(view_count), 0) from " ~ ref('mart_storefront_product_behavior'),
  "select count(*) from " ~ ref('fct_storefront_events') ~ " where event_type = 'page_view' and product_id is not null"
) }}
union all
{{ scalars_differ(
  'mart_storefront_product_behavior_carts',
  "select coalesce(sum(cart_count), 0) from " ~ ref('mart_storefront_product_behavior'),
  "select count(*) from " ~ ref('fct_storefront_events') ~ " where event_type = 'add_to_cart' and product_id is not null"
) }}
union all
{{ scalars_differ(
  'mart_storefront_product_behavior_purchases',
  "select coalesce(sum(purchase_count), 0) from " ~ ref('mart_storefront_product_behavior'),
  "select count(*) from " ~ ref('fct_storefront_events') ~ " where event_type = 'purchase' and product_id is not null"
) }}
