{{ scalars_differ(
  'mart_storefront_user_behavior_rows',
  "select count(*) from " ~ ref('mart_storefront_user_behavior'),
  "select count(*) from " ~ ref('dim_storefront_users')
) }}
union all
{{ scalars_differ(
  'mart_storefront_user_behavior_sessions',
  "select coalesce(sum(session_count), 0) from " ~ ref('mart_storefront_user_behavior'),
  "select count(*) from " ~ ref('mart_storefront_session')
) }}
union all
{{ scalars_differ(
  'mart_storefront_user_behavior_purchase_sessions',
  "select coalesce(sum(purchase_session_count), 0) from " ~ ref('mart_storefront_user_behavior'),
  "select count(*) from " ~ ref('mart_storefront_session') ~ " where has_purchase"
) }}
union all
{{ scalars_differ(
  'mart_storefront_user_behavior_cart_without_purchase',
  "select coalesce(sum(cart_no_purchase_sessions), 0) from " ~ ref('mart_storefront_user_behavior'),
  "select count(*) from " ~ ref('mart_storefront_session') ~ " where has_cart and not has_purchase"
) }}
