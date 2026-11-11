{{ scalars_differ(
  'mart_storefront_session_rows',
  "select count(*) from " ~ ref('mart_storefront_session'),
  "select count(distinct session_id) from " ~ ref('fct_storefront_events')
) }}
union all
{{ scalars_differ(
  'mart_storefront_session_events',
  "select coalesce(sum(event_count), 0) from " ~ ref('mart_storefront_session'),
  "select count(*) from " ~ ref('fct_storefront_events')
) }}
union all
{{ scalars_differ(
  'mart_storefront_session_cart',
  "select count(*) from " ~ ref('mart_storefront_session') ~ " where has_cart",
  "select count(distinct session_id) from " ~ ref('fct_storefront_events') ~ " where event_type = 'add_to_cart'"
) }}
union all
{{ scalars_differ(
  'mart_storefront_session_purchase',
  "select count(*) from " ~ ref('mart_storefront_session') ~ " where has_purchase",
  "select count(distinct session_id) from " ~ ref('fct_storefront_events') ~ " where event_type = 'purchase'"
) }}
