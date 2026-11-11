{{ keys_differ(
  'dim_zip_prefix',
  "select zip_prefix as key_value from " ~ ref('dim_zip_prefix'),
  "select distinct geolocation_zip_code_prefix as key_value from " ~ source('silver', 'olist_brazilian_ecommerce__geolocation')
) }}
union all
{{ scalars_differ(
  'dim_zip_prefix_multi_state',
  "select count(*) from " ~ ref('dim_zip_prefix') ~ " where multi_state_flag",
  "select count(*) from (select geolocation_zip_code_prefix from " ~ source('silver', 'olist_brazilian_ecommerce__geolocation') ~ " group by 1 having count(distinct geolocation_state) > 1) prefixes"
) }}
