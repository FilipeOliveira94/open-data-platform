{{ scalars_differ(
  'geography_contract_violation',
  "select count(*) from " ~ ref('fct_geography_quality_findings') ~ " where issue_class = 'contract_violation'",
  "select count(*) from " ~ source('gold_staging', 'quarantine_findings') ~ " where source_dataset in ('ibge_localidades.municipios', 'olist_brazilian_ecommerce.geolocation')"
) }}
union all
{{ scalars_differ(
  'zip_prefix_multi_state',
  "select count(*) from " ~ ref('fct_geography_quality_findings') ~ " where reason = 'zip_prefix_multi_state'",
  "select count(*) from " ~ ref('dim_zip_prefix') ~ " where multi_state_flag"
) }}
