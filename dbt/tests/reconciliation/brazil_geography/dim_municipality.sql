{{ keys_differ(
  'dim_municipality',
  "select municipality_id as key_value from " ~ ref('dim_municipality'),
  "select cast(\"municipio-id\" as bigint) as key_value from " ~ source('silver', 'ibge_localidades__municipios')
) }}
