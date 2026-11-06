{{ config(
    schema='brazil_geography',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('brazil_geography', 'dim_municipality') ~ "'"
    }
) }}

select
  cast("municipio-id" as bigint) as municipality_id,
  "municipio-nome" as municipality_name,
  {{ normalize_place('"municipio-nome"') }} as municipality_name_norm,
  "uf-sigla" as state_code,
  "uf-nome" as state_name,
  "regiao-sigla" as region_code,
  "regiao-nome" as region_name,
  "microrregiao-id" as microregion_id,
  "microrregiao-nome" as microregion_name
from {{ source('silver', 'ibge_localidades__municipios') }}
