{{ scalars_differ(
  'acquisition_contract_violation',
  "select count(*) from " ~ ref('fct_acquisition_quality_findings') ~ " where issue_class = 'contract_violation'",
  "select count(*) from " ~ source('gold_staging', 'quarantine_findings') ~ " where source_dataset in ('olist_marketing_funnel.qualified_leads', 'olist_marketing_funnel.closed_deals')"
) }}
union all
{{ scalars_differ(
  'won_seller_not_in_orders',
  "select count(*) from " ~ ref('fct_acquisition_quality_findings') ~ " where reason = 'won_seller_not_in_orders'",
  "select count(*) from " ~ ref('mart_lead_outcome') ~ " where is_won and not seller_matched_in_orders"
) }}
union all
{{ scalars_differ(
  'won_before_first_contact',
  "select count(*) from " ~ ref('fct_acquisition_quality_findings') ~ " where reason = 'won_before_first_contact'",
  "select count(*) from " ~ ref('mart_lead_outcome') ~ " where is_won and days_to_win is not null and days_to_win < 0"
) }}
union all
{{ scalars_differ(
  'lead_without_origin',
  "select count(*) from " ~ ref('fct_acquisition_quality_findings') ~ " where reason = 'lead_without_origin'",
  "select count(*) from " ~ ref('fct_leads') ~ " where origin is null or trim(origin) = ''"
) }}
union all
{{ scalars_differ(
  'deal_without_lead',
  "select count(*) from " ~ ref('fct_acquisition_quality_findings') ~ " where reason = 'deal_without_lead'",
  "select count(*) from " ~ ref('fct_closed_deals') ~ " d where not exists (select 1 from " ~ ref('fct_leads') ~ " l where l.mql_id = d.mql_id)"
) }}
