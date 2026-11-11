{{ scalars_differ(
  'mart_marketing_acquisition_leads',
  "select coalesce(sum(lead_count), 0) from " ~ ref('mart_marketing_acquisition'),
  "select count(*) from " ~ ref('fct_leads')
) }}
union all
{{ scalars_differ(
  'mart_marketing_acquisition_wins',
  "select coalesce(sum(win_count), 0) from " ~ ref('mart_marketing_acquisition'),
  "select count(*) from " ~ ref('mart_lead_outcome') ~ " where is_won"
) }}
