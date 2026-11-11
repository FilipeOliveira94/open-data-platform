{{ scalars_differ(
  'mart_lead_outcome_leads',
  "select count(*) from " ~ ref('mart_lead_outcome'),
  "select count(*) from " ~ ref('fct_leads')
) }}
union all
{{ scalars_differ(
  'mart_lead_outcome_wins',
  "select count(*) from " ~ ref('mart_lead_outcome') ~ " where is_won",
  "select count(*) from " ~ ref('fct_leads') ~ " l where exists (select 1 from " ~ ref('fct_closed_deals') ~ " d where d.mql_id = l.mql_id)"
) }}
union all
{{ scalars_near(
  'mart_lead_outcome_matched_gmv',
  "select coalesce(sum(matched_seller_item_gmv), 0) from " ~ ref('mart_lead_outcome') ~ " where seller_matched_in_orders",
  "select coalesce(sum(g.item_gmv), 0) from " ~ ref('fct_closed_deals') ~ " d join " ~ ref('fct_leads') ~ " l on l.mql_id = d.mql_id join " ~ ref('dim_sellers') ~ " s on s.seller_id = d.seller_id left join (select seller_id, sum(item_gmv) as item_gmv from " ~ ref('fct_order_items') ~ " group by 1) g on g.seller_id = d.seller_id",
  0.01
) }}
