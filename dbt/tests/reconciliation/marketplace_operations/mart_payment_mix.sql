{{ scalars_differ(
  'mart_payment_mix_rows',
  "select coalesce(sum(payment_row_count), 0) from " ~ ref('mart_payment_mix'),
  "select count(*) from " ~ ref('fct_payments')
) }}
union all
{{ scalars_near(
  'mart_payment_mix_collected',
  "select coalesce(sum(amount_collected), 0) from " ~ ref('mart_payment_mix'),
  "select coalesce(sum(payment_value), 0) from " ~ ref('fct_payments'),
  0.01
) }}
