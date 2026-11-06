{{ config(
    schema='gold_shared',
    properties={
      "format": "'PARQUET'",
      "location": "'" ~ gold_location('gold_shared', 'dim_date') ~ "'"
    }
) }}

with bounds as (
  select
    date '2016-09-01' as start_date,
    date '2026-12-31' as end_date
),
spine as (
  select sequence(start_date, end_date, interval '1' day) as days
  from bounds
)
select
  cast(d as date) as date_day,
  year(d) as year_num,
  month(d) as month_num,
  day(d) as day_of_month,
  day_of_week(d) as day_of_week,
  date_trunc('week', d) as week_start,
  date_trunc('month', d) as month_start,
  date_trunc('quarter', d) as quarter_start,
  date_trunc('year', d) as year_start,
  format_datetime(d, 'yyyyMM') as year_month,
  cast(year(d) as varchar) || 'Q' || cast(quarter(d) as varchar) as year_quarter
from spine
cross join unnest(days) as t(d)
