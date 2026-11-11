-- One calendar day from 2016-09-01 through 2026-12-31. The count is the span.
with bounds as (
  select date '2016-09-01' as start_date, date '2026-12-31' as end_date
),
expected as (
  select cast(d as date) as date_day
  from bounds
  cross join unnest(sequence(start_date, end_date, interval '1' day)) as t(d)
)
select
  'dim_date' as check_name,
  cast(coalesce(e.date_day, g.date_day) as varchar) as key_value,
  case when g.date_day is null then 'missing' else 'extra' end as side
from expected e
full outer join {{ ref('dim_date') }} g
  on g.date_day = e.date_day
where e.date_day is null
   or g.date_day is null

union all

select 'dim_date_duplicate', cast(date_day as varchar), 'duplicate'
from {{ ref('dim_date') }}
group by 2
having count(*) > 1

union all

select 'dim_date_parts', cast(date_day as varchar), 'parts'
from {{ ref('dim_date') }}
where month_num not between 1 and 12
   or day_of_month not between 1 and 31
   or year_num <> year(date_day)
