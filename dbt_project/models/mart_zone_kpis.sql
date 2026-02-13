{{ config(materialized='table') }}

select
  trip_date,
  pu_zone,
  count(*) as trip_count,
  round(sum(total_amount), 2) as total_revenue,
  round(avg(tip_amount), 2) as avg_tip
from {{ ref('stg_taxi_trips') }}
group by 1, 2
order by 1 desc, 3 desc
