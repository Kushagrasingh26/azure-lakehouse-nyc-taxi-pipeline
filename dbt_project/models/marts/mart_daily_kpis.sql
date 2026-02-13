{{ config(materialized='table') }}

select
  trip_date,
  count(*) as trip_count,
  round(avg(trip_distance), 3) as avg_trip_distance,
  round(avg(trip_duration_min), 3) as avg_trip_duration_min,
  round(sum(total_amount), 2) as total_revenue,
  round(avg(total_amount), 2) as avg_fare
from {{ ref('stg_taxi_trips') }}
group by 1
order by 1 desc
