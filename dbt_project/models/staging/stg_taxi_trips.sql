{{ config(materialized='view') }}

-- Read Silver Delta outputs as parquet
-- We will export parquet using Spark by reading delta and writing parquet (step in README).
-- dbt-duckdb reads parquet super easily.

select
  vendor_id,
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  passenger_count,
  trip_distance,
  pu_zone,
  do_zone,
  rate_code_id,
  payment_type,
  fare_amount,
  tip_amount,
  total_amount,
  trip_duration_min,
  trip_date
from read_parquet('../outputs/parquet/silver_taxi_trips/*.parquet')
