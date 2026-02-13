import os

from pyspark.sql import functions as F

from spark_session import get_spark
from bronze_ingestion import ingest_bronze
from silver_transform import build_silver, write_silver
from gold_aggregation import gold_daily_kpis, gold_zone_kpis, write_gold


def main() -> None:
    spark = get_spark()

    csv_path = "data/sample_nyc_taxi.csv"

    base = "outputs/lakehouse"
    bronze_path = f"{base}/bronze_taxi_trips"
    silver_path = f"{base}/silver_taxi_trips"
    gold_daily_path = f"{base}/gold_daily_kpis"
    gold_zone_path = f"{base}/gold_zone_kpis"

    os.makedirs("outputs", exist_ok=True)

    # Read raw
    raw_df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(csv_path)
    )

    # Bronze
    ingest_bronze(csv_path=csv_path, bronze_path=bronze_path, df=raw_df)

    bronze_df = spark.read.format("delta").load(bronze_path)

    # Silver
    silver_df = build_silver(bronze_df).cache()
    _ = silver_df.count()  # materialize cache
    write_silver(silver_df, silver_path)

    # Gold
    daily_df = gold_daily_kpis(silver_df)
    zone_df = gold_zone_kpis(silver_df)

    write_gold(daily_df, gold_daily_path)
    write_gold(zone_df, gold_zone_path)

    # Quick sanity output
    print("✅ Pipeline completed.")
    print("Gold daily sample:")
    daily_df.orderBy(F.desc("trip_date")).show(5, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
