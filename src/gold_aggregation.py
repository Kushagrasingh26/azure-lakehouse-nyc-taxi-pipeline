from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def gold_daily_kpis(silver_df: DataFrame) -> DataFrame:
    return (
        silver_df.groupBy("trip_date")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.avg("trip_distance"), 3).alias("avg_trip_distance"),
            F.round(F.avg("trip_duration_min"), 3).alias("avg_trip_duration_min"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("total_amount"), 2).alias("avg_fare"),
        )
    )


def gold_zone_kpis(silver_df: DataFrame) -> DataFrame:
    return (
        silver_df.groupBy("trip_date", "pu_zone")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
        )
    )


def write_gold(df: DataFrame, out_path: str) -> None:
    (df.write.format("delta")
     .mode("overwrite")
     .save(out_path))
