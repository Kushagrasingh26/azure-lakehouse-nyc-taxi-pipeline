from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_silver(bronze_df: DataFrame) -> DataFrame:
    """
    Clean + standardize types + basic validity filters.
    """
    df = (
        bronze_df
        .withColumn("tpep_pickup_datetime", F.to_timestamp("tpep_pickup_datetime"))
        .withColumn("tpep_dropoff_datetime", F.to_timestamp("tpep_dropoff_datetime"))
        .withColumn("trip_duration_min",
                    (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60.0)
        .filter(F.col("trip_distance") > 0)
        .filter(F.col("trip_duration_min") > 0)
        .filter(F.col("total_amount") >= 0)
    )

    # A simple partition key
    df = df.withColumn("trip_date", F.to_date("tpep_pickup_datetime"))

    return df


def write_silver(silver_df: DataFrame, silver_path: str) -> None:
    (silver_df.write.format("delta")
     .mode("overwrite")
     .partitionBy("trip_date")
     .save(silver_path))
