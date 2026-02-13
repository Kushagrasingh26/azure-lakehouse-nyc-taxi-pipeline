import os
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def ingest_bronze(csv_path: str, bronze_path: str, df: DataFrame | None = None) -> None:
    """
    Ingest raw CSV -> Bronze Delta table.
    If df is provided, uses that dataframe (useful for testing).
    """
    os.makedirs(os.path.dirname(bronze_path), exist_ok=True)

    if df is None:
        # Spark read is done in run_pipeline to reuse session; this is a helper.
        raise ValueError("df is required. Read CSV with Spark in run_pipeline and pass df here.")

    # Minimal raw standardization
    bronze_df = (
        df.withColumn("ingest_ts", F.current_timestamp())
          .withColumn("source_file", F.lit(os.path.basename(csv_path)))
    )

    (bronze_df.write.format("delta")
     .mode("overwrite")
     .save(bronze_path))
