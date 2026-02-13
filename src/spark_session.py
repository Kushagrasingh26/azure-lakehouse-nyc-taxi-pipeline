from pyspark.sql import SparkSession


def get_spark(app_name: str = "nyc-taxi-lakehouse") -> SparkSession:
    """
    Local Spark session configured with Delta Lake support.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")  # AQE
    )
    return builder.getOrCreate()
