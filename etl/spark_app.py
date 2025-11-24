from pyspark.sql import SparkSession


def build_spark(app_name: str) -> SparkSession:
    """
    Build and return a SparkSession.

    Extra Spark config can be injected via env or spark-submit flags.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )
