from pyspark.sql import DataFrame


def write_enriched_parquet(enriched: DataFrame, output_path: str) -> None:
    """
    Load step: write the enriched DataFrame as Parquet partitioned by symbol.
    """
    (
        enriched.write
        .mode("overwrite")
        .partitionBy("symbol")
        .parquet(output_path)
    )

    print(f"[INFO] Successfully wrote Parquet to: {output_path}")
