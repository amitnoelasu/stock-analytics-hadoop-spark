from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


def load_metadata(spark: SparkSession, metadata_path: str) -> DataFrame:
    """
    Extract symbol metadata from a CSV file.

    Expected original columns (subset is enough):
        Symbol, Security Name, Listing Exchange, Market Category,
        ETF, Round Lot Size, Test Issue, Financial Status,
        CQS Symbol, NASDAQ Symbol, NextShares
    """
    meta = (
        spark.read
        .option("header", True)
        .csv(metadata_path)
    )

    if meta.rdd.isEmpty():
        raise RuntimeError(f"Metadata file is empty or missing: {metadata_path}")

    col_map = {
        "Symbol": "symbol",
        "Security Name": "security_name",
        "Listing Exchange": "listing_exchange",
        "Market Category": "market_category",
        "ETF": "is_etf",
        "Round Lot Size": "round_lot_size",
        "Test Issue": "test_issue",
        "Financial Status": "financial_status",
        "CQS Symbol": "cqs_symbol",
        "NASDAQ Symbol": "nasdaq_symbol",
        "NextShares": "nextshares",
    }

    selected_cols = []
    for original, new_name in col_map.items():
        if original in meta.columns:
            selected_cols.append(F.col(original).alias(new_name))

    if "Symbol" not in meta.columns:
        raise RuntimeError("Metadata file is missing 'Symbol' column needed for join.")

    meta = meta.select(*selected_cols)

    # Normalize symbol for join
    meta = meta.withColumn("symbol", F.upper(F.col("symbol")))

    if "round_lot_size" in meta.columns:
        meta = meta.withColumn("round_lot_size", F.col("round_lot_size").cast(DoubleType()))

    return meta
