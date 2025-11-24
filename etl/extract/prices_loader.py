import os
from functools import reduce
from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType


def _normalize_price_columns(df: DataFrame) -> DataFrame:
    """
    Normalize/rename known price columns to a consistent schema.
    """
    rename_map = {
        "Date": "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adj_close",
        "Adj_Close": "adj_close",
        "Volume": "volume",
    }

    for old, new in rename_map.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)

    return df


def _cast_price_types(df: DataFrame) -> DataFrame:
    """
    Cast columns to expected types for downstream processing.
    """
    df = (
        df.withColumn("date", F.to_date(F.col("date")))
          .withColumn("open", F.col("open").cast(DoubleType()))
          .withColumn("high", F.col("high").cast(DoubleType()))
          .withColumn("low", F.col("low").cast(DoubleType()))
          .withColumn("close", F.col("close").cast(DoubleType()))
          .withColumn("adj_close", F.col("adj_close").cast(DoubleType()))
          .withColumn("volume", F.col("volume").cast(LongType()))
    )
    return df


def load_prices(
    spark: SparkSession,
    prices_path: str,
) -> DataFrame:
    """
    Extract price data from CSV files under `prices_path`.

    - Assumes one file per symbol, filename like 'AAPL.csv'
    - Symbol is derived from the filename without extension
    - Validates that required columns are present
    """

    base_dir = prices_path

    if not os.path.isdir(base_dir):
        raise RuntimeError(f"Prices path is not a directory or does not exist: {base_dir}")

    file_names: List[str] = [
        f for f in os.listdir(base_dir)
        if f.lower().endswith(".csv")
    ]

    if not file_names:
        raise RuntimeError(f"No .csv files found in directory: {base_dir}")

    print(f"[INFO] Found {len(file_names)} CSV files in {base_dir}")

    dataframes: List[DataFrame] = []

    for fname in file_names:
        full_path = os.path.join(base_dir, fname)
        symbol = os.path.splitext(fname)[0].upper()

        print(f"[INFO] Reading file: {full_path} (symbol={symbol})")

        df_file = (
            spark.read
            .option("header", True)
            .csv(full_path)
        )

        df_file = _normalize_price_columns(df_file)

        required_cols = ["date", "open", "high", "low", "close", "adj_close", "volume"]
        missing = [c for c in required_cols if c not in df_file.columns]
        if missing:
            raise RuntimeError(
                f"File {full_path} is missing expected columns: {missing}"
            )

        df_file = _cast_price_types(df_file)

        # Add symbol column as a literal derived from filename
        df_file = df_file.withColumn("symbol", F.lit(symbol))

        dataframes.append(df_file)

    if not dataframes:
        raise RuntimeError("No DataFrames were created from price files.")

    combined_df: DataFrame = reduce(
        lambda a, b: a.unionByName(b, allowMissingColumns=False),
        dataframes,
    )

    print(
        "[INFO] Combined price DataFrame has "
        f"{combined_df.count()} rows and "
        f"{combined_df.select('symbol').distinct().count()} distinct symbols."
    )

    return combined_df
