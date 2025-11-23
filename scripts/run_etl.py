#!/usr/bin/env python3
"""
run_etl.py

PySpark ETL for NASDAQ historical prices + metadata.

- Reads raw OHLCV CSVs (one file per symbol)
- Extracts `symbol` from filename
- Loads metadata CSV (symbols_valid_meta.csv)
- Joins price data with metadata on `symbol`
- Normalizes column names and casts types
- Writes partitioned Parquet by `symbol`

Example:
    spark-submit run_etl.py \
        --prices-path data/raw/stocks \
        --metadata-path data/raw/symbols_valid_meta.csv \
        --output-path data/clean/prices
"""

import argparse
import sys
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Spark ETL for NASDAQ stock data + metadata")

    parser.add_argument(
        "--prices-path",
        type=str,
        default="data/raw/stocks",
        help="Directory containing raw price CSV files (one per symbol).",
    )
    parser.add_argument(
        "--metadata-path",
        type=str,
        default="data/raw/symbols_valid_meta.csv",
        help="Path to metadata CSV (symbols_valid_meta.csv).",
    )
    parser.add_argument(
        "--output-path",
        type=str,
        default="data/clean/prices",
        help="Output path for partitioned Parquet data.",
    )
    parser.add_argument(
        "--file-glob",
        type=str,
        default="*.csv",
        help="Glob pattern for price files inside prices-path (default: *.csv).",
    )
    parser.add_argument(
        "--app-name",
        type=str,
        default="StockMarketETL",
        help="Spark application name.",
    )

    return parser.parse_args()


def build_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )
    return spark


import os
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType

def load_prices(spark: SparkSession, prices_path: str, file_glob: str):
    """
    Load all CSV files from `prices_path` using a simple Python for-loop.
    - No glob patterns
    - No regex on paths
    - Symbol is derived from the filename (A.csv -> A)
    """

    # Normalize path (forward slashes not strictly required here, but harmless)
    base_dir = prices_path

    if not os.path.isdir(base_dir):
        raise RuntimeError(f"Prices path is not a directory or does not exist: {base_dir}")

    # List all CSV files in the directory
    file_names = [f for f in os.listdir(base_dir) if f.lower().endswith(".csv")]

    if not file_names:
        raise RuntimeError(f"No .csv files found in directory: {base_dir}")

    print(f"[INFO] Found {len(file_names)} CSV files in {base_dir}")

    dataframes = []

    for fname in file_names:
        full_path = os.path.join(base_dir, fname)
        # Symbol is the filename without extension, uppercased
        symbol = os.path.splitext(fname)[0].upper()

        print(f"[INFO] Reading file: {full_path} (symbol={symbol})")

        df_file = (
            spark.read
            .option("header", True)
            .csv(full_path)
        )

        # Rename columns if needed
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
            if old in df_file.columns:
                df_file = df_file.withColumnRenamed(old, new)

        # Ensure required columns
        required_cols = ["date", "open", "high", "low", "close", "adj_close", "volume"]
        missing = [c for c in required_cols if c not in df_file.columns]
        if missing:
            raise RuntimeError(
                f"File {full_path} is missing expected columns: {missing}"
            )

        # Cast types
        df_file = (
            df_file.withColumn("date", F.to_date(F.col("date")))
                   .withColumn("open", F.col("open").cast(DoubleType()))
                   .withColumn("high", F.col("high").cast(DoubleType()))
                   .withColumn("low", F.col("low").cast(DoubleType()))
                   .withColumn("close", F.col("close").cast(DoubleType()))
                   .withColumn("adj_close", F.col("adj_close").cast(DoubleType()))
                   .withColumn("volume", F.col("volume").cast(LongType()))
        )

        # Add symbol column as a literal (no regex)
        df_file = df_file.withColumn("symbol", F.lit(symbol))

        dataframes.append(df_file)

    if not dataframes:
        raise RuntimeError("No DataFrames were created from price files.")

    # Union all per-file DataFrames into one big DataFrame
    combined_df: DataFrame = reduce(
        lambda a, b: a.unionByName(b, allowMissingColumns=False),
        dataframes,
    )

    print(f"[INFO] Combined price DataFrame has {combined_df.count()} rows "
          f"and {combined_df.select('symbol').distinct().count()} distinct symbols.")

    return combined_df




def load_metadata(spark: SparkSession, metadata_path: str):
    meta = (
        spark.read
        .option("header", True)
        .csv(metadata_path)
    )

    if meta.rdd.isEmpty():
        raise RuntimeError(f"Metadata file is empty or missing: {metadata_path}")

    # Select and rename useful columns if they exist
    # Original sample columns:
    # Nasdaq Traded, Symbol, Security Name, Listing Exchange, Market Category,
    # ETF, Round Lot Size, Test Issue, Financial Status, CQS Symbol,
    # NASDAQ Symbol, NextShares
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

    # Normalize symbol to uppercase for join
    meta = meta.withColumn("symbol", F.upper(F.col("symbol")))

    # Cast a few obvious types (optional, can stay string if you want)
    if "round_lot_size" in meta.columns:
        meta = meta.withColumn("round_lot_size", F.col("round_lot_size").cast(DoubleType()))

    return meta


def enrich_and_write(prices_df, meta_df, output_path: str):
    # Left join: keep all price records even if some symbols are missing in metadata
    enriched = prices_df.join(meta_df, on="symbol", how="left")

    # Basic sanity checks
    total_rows = enriched.count()
    distinct_symbols = enriched.select("symbol").distinct().count()

    if total_rows == 0:
        raise RuntimeError("Enriched DataFrame is empty after join.")

    print(f"[INFO] Enriched rows: {total_rows}")
    print(f"[INFO] Distinct symbols: {distinct_symbols}")
    print(f"[INFO] Sample schema:")
    enriched.printSchema()

    # Write partitioned by symbol as Parquet
    (
        enriched.write
        .mode("overwrite")
        .partitionBy("symbol")
        .parquet(output_path)
    )

    print(f"[INFO] Successfully wrote Parquet to: {output_path}")


def main():
    args = parse_args()

    try:
        spark = build_spark(args.app_name)

        print(f"[INFO] Loading price data from: {args.prices_path}")
        prices_df = load_prices(spark, args.prices_path, args.file_glob)

        print(f"[INFO] Loaded price data with {prices_df.count()} rows "
              f"and {prices_df.select('symbol').distinct().count()} distinct symbols.")

        print(f"[INFO] Loading metadata from: {args.metadata_path}")
        meta_df = load_metadata(spark, args.metadata_path)

        print(f"[INFO] Loaded metadata with {meta_df.count()} rows.")

        print("[INFO] Joining price data with metadata...")
        enrich_and_write(prices_df, meta_df, args.output_path)

        spark.stop()
        sys.exit(0)

    except Exception as e:
        # Print error and exit with non-zero status
        print(f"[ERROR] ETL failed: {e}", file=sys.stderr)
        try:
            # If Spark was initialized, stop it
            spark
        except NameError:
            spark = None
        if spark is not None:
            spark.stop()
        sys.exit(1)


if __name__ == "__main__":
    main()
