import argparse


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Spark ETL for stock price data + metadata"
    )

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
        help="Path to metadata CSV.",
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
        help="Glob pattern for price files (kept for compatibility).",
    )
    parser.add_argument(
        "--app-name",
        type=str,
        default="StockMarketETL",
        help="Spark application name.",
    )

    return parser.parse_args()
