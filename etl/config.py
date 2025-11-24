# etl/config.py
from dataclasses import dataclass, field
from typing import Sequence, List
import argparse


@dataclass
class ETLConfig:
    prices_path: str
    metadata_path: str
    output_path: str
    app_name: str = "StockMarketETL"

    # NEW: indicator config
    ma_windows: Sequence[int] = field(default_factory=lambda: (20, 50))
    vol_window: int = 20

    @classmethod
    def from_cli(cls) -> "ETLConfig":
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
            "--app-name",
            type=str,
            default="StockMarketETL",
            help="Spark application name.",
        )
        # NEW CLI options (optional to override defaults)
        parser.add_argument(
            "--ma-windows",
            type=str,
            default="20,50",
            help="Comma-separated moving-average windows in days, e.g. '20,50'.",
        )
        parser.add_argument(
            "--vol-window",
            type=int,
            default=20,
            help="Window size in days for rolling volatility.",
        )

        args = parser.parse_args()

        ma_windows: List[int] = [
            int(x.strip()) for x in args.ma_windows.split(",") if x.strip()
        ]

        return cls(
            prices_path=args.prices_path,
            metadata_path=args.metadata_path,
            output_path=args.output_path,
            app_name=args.app_name,
            ma_windows=ma_windows,
            vol_window=args.vol_window,
        )
