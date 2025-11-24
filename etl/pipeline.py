# etl/pipeline.py
from .config import ETLConfig
from .spark_app import build_spark
from .extract.prices_loader import load_prices
from .extract.metadata_loader import load_metadata
from .transform.enrich import enrich_prices_with_metadata
from .load.writer import write_enriched_parquet
from .sql.query_parquet import run_basic_sql_checks  # 👈 NEW


def run_etl_pipeline(config: ETLConfig) -> None:
    """
    High-level ETL orchestration.

    E = extract (prices + metadata)
    T = transform (join / indicators / checks)
    L = load (write partitioned Parquet)
    + run some SQL checks for quick validation.
    """
    spark = None
    try:
        spark = build_spark(config.app_name)

        print(f"[INFO] Extracting price data from: {config.prices_path}")
        prices_df = load_prices(spark, config.prices_path)

        print(f"[INFO] Extracting metadata from: {config.metadata_path}")
        meta_df = load_metadata(spark, config.metadata_path)

        print("[INFO] Transforming data (join + checks + indicators)...")
        # If your ETLConfig has ma_windows / vol_window, pass them here:
        enriched_df = enrich_prices_with_metadata(
            prices_df,
            meta_df,
            ma_windows=getattr(config, "ma_windows", (20, 50)),
            vol_window=getattr(config, "vol_window", 20),
        )

        # 🔹 Run SQL queries as part of the ETL run
        print("[INFO] Running SQL validation / exploration on enriched data...")
        run_basic_sql_checks(spark, enriched_df)

        print(f"[INFO] Loading data to: {config.output_path}")
        write_enriched_parquet(enriched_df, config.output_path)

    finally:
        if spark is not None:
            spark.stop()
