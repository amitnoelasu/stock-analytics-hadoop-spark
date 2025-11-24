# etl/transform/enrich.py

from pyspark.sql import DataFrame, functions as F

from .prices_transform import add_technical_indicators


def enrich_prices_with_metadata(
    prices_df: DataFrame,
    meta_df: DataFrame,
    ma_windows=None,
    vol_window: int = 20,
) -> DataFrame:
    """
    Transform step:
    - Adds technical indicators (moving averages + volatility)
    - Joins prices with metadata on `symbol`
    - Runs simple sanity checks
    """
    if ma_windows is None:
        ma_windows = (20, 50)

    # 1) Add indicators on the prices dataframe
    print(f"[INFO] Adding technical indicators (MAs={ma_windows}, vol_window={vol_window})...")
    prices_with_indicators = add_technical_indicators(
        prices_df,
        ma_windows=ma_windows,
        vol_window=vol_window,
    )

    # 2) Join with metadata (left join to keep all price rows)
    enriched = prices_with_indicators.join(meta_df, on="symbol", how="left")

    # 3) Basic sanity checks
    total_rows = enriched.count()
    distinct_symbols = enriched.select("symbol").distinct().count()

    if total_rows == 0:
        raise RuntimeError("Enriched DataFrame is empty after join.")

    print(f"[INFO] Enriched rows (with indicators): {total_rows}")
    print(f"[INFO] Distinct symbols (after join): {distinct_symbols}")
    print("[INFO] Enriched schema:")
    enriched.printSchema()

    return enriched
