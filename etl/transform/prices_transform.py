# transformations/prices_transform.py

from typing import Iterable, Tuple

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def add_technical_indicators(
    df: DataFrame,
    ma_windows: Iterable[int] = (20, 50),
    vol_window: int = 20,
) -> DataFrame:
    """
    Add rolling moving averages and volatility columns.

    - 1-day return: (close / lag(close) - 1)
    - Rolling volatility: stddev of 1-day returns over `vol_window` rows
    - Moving averages: rolling mean of `close` over each window in `ma_windows`

    All calculations are done per symbol, ordered by date.
    """

    # Ensure we have the required columns
    required_cols = {"symbol", "date", "close"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"DataFrame is missing required columns for indicators: {missing}")

    # Window for lag / returns
    w_lag = Window.partitionBy("symbol").orderBy("date")

    # 1-day return
    df = df.withColumn("close_lag1", F.lag("close").over(w_lag))
    df = df.withColumn(
        "return_1d",
        F.when(F.col("close_lag1").isNotNull(),
               F.col("close") / F.col("close_lag1") - F.lit(1.0))
         .otherwise(F.lit(None))
    )

    # Rolling volatility over `vol_window` rows (per symbol)
    w_vol = (
        Window.partitionBy("symbol")
        .orderBy("date")
        .rowsBetween(-vol_window + 1, 0)
    )

    df = df.withColumn(
        f"vol_{vol_window}d",
        F.stddev_pop("return_1d").over(w_vol)
    )

    # Moving averages over each window in ma_windows
    for win in ma_windows:
        w_ma = (
            Window.partitionBy("symbol")
            .orderBy("date")
            .rowsBetween(-win + 1, 0)
        )
        df = df.withColumn(
            f"ma_{win}",
            F.avg("close").over(w_ma)
        )

    # We don't need the helper lag column anymore
    df = df.drop("close_lag1")

    return df


def transform_prices(
    prices_df: DataFrame,
    ma_windows: Iterable[int] = (20, 50),
    vol_window: int = 20,
) -> DataFrame:
    """
    Facade for all price-related transformations.
    Currently:
      - assumes prices_df is already normalized & typed (done in extract)
      - adds technical indicators (MAs & volatility)
    """
    enriched = add_technical_indicators(
        prices_df,
        ma_windows=ma_windows,
        vol_window=vol_window,
    )
    return enriched
