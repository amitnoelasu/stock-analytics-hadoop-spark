# etl/analysis/sql_examples.py

from pyspark.sql import SparkSession, DataFrame


def run_basic_sql_checks(spark: SparkSession, enriched_df: DataFrame) -> None:
    """
    Register the enriched DataFrame as a temp view and run some example SQL queries.

    This is purely for validation / exploration. You can customize or remove as needed.
    """
    # Register temp view
    enriched_df.createOrReplaceTempView("prices_enriched")

    print("\n[SQL] Sample 5 rows from prices_enriched:")
    spark.sql(
        """
        SELECT 
            symbol, 
            date, 
            close, 
            ma_20, 
            ma_50, 
            vol_20d AS daily_volatility
        FROM prices_enriched
        ORDER BY symbol, date
        LIMIT 5
        """
    ).show(truncate=False)

    print("\n[SQL] Per-symbol summary (row count + avg close + avg volatility):")
    spark.sql(
        """
        SELECT
            symbol,
            COUNT(*)     AS num_rows,
            AVG(close)   AS avg_close,
            AVG(vol_20d) AS avg_volatility
        FROM prices_enriched
        GROUP BY symbol
        ORDER BY symbol
        """
    ).show(truncate=False)
