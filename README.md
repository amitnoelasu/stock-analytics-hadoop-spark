# 📈 Stock Market ETL Pipeline (PySpark + Docker)

A modular, production-style **PySpark ETL pipeline** that ingests raw stock price data (CSV), enriches it with metadata, computes technical indicators (moving averages, volatility, returns), and writes the results as **partitioned Parquet datasets**.

The project is fully containerized using Docker and includes optional integration with **HDFS and Hive** for distributed storage/queries.

# Data source: https://www.kaggle.com/datasets/jacksoncrow/stock-market-dataset?resource=download

---

## 🚀 Features

### ✔ Extract
- Automatically loads all ticker CSV files  
- Parses dates, normalizes schema  
- Loads NASDAQ metadata  

### ✔ Transform
- Computes:
  - Daily returns  
  - Rolling volatility (20-day)  
  - Moving averages (MA20, MA50)  
- Joins prices with metadata  
- Cleans up column types  

### ✔ Load
- Writes cleaned, enriched datasets as **partitioned Parquet**  
- Supports:
  - Local filesystem  
  - **HDFS** (`hdfs://…`)  
  - Optional Hive table creation  

### ✔ Built-in SQL Queries
After ETL, Spark runs demonstration SQL queries for exploration.

---

## 📂 Project Structure

etl/
│
├── extract/
│   ├── prices_loader.py
│   └── metadata_loader.py
│
├── transform/
│   ├── indicators.py       # returns, MA20, MA50, volatility
│   ├── joiner.py           # join w/ metadata
│   └── enrich.py           # orchestrates transformations
│
├── load/
│   └── writer.py           # writes Parquet (local or HDFS)
│
├── analysis/
│   └── sql_examples.py     # exploratory SQL queries
│
├── spark_app.py            # SparkSession builder
├── config.py               # CLI + runtime config
└── pipeline.py             # Full ETL pipeline orchestration

run_etl.py                  # Entrypoint
Dockerfile
docker-compose.yml



# Input data format
Date,Open,High,Low,Close,Adj Close,Volume
2024-01-02,189.98,190.85,187.20,189.00,189.00,45000000

# Metadata CSV example
Symbol,Security Name,Listing Exchange,Market Category,ETF,...
AAPL,Apple Inc,Nasdaq Global Select,Q,...

# Output format (Parquet)
prices_enriched/
└── symbol=AAPL/
      part-xxxxx.snappy.parquet
└── symbol=AMZN/
└── symbol=MSFT/

# Output path for test-data: /test_data/clean


