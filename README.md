# 📈 Stock Market ETL Pipeline (PySpark + Docker)

A modular, production-style **PySpark ETL pipeline** that ingests raw stock price data (CSV), enriches it with metadata, computes technical indicators (moving averages, volatility, returns), and writes the results as **partitioned Parquet datasets**.

The project is fully containerized using Docker and includes optional integration with **HDFS and Hive** for distributed storage/queries.

Data source: https://www.kaggle.com/datasets/jacksoncrow/stock-market-dataset?resource=download

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

└── 📁etl
        └── 📁extract
            ├── __init__.py
            ├── metadata_loader.py
            ├── prices_loader.py
        └── 📁load
            ├── __init__.py
            ├── writer.py
        └── 📁sql
            ├── query_parquet.py
        └── 📁transform
            ├── __init__.py
            ├── enrich.py
            ├── prices_transform.py
        ├── __init__.py
        ├── cli.py
        ├── config.py
        ├── pipeline.py
        ├── spark_app.py
    └── 📁notebooks
    └── 📁test_data
        └── 📁clean
            └── 📁prices_test (generated folder)
                └── 📁symbol=A
                    ├── .part-00000-c4021f04-770f-49c0-83eb-5fe7eac4da81.c000.snappy.parquet.crc
                    ├── part-00000-c4021f04-770f-49c0-83eb-5fe7eac4da81.c000.snappy.parquet
                    ....
        └── 📁raw
            └── 📁meta
                ├── symbols_valid_meta.csv
            └── 📁stocks
                ├── A.csv
                ├── AA.csv
                ├── AACG.csv
                ├── AAL.csv
                ├── AAMC.csv
    ├── .dockerignore
    ├── .gitignore
    ├── docker-compose.yaml
    ├── Dockerfile
    ├── README.md
    ├── requirements.txt
    └── run_etl.py # Entrypoint


### Input data format
Date,Open,High,Low,Close,Adj Close,Volume
2024-01-02,189.98,190.85,187.20,189.00,189.00,45000000

### Metadata CSV example
Symbol,Security Name,Listing Exchange,Market Category,ETF,...
AAPL,Apple Inc,Nasdaq Global Select,Q,...

### Output format (Parquet)
prices_enriched/
└── symbol=AAPL/
      part-xxxxx.snappy.parquet
└── symbol=AMZN/
└── symbol=MSFT/

### Output path for test-data: /test_data/clean


