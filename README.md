# Fin Risk Observatory

An end-to-end data engineering pipeline for financial market risk analysis.

## Overview
This project ingests daily equity price data from free public APIs, stores raw data in Amazon S3, curates clean analytics-ready datasets, and computes financial risk metrics using Amazon Athena. The pipeline is orchestrated with Apache Airflow and includes anomaly detection for large price movements and volume spikes.

## Architecture
APIs → Python ETL → S3 (raw)
      → Curated CSV (partitioned)
      → Athena CTAS (Parquet analytics)
      → Airflow orchestration & monitoring

## Key Features
- Daily ingestion of stock prices (AAPL, MSFT, TSLA, NVDA)
- Partitioned data lake design (dt=YYYY-MM-DD)
- Analytics tables in Parquet (daily returns, anomalies)
- Automated orchestration with Airflow
- Data quality and anomaly checks

## Tech Stack
- Python
- AWS S3, Athena
- Apache Airflow (Docker)
- SQL (CTAS, window functions)
- Parquet

## How to Run
1. Configure `.env`
2. Run ingestion and curation scripts
3. Start Airflow with Docker Compose
4. Trigger DAG `fin_risk_daily_pipeline`

## Example Analytics
- Daily returns
- Volatility and price jumps (>5%)
- Volume spikes (>3× rolling average)


