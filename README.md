# Fin Risk Observatory

An end-to-end data engineering pipeline for financial market risk analysis.

This project ingests daily equity price data from free public APIs, stores raw data in Amazon S3, curates analytics-ready datasets, and computes financial risk metrics such as daily returns, anomalies, and Sharpe ratios using Amazon Athena. The entire workflow is orchestrated with Apache Airflow and designed with production-style data modeling and monitoring in mind.

---

## Architecture Overview

APIs → Python ETL → S3 (raw, partitioned)  
   → Curated CSV (clean schema)  
   → Athena CTAS (Parquet analytics tables)  
   → Airflow orchestration & anomaly checks  

**Core principles**
- Separation of raw, curated, and analytics layers
- Partitioned data lake design (`dt=YYYY-MM-DD`)
- SQL-based analytics using window functions
- Automated orchestration and failure detection

---

## User Stories

### As a finance or data analyst
- I want daily OHLCV price data for selected equities (e.g., AAPL, MSFT, TSLA, NVDA) so that I can analyze returns and performance.
- I want analytics-ready tables so that I can compute risk metrics without manually cleaning API data.

### As a risk analyst
- I want the pipeline to flag unusually large price moves (e.g., >5%) so that I can investigate potential market events.
- I want volume spike detection to identify abnormal trading activity.

### As a data engineer
- I want raw ingested data stored in S3 with deterministic date partitions so that downstream transformations are reproducible.
- I want curated datasets with consistent schemas for reliable SQL analytics.
- I want orchestration so that ingestion, curation, and analytics refresh run in the correct order.
- I want the pipeline to fail fast when anomalies are detected.

### As a reviewer or interviewer
- I want clear documentation and a reproducible setup so that I can evaluate the project quickly.

---

## Key Features

- Daily ingestion of equity price data from free public APIs
- Partitioned raw data lake in Amazon S3
- Curated CSV layer with standardized schema
- Analytics tables stored as Parquet (cost-efficient Athena queries)
- Financial metrics:
  - Daily returns
  - Price jump detection (>5%)
  - Volume spike detection (>3× rolling average)
  - Sharpe ratio (daily and annualized)
- Apache Airflow DAG for orchestration and monitoring
- GitHub Actions for automated Python linting

---

## Tech Stack

- **Python** (ETL, validation)
- **Amazon S3** (data lake)
- **Amazon Athena** (SQL analytics, CTAS)
- **Apache Airflow** (orchestration, Docker-based)
- **SQL** (window functions, analytics modeling)
- **Parquet** (columnar analytics storage)
- **GitHub Actions** (CI linting)

---

## Repository Structure

fin-risk-observatory/
├── src/
│ ├── extract/ # API ingestion logic
│ ├── curate/ # Raw → curated transformations
│ ├── load/ # S3 helpers
│ └── common/ # Config, HTTP utilities
│
├── airflow/
│ ├── dags/
│ │ └── fin_pipeline_dag.py
│ ├── sql/
│ │ ├── refresh_fct_daily_returns.sql
│ │ ├── refresh_fct_anomalies.sql
│ │ └── check_anomalies.sql
│ └── docker-compose.yaml
│
├── .github/workflows/
│ └── lint.yml
│
├── README.md
├── requirements.txt
├── pyproject.toml
├── .env.example
└── .gitignore


---

## How to Run

### Prerequisites

- Python 3.x
- AWS account (free tier is sufficient)
- AWS CLI configured (`aws configure`)
- An S3 bucket you own
- Amazon Athena enabled in your AWS region
- Docker + Docker Compose (for Airflow)

---

### 1) Clone the repository

```bash
git clone <your-repo-url>
cd fin-risk-observatory
```

### 2) Set up Python environment
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3) Configure environment variables
Create a `.env` file from the template:
```bash
cp .env.example .env
```

Fill in:
- `ALPHAVANTAGE_KEY`
- `AWS_KEY`
- `S3_BUCKET`
- `ATHENA_OUTPUT_S3`

Load variables into your shell:
```bash
set -a; source .env; set +a
```

### 4) Run raw ingestion (API → S3)
```bash
python -m src.run_ingest_prices_raw
```

Expected output:

S3 objects like
`raw/alphavantage/daily/symbol=NVDA/dt=YYYY-MM-DD/data.json`

### 5) Curate data (raw → curated CSV)
```bash
python -m src.curate_prices_csv --dt YYYY-MM-DD
```

Expected output:

`curated/prices_daily_csv/dt=YYYY-MM-DD/prices_daily.csv`

### 6) Athena setup (one-time)

In the Athena console:

Set query results location to  `ATHENA_OUTPUT_S3`

Create database:

`CREATE DATABASE IF NOT EXISTS fin_risk;`


Create external table over curated CSV

Run `MSCK REPAIR TABLE` to load partitions

### 7) Build analytics tables

Using Athena CTAS:

- fct_daily_returns

- fct_anomalies

- fct_sharpe_ratio

These tables are stored as Parquet under:

`analytics/`

### 8) Run Airflow (local orchestration)
```bash
cd airflow
docker compose up
```

Open Airflow UI:

http://localhost:8081


Login:

- Username: admin
- Password: admin

Enable and trigger DAG:
`fin_risk_daily_pipeline`

Example Analytics Queries

- NVDA daily returns
```sql
SELECT *
FROM fin_risk.fct_daily_returns
WHERE symbol = 'NVDA'
ORDER BY trade_date DESC
LIMIT 10;
```

- Sharpe ratio
```sql
SELECT *
FROM fin_risk.fct_sharpe_ratio;
```
---

## CI / Code Quality

-GitHub Actions automatically runs Python linting using ruff

- Enforces consistent formatting and catches common issues on every push

## Future Improvements

- Integrate macroeconomic indicators (FRED)

- Rolling Sharpe ratios and volatility

- Portfolio-level risk metrics

- Slack or email alerts from Airflow

- dbt-based transformations instead of CTAS

## Disclaimer

This project is for educational and portfolio purposes only and does not constitute financial advice.



