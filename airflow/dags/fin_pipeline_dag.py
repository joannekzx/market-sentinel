from datetime import datetime
import os
import subprocess
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.hooks.athena import AthenaHook

PROJECT_ROOT = "/opt/airflow/project"  # we will mount your repo here
DT = datetime.now().date().isoformat()

AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-1")
ATHENA_DB = "fin_risk"
ATHENA_OUTPUT = os.getenv("ATHENA_OUTPUT_S3")

def run_cmd(cmd: list[str]):
    result = subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"Command failed: {' '.join(cmd)}")

def ingest_raw():
    run_cmd(["bash", "-lc", "set -a; source .env; set +a; python -m src.run_ingest_prices_raw"])

def curate_csv():
    run_cmd(["bash", "-lc", f"set -a; source .env; set +a; python -m src.curate_prices_csv --dt {DT}"])

def check_anomalies():
    hook = AthenaHook(aws_conn_id="aws_default", region_name=AWS_REGION)
    rows = hook.get_records(
        sql="SELECT COUNT(*) FROM fin_risk.fct_anomalies",
        database=ATHENA_DB,
        output_location=ATHENA_OUTPUT,
    )
    anomaly_count = rows[0][0]
    print(f"[INFO] anomaly_count={anomaly_count}")
    if anomaly_count and int(anomaly_count) > 0:
        raise ValueError(f"Anomalies detected: {anomaly_count}")

default_args = {"owner": "joanne", "retries": 0}

with DAG(
    dag_id="fin_risk_daily_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["finance", "athena", "s3"],
) as dag:

    t1 = PythonOperator(task_id="ingest_raw_prices", python_callable=ingest_raw)
    t2 = PythonOperator(task_id="curate_prices_csv", python_callable=curate_csv)

    refresh_returns = AthenaOperator(
        task_id="refresh_fct_daily_returns",
        query="sql/refresh_fct_daily_returns.sql",
        database=ATHENA_DB,
        output_location=ATHENA_OUTPUT,
        aws_conn_id="aws_default",
        region_name=AWS_REGION,
    )

    refresh_anoms = AthenaOperator(
        task_id="refresh_fct_anomalies",
        query="sql/refresh_fct_anomalies.sql",
        database=ATHENA_DB,
        output_location=ATHENA_OUTPUT,
        aws_conn_id="aws_default",
        region_name=AWS_REGION,
    )

    t5 = PythonOperator(task_id="check_anomalies", python_callable=check_anomalies)

    t1 >> t2 >> refresh_returns >> refresh_anoms >> t5

