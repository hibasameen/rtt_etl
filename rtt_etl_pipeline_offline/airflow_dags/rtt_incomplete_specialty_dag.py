"""Airflow DAG (example) to run the RTT ETL monthly."""

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="rtt_waiting_times_etl",
    start_date=datetime(2024, 1, 1),
    schedule="@monthly",
    catchup=False,
    max_active_runs=1,
    tags=["nhs", "rtt", "etl"],
) as dag:

    run_etl = BashOperator(
        task_id="run_rtt_etl",
        bash_command="python -m src.rtt_etl --out /opt/airflow/data --start-fy 2014 --end-fy 2018 --dataset provider_trust_all",
    )

    run_etl
