from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

# 👇 important (your project path)
sys.path.append("/workspaces/orasync")

from pipeline_final import run_pipeline

default_args = {
    "owner": "yash",
    "start_date": datetime(2024, 1, 1),
    "retries": 1
}

dag = DAG(
    dag_id="orasync_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

run_task = PythonOperator(
    task_id="run_pipeline",
    python_callable=run_pipeline,
    dag=dag
)
