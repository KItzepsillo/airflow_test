from datetime import datetime, timedelta


from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'Kevin',
    "retries": 5,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id="dag_with_catchup_backfill_v02",
    default_args=default_args,
    start_date= datetime(2024,8,14),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    task1 = BashOperator(
        task_id="task1",
        bash_command='echo This a simple bash command'
    )

    task1