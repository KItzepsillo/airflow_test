from datetime import datetime, timedelta


from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'Kevin',
    "retries": 5,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id="dag_with_cron_expression_v02",
    default_args=default_args,
    start_date= datetime(2024,8,10),
    schedule_interval="0 3 * * Tue,Fri", # "@daily"
    catchup=False,
) as dag:
    task1 = BashOperator(
        task_id="task1",
        bash_command='echo This a simple bash command'
    )

    task1