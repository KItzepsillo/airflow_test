from datetime import datetime, timedelta


from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'Kevin',
    "retries": 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='out_first_dag_v4',
    default_args=default_args,
    description='This is our first dag that we write',
    start_date= datetime(2024,8,14,2),
    schedule_interval='@daily'
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command="echo hello world, this is the first task!"
    )

    task2 = BashOperator(
        task_id='second_task',
        bash_command="echo task2"
    ) 

    task3 = BashOperator(
        task_id='third_task',
        bash_command="echo task2"
    ) 

    # Task Dependency method 1
    # task1.set_downstream(task2)
    # task1.set_downstream(task3)


    # Task Dependency method 2
    # task1 >> task2
    # task1 >> task3


    # Task Dependency method 3
    task1 >> [task2, task3]