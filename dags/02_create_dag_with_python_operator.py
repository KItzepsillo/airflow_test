from datetime import datetime, timedelta


from airflow import DAG
from airflow.operators.python import PythonOperator

#Careful, using xcom the size it's only 48Kbyte

default_args = {
    'owner': 'Kevin',
    "retries": 5,
    'retry_delay': timedelta(minutes=1)
}

def greet(ti):
    first_name = ti.xcom_pull(task_ids='get_name', key='first_name')
    last_name = ti.xcom_pull(task_ids='get_name', key='last_name')
    age = ti.xcom_pull(task_ids='get_age', key='age')
    print(f"Hello World! My name is {first_name} {last_name}, and I am {age} years old!")

# Return only one value
# def get_name():
#     return 'Jerry'

def get_name(ti):
    ti.xcom_push(key='first_name', value='Jerry')
    ti.xcom_push(key='last_name', value='Fridman')

def get_age(ti):
    ti.xcom_push(key='age', value=19)

with DAG (
    default_args=default_args,
    dag_id="our_dag_with_python_operator_v05",
    description="Our first dag using python operator",
    start_date= datetime(2024,8,15),
    schedule_interval="@daily"
) as dag:
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet,
        # op_kwargs={'age': 28},
    )

    task2 = PythonOperator(
        task_id = 'get_name',
        python_callable=get_name,

    )

    task3 = PythonOperator(
        task_id = 'get_age',
        python_callable=get_age,

    )

    [task2, task3] >> task1