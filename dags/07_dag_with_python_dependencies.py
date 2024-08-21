from datetime import datetime, timedelta


from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'Kevin',
    "retries": 5,
    'retry_delay': timedelta(minutes=1)
}

def get_sklearn():
    import sklearn
    print(f"scikit learn version: {sklearn.__version__}")

def get_matplotlib():
    import matplotlib
    print(f"matplotlib with version: {matplotlib.__version__}")

with DAG(
    default_args=default_args,
    dag_id="dag_with_python_dependencies_v01",
    start_date= datetime(2024,8,15),
    schedule_interval="@daily"
) as dag:
    task1 = PythonOperator(
        task_id='get_sklearn',
        python_callable=get_sklearn
    )

    task2 = PythonOperator(
        task_id='get_matplotlib',
        python_callable=get_matplotlib
    )

    task2 >> task1