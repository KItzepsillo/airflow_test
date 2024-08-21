from datetime import datetime, timedelta


from airflow.decorators import dag, task

#Careful, using xcom the size it's only 48Kbyte

default_args = {
    'owner': 'Kevin',
    "retries": 5,
    'retry_delay': timedelta(minutes=1)
}

@dag(
    dag_id='dag_with_taskflow_api_02',
    default_args=default_args,
    start_date=datetime(2024,8,18),
    schedule_interval='@daily',
)


def hello_wold_etl():

    @task(multiple_outputs=True)
    def get_name():
        return {
            "first_name":"Jerry",
            "last_name": "Fridman"
        }
    
    @task()
    def get_age():
        return 19

    @task()
    def greet(first_name, last_name, age):
        print(f"Hello World! My name is {first_name} {last_name} and I am {age} years old!")
    
    # name = get_name()
    name_dict = get_name()
    age = get_age()

    greet(first_name= name_dict['first_name'], last_name=name_dict['last_name'], age=age)

greet_dag = hello_wold_etl()