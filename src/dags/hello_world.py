from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from lib_a import test_print_name2
from lib_a.core import hello2


def return_name(name):
    return name

with DAG(
    dag_id="hello_world",
    start_date=datetime(2024, 1, 1),
    schedule="0 12 * * *",
    default_args={
        "owner": "data_team",
        "retries": 3,
    },
    tags=["production"],
    catchup=False,
) as dag:


    hello_task = PythonOperator(
        task_id='print_hello',
        python_callable=lambda: hello2())

    hello_task2 = PythonOperator(
        task_id='print_hello2',
        python_callable=lambda: test_print_name2())


    hello_task >> hello_task2