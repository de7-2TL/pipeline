from airflow.models import Variable
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
import pendulum
from datetime import datetime, timedelta


with DAG(
    dag_id='Project Pipeline',
    start_date = pendulum.today('UTC').add(days=-2),
    schedule = '15 * * * *',
    catchup=False
) as dag:
    pass
