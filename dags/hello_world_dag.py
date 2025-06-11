from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

def print_hello():
    logger.info("Hello World!")
    return "Hello World!"

default_args = {
    'owner': 'kdk0411',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 9),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'hello_world_test',
    default_args=default_args,
    description='Worker 테스트용 Hello World DAG',
    schedule_interval=None,
    catchup=False
) as dag:

    task1 = PythonOperator(
        task_id='hello_world_worker1',
        python_callable=print_hello
    )

    task2 = PythonOperator(
        task_id='hello_world_worker2',
        python_callable=print_hello
    )

    task1 >> task2 