from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
from operators.gold_extract import GoldExtractOperator
from operators.gold_save import GoldSaveOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# MinIO 설정
minio_config = {
    'minio_endpoint': 'localhost:9000',
    'minio_access_key': 'minioadmin',
    'minio_secret_key': 'minioadmin',
    'minio_bucket': 'gold-data'
}

dag = DAG(
    'gold_price_pipeline',
    default_args=default_args,
    description='금값 시세 크롤링 및 저장 파이프라인',
    schedule_interval='0 9-15 * * 1-5',  # 평일 9시부터 15시까지 매시 정각
    catchup=False
)

# Extract Task
extract_task = GoldExtractOperator(
    task_id='extract_gold_price',
    dag=dag,
    **minio_config
)

# Transform Task (DockerOperator 사용)
transform_task = DockerOperator(
    task_id='transform_gold_price',
    image='gold-transform:latest',  # PySpark가 설치된 Docker 이미지
    command=[
        'python', '/app/gold_transform.py',
        '{{ task_instance.xcom_pull(task_ids="extract_gold_price") }}',  # raw_file_path
        minio_config['minio_endpoint'],
        minio_config['minio_access_key'],
        minio_config['minio_secret_key'],
        minio_config['minio_bucket']
    ],
    docker_url='unix://var/run/docker.sock',  # Docker daemon URL
    network_mode='bridge',
    dag=dag
)

# Save Task
save_task = GoldSaveOperator(
    task_id='save_gold_price',
    postgres_conn_id='postgres_default',
    dag=dag,
    **minio_config
)

# Task 의존성 설정
extract_task >> transform_task >> save_task 