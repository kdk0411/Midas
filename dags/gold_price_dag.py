import logging
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator


from operators.gold_extract import GoldExtractOperator
from operators.gold_save import GoldSaveOperator

from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# Variable이 없을 경우를 대비한 기본값 설정
try:
    GOLD_PRICE_MODE = Variable.get("gold_price_mode")
except:
    logger.warning("gold_price_mode variable not found, using default value: backfill")
    GOLD_PRICE_MODE = "backfill"

# 실행 모드에 따른 DAG 설정
def get_dag_config():
    if GOLD_PRICE_MODE == "fullrefresh":
        logger.info(f"Running in fullrefresh mode")
        return {
            'schedule_interval': None,  # 스케줄링 비활성화
            'catchup': False  # 이전 실행 비활성화
        }
    elif GOLD_PRICE_MODE == "backfill":  # backfill 모드
        logger.info(f"Running in backfill mode")
        return {
            'schedule_interval': '*/1 9-15 * * 1-5',  # 평일 9시부터 15시까지 1분마다
            'catchup': False  # 이전 실행 비활성화
        }
    else:
        logger.error(f"Invalid gold_price_mode: {GOLD_PRICE_MODE}")
        raise ValueError(f"Invalid gold_price_mode: {GOLD_PRICE_MODE}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,  # 이전 실행 의존성 제거
    'start_date': datetime(2025, 6, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'queue': 'a-worker_1'  # 특정 worker 지정
}

# MinIO 설정
minio_config = {
    'minio_endpoint': 'localhost:9000',
    'minio_access_key': 'minioadmin',
    'minio_secret_key': 'minioadmin',
    'minio_bucket': 'gold-data'
}

# Selenium Grid 설정
selenium_config = {
    'selenium_hub_url': 'http://selenium-hub:4444',
    'browser_name': 'chrome'
}

dag_config = get_dag_config()

dag = DAG(
    'gold_price_pipeline',
    default_args=default_args,
    description='금값 시세 크롤링 및 저장 파이프라인',
    **dag_config
)

# Extract Task
extract_task = GoldExtractOperator(
    task_id='extract_gold_price',
    dag=dag,
    mode=Variable.get("gold_price_mode", "backfill"),  # Airflow Variable에서 모드 가져오기
    **minio_config
)

# # Transform Task (DockerOperator 사용)
# transform_task = DockerOperator(
#     task_id='transform_gold_price',
#     image='gold-transform:latest',  # PySpark가 설치된 Docker 이미지
#     command=[
#         'python', '/app/gold_transform.py',
#         '{{ task_instance.xcom_pull(task_ids="extract_gold_price") }}',  # raw_file_path
#         minio_config['minio_endpoint'],
#         minio_config['minio_access_key'],
#         minio_config['minio_secret_key'],
#         minio_config['minio_bucket']
#     ],
#     docker_url='unix://var/run/docker.sock',  # Docker daemon URL
#     network_mode='bridge',
#     dag=dag
# )

# # Save Task
# save_task = GoldSaveOperator(
#     task_id='save_gold_price',
#     postgres_conn_id='postgres_default',
#     dag=dag,
#     **minio_config
# )

# # Task 의존성 설정
# extract_task >> transform_task >> save_task
extract_task