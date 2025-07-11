import logging
from airflow import DAG
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.providers.docker.operators.docker import DockerOperator

from operators.gold_save import GoldSaveOperator
from operators.gold_extract import GoldExtractOperator
from sensor.gold_price_sensor import GoldPriceSensor

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
            'catchup': False,  # 이전 실행 비활성화
            'max_active_runs': 1,  # 동시 실행 DAG 인스턴스 수 제한
            'max_active_tasks': 1  # 동시 실행 태스크 수 제한
        }
    elif GOLD_PRICE_MODE == "backfill":  # backfill 모드
        logger.info(f"Running in backfill mode")
        return {
            'schedule_interval': '*/1 9-15 * * 1-5',  # 평일 9시부터 15시까지 1분마다 (한국시간 기준)
            'catchup': False,  # 이전 실행 비활성화
            'max_active_runs': 1,  # 동시 실행 DAG 인스턴스 수 제한 (중복 실행 방지)
            'max_active_tasks': 1  # 동시 실행 태스크 수 제한
        }
    else:
        logger.error(f"Invalid gold_price_mode: {GOLD_PRICE_MODE}")
        raise ValueError(f"Invalid gold_price_mode: {GOLD_PRICE_MODE}")

def get_minio_conn(conn_id='minio'):
    """
    MinIO 연결 정보를 Airflow Connection에서 가져오는 함수
    - Connection ID를 통해 엔드포인트, 접근 키, 비밀 키 획득
    - 보안을 위해 하드코딩 대신 Connection 사용
    """
    conn = BaseHook.get_connection(conn_id)
    # endpoint_url은 extra에 저장됨
    endpoint = conn.extra_dejson.get('endpoint_url')
    access_key = conn.login
    secret_key = conn.password
    return endpoint, access_key, secret_key

minio_endpoint, minio_access_key, minio_secret_key = get_minio_conn('minio')

# MinIO 설정
minio_config = {
    'minio_endpoint': minio_endpoint,
    'minio_access_key': minio_access_key,
    'minio_secret_key': minio_secret_key,
    'minio_bucket': 'gold-data'
}

default_args = {
    'owner': 'kdk0411',
    'depends_on_past': False,  # 이전 실행 의존성 제거
    'start_date': datetime(2025, 6, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'task_concurrency': 1,  # 동일한 태스크의 동시 실행 수 제한
    'pool_slots': 1,  # 리소스 풀 슬롯 사용량 제한
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
    description='금값 시세 데이터 가용성 확인 및 저장 파이프라인',
    **dag_config
)

# Sensor Task - 데이터 가용성 확인
sensor_task = GoldPriceSensor(
    task_id='gold_price_sensor',
    mode=GOLD_PRICE_MODE,
    check_market_status=True,  # 시장 상태 확인 여부
    poke_interval=30,  # 30초마다 확인
    timeout=300,  # 5분 타임아웃
    dag=dag
)

# Extract Task - 데이터 저장
extract_task = GoldExtractOperator(
    task_id='extract_gold_price',
    mode=GOLD_PRICE_MODE,  # 모드 직접 전달
    dag=dag,
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

# Task 의존성 설정 - Sensor 먼저 실행 후 Extract 실행
sensor_task >> extract_task