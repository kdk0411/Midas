import pytz
import logging
import datetime
from airflow import DAG

from minio.minio import get_minio_conn
from operators.gold_save import GoldSaveOperator
from operators.gold_extract import GoldExtractOperator
from operators.gold_transform import GoldTransformOperator

logger = logging.getLogger(__name__)

minio_endpoint, minio_access_key, minio_secret_key = get_minio_conn()

# MinIO 설정
minio_config = {
    'minio_endpoint': minio_endpoint,
    'minio_access_key': minio_access_key,
    'minio_secret_key': minio_secret_key,
    'minio_bucket': 'gold-data'
}

# 한국 시간대 설정
kst = pytz.timezone('Asia/Seoul')

default_args = {
    'owner': 'kdk0411',
    'depends_on_past': False,  # 이전 실행 의존성 제거
    'start_date': datetime.datetime(2024, 1, 1, tzinfo=kst),  # 한국 시간 기준
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': datetime.timedelta(minutes=5),
    'task_concurrency': 1,  # 동일한 태스크의 동시 실행 수 제한
    'pool_slots': 1,  # 리소스 풀 슬롯 사용량 제한
}

# Selenium Grid 설정
selenium_config = {
    'selenium_hub_url': 'http://selenium-hub:4444',
    'browser_name': 'chrome'
}
# =======================================================================================
#                                       DAG Init
# =======================================================================================
with DAG(
    'gold_price_pipeline',
    default_args=default_args,
    description='금값 데이터 파이프라인',
    schedule_interval='0 * 9-15 * * 1-5',  # 평일 9시부터 15시까지 1분마다 (한국 시간)
    catchup=False,  # 이전 실행 비활성화
    max_active_runs=1,  # 동시 실행 DAG 인스턴스 수 제한 (중복 실행 방지)
    max_active_tasks=1  # 동시 실행 태스크 수 제한
) as dag:
    
    # Extract Task - 데이터 추출
    extract_task = GoldExtractOperator(
        task_id='extract_gold_price',
        minio_config=minio_config,  # minio_config 딕셔너리 직접 전달
    )

    # Transform Task (DockerOperator 사용)
    transform_task = GoldTransformOperator(
        task_id='transform_gold_price',
        minio_config=minio_config,
    )

    # Save Task - 데이터 저장
    save_task = GoldSaveOperator(
        task_id='save_gold_price',
        minio_config=minio_config,  # minio_config 딕셔너리 직접 전달
    )

    # Task 의존성 설정 - Extract 실행 후 Transform 실행 후 Save 실행
    extract_task >> transform_task >> save_task