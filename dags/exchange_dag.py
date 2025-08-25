import pytz
import logging
import datetime
from airflow import DAG

from minio.minio import get_minio_conn
from operators.exchange_extract import ExchangeExtractOperator

logger = logging.getLogger(__name__)

minio_endpoint, minio_access_key, minio_secret_key = get_minio_conn()
minio_conifg = {
    'minio_endpoint': minio_endpoint,
    'minio_access_key': minio_access_key,
    'minio_secret_key': minio_secret_key,
    'minio_bucket': 'exchange-data'
}

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
# =======================================================================================
#                                       DAG Init
# =======================================================================================
with DAG(
    'exchange_pipeline',
    default_args=default_args,
    description='환율 데이터 파이프라인',
    schedule_interval='0 21 * * 6',
    catchup=False,
    max_active_runs=1,  # 동시 실행 DAG 인스턴스 수 제한 (중복 실행 방지)
    max_active_tasks=1  # 동시 실행 태스크 수 제한
) as dag:

    # Extract Task - 데이터 추출
    extract_task = ExchangeExtractOperator(
        task_id='extract_exchange_data',
        minio_config=minio_config,  # minio_config 딕셔너리 직접 전달
    )

# Task 의존성 설정
extrackt_task >> 