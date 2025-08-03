import logging
import datetime
import pytz
from airflow import DAG
from airflow.hooks.base import BaseHook

from operators.lotto_save import LottoSaveOperator
from operators.lotto_extract import LottoExtractOperator

logger = logging.getLogger(__name__)

def get_minio_conn(conn_id='minio'):
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
    'minio_bucket': 'lotto-data'
}

# 한국 시간대 설정
kst = pytz.timezone('Asia/Seoul')

default_args = {
    'owner': 'kdk0411',
    'depends_on_past': False,  # 이전 실행 의존성 제거
    'start_date': datetime.datetime(2025, 6, 5, tzinfo=kst),  # 한국 시간 기준
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': datetime.timedelta(minutes=5),
}

with DAG(
    'lotto_pipeline',
    default_args=default_args,
    description='로또 데이터 추출 및 저장 파이프라인',
    schedule_interval='0 21 * * 6',  # 매주 토요일 오후 9시 00분 (한국 시간)
    catchup=False
) as dag:

    # Extract Task
    extract_task = LottoExtractOperator(
        task_id='extract_lotto_data',
        minio_config=minio_config,
    )
    
    # Save Task
    save_task = LottoSaveOperator(
        task_id='save_lotto_data',
        minio_config=minio_config,
    )
    
    # Task 의존성 설정
    extract_task >> save_task