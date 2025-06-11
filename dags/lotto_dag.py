import logging
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator


from operators.lotto_extract import LottoExtractOperator

from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# Variable이 없을 경우를 대비한 기본값 설정
try:
    LOTTO_MODE = Variable.get("lotto_mode")
except:
    logger.warning("lotto_mode variable not found, using default value: backfill")
    LOTTO_MODE = "backfill"

# 실행 모드에 따른 DAG 설정
def get_dag_config():
    if LOTTO_MODE == "fullrefresh":
        logger.info(f"Running in fullrefresh mode")
        return {
            'schedule_interval': None,  # 스케줄링 비활성화
            'catchup': False  # 이전 실행 비활성화
        }
    elif LOTTO_MODE == "backfill":  # backfill 모드
        logger.info(f"Running in backfill mode")
        return {
            'schedule_interval': '40 20 * * 6',  # 토요일 오후 8시 40분에 1회 실행
            'catchup': False  # 이전 실행 비활성화
        }
    else:
        logger.error(f"Invalid lotto_mode: {LOTTO_MODE}")
        raise ValueError(f"Invalid lotto_mode: {LOTTO_MODE}")

default_args = {
    'owner': 'kdk0411',
    'depends_on_past': False,  # 이전 실행 의존성 제거
    'start_date': datetime(2025, 6, 5),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'queue': 'a-worker_2'  # 특정 worker 지정
}

# MinIO 설정
minio_config = {
    'minio_endpoint': 'localhost:9000',
    'minio_access_key': 'minioadmin',
    'minio_secret_key': 'minioadmin',
    'minio_bucket': 'lotto-data'
}

dag_config = get_dag_config()

dag = DAG(
    'lotto_pipeline',
    default_args=default_args,
    description='로또 데이터 추출 및 저장 파이프라인',
    **dag_config
)

# Extract Task
extract_task = LottoExtractOperator(
    task_id='extract_lotto_data',
    dag=dag,
    mode=Variable.get("lotto_mode", "backfill"),  # Airflow Variable에서 모드 가져오기
    **minio_config
)

# Transform Task
# Save Task

extract_task