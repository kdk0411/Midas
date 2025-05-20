from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import logging
import json
from datetime import datetime
from minio import Minio
import io

class GoldExtractOperator(BaseOperator):
    @apply_defaults
    def __init__(self, minio_endpoint='localhost:9000', minio_access_key='minioadmin', 
                 minio_secret_key='minioadmin', minio_bucket='gold-data', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(__name__)
        self.minio_endpoint = minio_endpoint
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.minio_bucket = minio_bucket

    def execute(self, context):
        self.logger.info("금값 추출 시작")
        
        # Chrome 옵션 설정
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        
        try:
            # MinIO 클라이언트 초기화
            minio_client = Minio(
                self.minio_endpoint,
                access_key=self.minio_access_key,
                secret_key=self.minio_secret_key,
                secure=False
            )
            
            # 버킷이 없으면 생성
            if not minio_client.bucket_exists(self.minio_bucket):
                minio_client.make_bucket(self.minio_bucket)
            
            # 크롤링 실행
            driver = webdriver.Chrome(options=chrome_options)
            url = 'https://m.stock.naver.com/marketindex/metals/M04020000'
            driver.get(url)
            
            wait = WebDriverWait(driver, 10)
            price_element = wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "span.stock_price"))
            )
            
            price = price_element.text
            current_time = datetime.now()
            
            # 데이터를 JSON 형식으로 변환
            data = {
                'gold_price': price,
                'reg_date': current_time.isoformat(),
                'run_time': current_time.strftime('%H:%M')
            }
            
            # JSON 파일을 MinIO에 업로드
            json_data = json.dumps(data)
            file_name = f"raw/gold_price_{current_time.strftime('%Y%m%d_%H%M%S')}.json"
            
            minio_client.put_object(
                bucket_name=self.minio_bucket,
                object_name=file_name,
                data=io.BytesIO(json_data.encode()),
                length=len(json_data.encode()),
                content_type='application/json'
            )
            
            self.logger.info(f"추출된 금값을 MinIO에 저장 완료: {file_name}")
            return file_name
            
        except Exception as e:
            self.logger.error(f"금값 추출 중 오류 발생: {str(e)}")
            raise
        finally:
            if 'driver' in locals():
                driver.quit() 