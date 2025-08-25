import io
import boto3
import logging
import datetime
import requests
import pandas as pd

from bs4 import BeautifulSoup
from borocore.config import Config
from airflow.models import BaseOperator

class ExchangeExtractOperator(BaseOperator):
    def __init__(self, minio_config=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        if minio_config is None:
            minio_config = {
                'minio_endpoint': 'localhost:9000',
                'minio_access_key': 'minio',
                'minio_secret_key': 'minio123',
                'minio_bucket': 'exchange-data'
            }
        
        self.minio_endpoint = minio_config["minio_endpoint"]
        self.minio_access_key = minio_config["minio_access_key"]
        self.minio_secret_key = minio_config["minio_secret_key"]
        self.minio_bucket = minio_config["minio_bucket"]
        self.logger = logging.getLogger(__name__)
        self.s3_client = None

    def s3_client_init(self):
        """
        S3 클라이언트 초기화 (MinIO 호환)
        - boto3 클라이언트를 MinIO 엔드포인트로 설정
        - S3v4 서명 방식 사용으로 MinIO 호환성 확보
        """
        s3_client = boto3.client(
            's3',
            endpoint_url=f'http://{self.minio_endpoint}',
            aws_access_key_id=self.minio_access_key,
            aws_secret_access_key=self.minio_secret_key,
            config=Config(signature_version='s3v4'),
        )
        return s3_client
    
    def execute(self, context):
        self.logger.info("환율 데이터 추출 및 저장 시작")
        
        if self.s3_client is None:
            self.s3_client = self.s3_client_init()
            
        try:
            # MinIO 버킷 확인 및 생성
            try:
                self.s3_client.head_bucket(Bucket=self.minio_bucket)
            except:
                self.s3_client.create_bucket(Bucket=self.minio_bucket)
            
            currencies = ["USD", "EUR","JPY", "CNY", "THB", "IDR",  "GBP", 
                "CHF", "CZK", "SAR", "AED", "MYR", "DKK", "AUD", 
                "BHD", "BND", "CAD", "HKD", "KWD", "NOK", "NZD",
                "SEK", "SGD"]
            
            base_url_template = "https://finance.naver.com/marketindex/exchangeDailyQuote.nhn?marketindexCd=FX_{country_code}KRW&page="
            
            # 데이터 저장을 위한 딕셔너리 초기화
            data_dict = {}
            dates = []
            
            for country_code in currencies:
                base_url = base_url_template.format(country_code=country_code)
                currency_data = []
                
                for page in range(1, 10):
                    url = base_url + str(page)
                    response = requests.get(url)
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    rows = soup.select('table > tbody > tr')
                    if not rows:
                        break
                    
                    for row in rows:
                        cols = row.find_all('td')
                        if len(cols) < 2:
                            self.logger.info(f"날짜와 가격이 없습니다.")
                            continue
                        
                        date = cols[0].text.strip()
                        try:
                            date_object = datetime.datetime.strptime(date, "%Y.%m.%d")
                            formatted_date = date_object.strftime("%Y-%m-%d")
                        except ValueError as e:
                            self.logger.error(f"날짜 형식 변환 오류: {date}, {e}")
                            continue

                        price = cols[1].text.strip().replace(",", "")
                        
                        if country_code == currencies[0]:
                            dates.append(formatted_date)
                        
                        currency_data.append(float(price))
                
                # 첫 번째 통화인 경우 날짜 데이터 저장
                if country_code == currencies[0]:
                    data_dict["DATE"] = dates
                
                # 데이터 길이 맞추기 (None으로 패딩)
                while len(currency_data) < len(data_dict["DATE"]):
                    currency_data.append(None)
                
                data_dict[country_code] = currency_data
            
            # 전체 통화 데이터를 하나의 CSV로 저장
            df_all = pd.DataFrame(data_dict)
            file_name = "exchange_data_all_currencies.csv"
            self.s3_client.put_object(
                Bucket=self.minio_bucket,
                Key=file_name,
                Body=df_all.to_csv(index=False).encode('utf-8'),
                ContentType='text/csv'
            )
            
            self.logger.info(f"환율 데이터 저장 완료: {file_name}")

        except Exception as e:
            self.logger.error(f"환율 데이터 추출 및 저장 중 오류 발생: {str(e)}")
            raise
        

