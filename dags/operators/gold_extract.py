import io
import json
import boto3
import logging
import datetime
import requests

from botocore.config import Config

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
class GoldExtractOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        minio_endpoint='localhost:9000', 
        minio_access_key='minioadmin', 
        minio_secret_key='minioadmin', 
        minio_bucket='gold-data',
        mode='backfill',
        *args,
        **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(__name__)
        self.minio_endpoint = minio_endpoint
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.minio_bucket = minio_bucket
        self.mode = mode
        self.s3_client = self.s3_client_init()
    
    def s3_client_init(self):
        """
        S3 클라이언트 초기화 (MinIO 호환)
        """
        s3_client = boto3.client(
            's3',
            endpoint_url=f'http://{self.minio_endpoint}',
            aws_access_key_id=self.minio_access_key,
            aws_secret_access_key=self.minio_secret_key,
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'  # MinIO는 리전이 필요 없지만, boto3는 필수값
        )
        return s3_client
    
    def execute(self, context):
        self.logger.info("금값 추출 시작")
        
        try:
            # 버킷이 없으면 생성
            try:
                self.s3_client.head_bucket(Bucket=self.minio_bucket)
            except:
                self.s3_client.create_bucket(Bucket=self.minio_bucket)

            if self.mode == 'backfill':
                api_url = "https://m.stock.naver.com/front-api/chart/pricesByPeriod?reutersCode=M04020000&category=metals&chartInfoType=gold&scriptChartType=day"
                response = requests.get(api_url)
                json_data = response.json()
                if not json_data['isSuccess']:
                    self.logger.error(f"API 호출 실패\nmessage: {json_data['message']}\ndetailCode: {json_data['detailCode']}")
                    return
                if not json_data['result']['marketStatus'] == 'OPEN':
                    self.logger.info(f"오늘 주식 시장이 열리지 않았습니다. 오늘 날짜의 데이터는 추출하지 않습니다.")
                    return
                # result 데이터만 추출하여 저장
                result_data = json_data['result']
                current_date = result_data['tradeBaseAt'] # 20250604 형식
                file_name = f"gold_price_{current_date}.json"
            elif self.mode == 'fullrefresh':
                api_url = "https://m.stock.naver.com/front-api/chart/pricesByPeriod?reutersCode=M04020000&category=metals&chartInfoType=gold&scriptChartType=areaYearFive"
                response = requests.get(api_url)
                json_data = response.json()
                if not json_data['isSuccess']:
                    self.logger.error(f"API 호출 실패\nmessage: {json_data['message']}\ndetailCode: {json_data['detailCode']}")
                    return
                result_data = json_data['result']
                current_date = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d')
                file_name = f"gold_price_FULL_REFRESH_{current_date}.json"                
            else:
                raise ValueError(f"Invalid mode: {self.mode}")
            # 데이터를 바로 저장
            self.s3_client.put_object(
                Bucket=self.minio_bucket,
                Key=file_name,
                Body=json.dumps(result_data, ensure_ascii=False).encode('utf-8'),
            )
            
            self.logger.info(f"추출된 금값을 MinIO에 저장 완료: {file_name}")
            return file_name
            
        except Exception as e:
            self.logger.error(f"금값 추출 중 오류 발생: {str(e)}")
            raise