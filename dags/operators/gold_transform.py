import io
import json
import boto3
import logging
import datetime
import pandas as pd
from botocore.config import Config
from airflow.models import BaseOperator

class GoldTransformOperator(BaseOperator):
    """
    금값 데이터 Json to csv 변환 Operator
    - MinIO에서 JSON 파일 읽기
    - priceInfos 데이터 추출 및 CSV 변환
    - Backfill 모드에 최적화된 덮어쓰기 방식
    - MinIO에 CSV 파일 저장
    """
    
    def __init__(self, minio_config=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(__name__)
        
        if minio_config is None:
            minio_config = {
                'minio_endpoint': 'localhost:9000',
                'minio_access_key': 'minio',
                'minio_secret_key': 'minio123',
                'minio_bucket': 'gold-data'
            }
        
        self.minio_endpoint = minio_config["minio_endpoint"]
        self.minio_access_key = minio_config["minio_access_key"]
        self.minio_secret_key = minio_config["minio_secret_key"]
        self.minio_bucket = minio_config["minio_bucket"]
        
        # s3_client는 execute 메서드에서 필요할 때 초기화하도록 변경
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
            region_name='us-east-1'  # MinIO는 리전이 필요 없지만, boto3는 필수값
        )
        return s3_client
    
    def read_json_from_minio(self, execution_date):
        """
        MinIO에서 JSON 파일 읽기
        - 실행 날짜에 맞는 JSON 파일 경로 생성
        - JSON 데이터 파싱하여 priceInfos 추출
        """
        try:
            # 실행 날짜 형식에 맞게 파일명 생성 (YYYYMMDD)
            date_str = execution_date.strftime('%Y%m%d')
            json_key = f'gold_price_{date_str}.json'
            
            self.logger.info(f"JSON 파일 읽기 시작: {json_key}")
            
            # S3 클라이언트로부터 JSON 파일 읽기
            response = self.s3_client.get_object(Bucket=self.minio_bucket, Key=json_key)
            json_content = response['Body'].read().decode('utf-8')
            
            # JSON 파싱
            data = json.loads(json_content)
            
            # priceInfos 추출
            if 'priceInfos' not in data:
                raise ValueError("JSON 파일에 'priceInfos' 키가 없습니다.")
            
            price_infos = data['priceInfos']
            self.logger.info(f"JSON 파일 읽기 완료: {len(price_infos)}개의 priceInfo 데이터")
            
            return price_infos
            
        except Exception as e:
            self.logger.error(f"JSON 파일 읽기 실패: {str(e)}")
            raise
    
    def extract_required_fields(self, price_infos):
        """
        priceInfos에서 필요한 필드만 추출
        - localDateTime, currentPrice, accumulatedTradingVolume
        """
        extracted_data = []
        
        for price_info in price_infos:
            try:
                # 필요한 필드 추출
                local_date_time = price_info['localDateTime']
                current_price = price_info['currentPrice']
                accumulated_trading_volume = price_info['accumulatedTradingVolume']
                
                # 필수 필드 검증
                if local_date_time is None or current_price is None or accumulated_trading_volume is None:
                    self.logger.warning(f"필수 필드가 누락된 데이터 건너뛰기: {price_info}")
                    continue
                
                extracted_data.append({
                    'localDateTime': local_date_time,
                    'currentPrice': current_price,
                    'accumulatedTradingVolume': accumulated_trading_volume
                })
                
            except Exception as e:
                self.logger.error(f"데이터 추출 중 오류: {str(e)}, 데이터: {price_info}")
                continue
        
        self.logger.info(f"필요한 필드 추출 완료: {len(extracted_data)}개의 데이터")
        return extracted_data
    
    def save_csv_to_minio(self, df, execution_date):
        """
        DataFrame을 CSV로 변환하여 MinIO에 저장 (덮어쓰기)
        """
        try:
            date_str = execution_date.strftime('%Y%m%d')
            csv_key = f'csv/gold_price_{date_str}.csv'
            
            # DataFrame을 CSV 문자열로 변환
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_content = csv_buffer.getvalue()
            
            # MinIO에 CSV 파일 업로드 (덮어쓰기)
            self.s3_client.put_object(
                Bucket=self.minio_bucket,
                Key=csv_key,
                Body=csv_content.encode('utf-8'),
                ContentType='text/csv'
            )
            
            self.logger.info(f"CSV 파일 저장 완료: {csv_key}, 데이터 수: {len(df)}")
            
        except Exception as e:
            self.logger.error(f"CSV 파일 저장 실패: {str(e)}")
            raise
    
    def execute(self, context):
        """
        Operator 실행 메인 로직 (Backfill 모드 최적화)
        """
        try:
            self.logger.info("금값 데이터 JSON to CSV 변환 시작")
            
            # 실행 날짜 가져오기
            execution_date = context['execution_date']
            self.logger.info(f"실행 날짜: {execution_date}")
            
            # S3 클라이언트 초기화
            self.s3_client = self.s3_client_init()
            
            # 1. JSON 파일에서 데이터 읽기
            price_infos = self.read_json_from_minio(execution_date)
            
            # 2. 필요한 필드만 추출
            extracted_data = self.extract_required_fields(price_infos)
            
            if not extracted_data:
                self.logger.warning("추출할 데이터가 없습니다.")
                return
            
            # 3. DataFrame 생성
            df = pd.DataFrame(extracted_data)
            self.logger.info(f"DataFrame 생성 완료: {len(df)}개의 데이터")
            
            # 4. CSV 파일로 저장
            self.save_csv_to_minio(df, execution_date)
            
            self.logger.info("금값 데이터 JSON to CSV 변환 완료")
            
        except Exception as e:
            self.logger.error(f"Operator 실행 실패: {str(e)}")
            raise