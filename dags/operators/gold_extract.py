import io
import json
import boto3
import logging
import datetime
import requests

from botocore.config import Config

from airflow.models import BaseOperator

class GoldExtractOperator(BaseOperator):
    """
    금값 데이터 추출 및 저장 Operator
    - 직접 API 호출하여 데이터 추출
    - 파일 존재 여부 확인 후 갱신/생성
    - MinIO에 데이터 저장
    """
    
    def __init__(self, minio_config=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(__name__)
        
        # minio_config가 None이면 기본값 사용
        if minio_config is None:
            minio_config = {
                'minio_endpoint': 'localhost:9000',
                'minio_access_key': 'minio',
                'minio_secret_key': 'minio123',
                'minio_bucket': 'gold-data'
            }
        
        # minio_config에서 값들을 추출하여 인스턴스 변수로 설정
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
    
    def execute(self, context):
        self.logger.info("금값 데이터 추출 및 저장 시작")
        
        # s3_client 초기화
        if self.s3_client is None:
            self.s3_client = self.s3_client_init()
        
        try:
            # 버킷이 없으면 생성
            try:
                self.s3_client.head_bucket(Bucket=self.minio_bucket)
            except:
                self.s3_client.create_bucket(Bucket=self.minio_bucket)

            # 공통 변수 초기화
            file_exists = False
            current_price_count = 0
            previous_price_count = 0

            api_url = "https://m.stock.naver.com/front-api/chart/pricesByPeriod?reutersCode=M04020000&category=metals&chartInfoType=gold&scriptChartType=day"
            
            # HTTP 요청 시도
            try:
                response = requests.get(api_url, timeout=30)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                self.logger.error(f"API 요청 실패: {str(e)}")
                raise
            
            json_data = response.json()
            
            # API 응답 성공 여부 확인
            if not json_data['isSuccess']:
                self.logger.error(f"API 호출 실패: {json_data['message']}")
                return
            
            # result 데이터 추출
            result_data = json_data['result']
            current_date = result_data['tradeBaseAt']
            market_status = result_data.get('marketStatus', 'UNKNOWN')
            
            # 데이터 품질 검증
            if not current_date:
                self.logger.error("tradeBaseAt 필드가 비어있습니다.")
                return
            
            if not result_data.get('priceInfos'):
                self.logger.warning(f"날짜 {current_date}의 가격 정보가 없습니다.")
                return
            
            self.logger.info(f"거래일: {current_date}, 시장상태: {market_status}")
            file_name = f"gold_price_{current_date}.json"
            
            # MinIO에서 파일 존재 여부 확인
            try:
                existing_obj = self.s3_client.get_object(Bucket=self.minio_bucket, Key=file_name)
                existing_data = json.loads(existing_obj['Body'].read().decode('utf-8'))
                previous_price_count = len(existing_data.get('priceInfos', []))
                file_exists = True
                self.logger.info(f"기존 파일 발견: {file_name} (기존 가격 정보: {previous_price_count}개)")
            except self.s3_client.exceptions.NoSuchKey:
                self.logger.info(f"새로운 파일 생성: {file_name}")
            except Exception as e:
                self.logger.info(f"파일 체크 중 오류 (새로 생성): {str(e)}")
            
            # 현재 데이터 통계
            current_price_count = len(result_data.get('priceInfos', []))
            
            # 갱신 정보 로깅
            if file_exists:
                price_diff = current_price_count - previous_price_count
                self.logger.info(f"데이터 갱신: {previous_price_count}개 → {current_price_count}개 (차이: {price_diff:+d}개)")
            else:
                self.logger.info(f"첫 데이터 저장: {current_price_count}개 가격 정보")
            
            # 최신 가격 정보 로깅
            if result_data.get('priceInfos'):
                latest_price_info = result_data['priceInfos'][-1]
                self.logger.info(f"최신 가격: {latest_price_info['currentPrice']:,}원 ({latest_price_info['localDateTime']})")
            
            # 데이터를 UTF-8 인코딩으로 저장
            self.s3_client.put_object(
                Bucket=self.minio_bucket,
                Key=file_name,
                Body=json.dumps(result_data, ensure_ascii=False, indent=2).encode('utf-8'),
                ContentType='application/json; charset=utf-8'
            )
            if file_exists:
                    self.logger.info(f"금값 데이터 갱신 완료: {file_name}")
            else:
                self.logger.info(f"금값 데이터 첫 저장 완료: {file_name}")
            self.logger.info(f"총 저장된 가격 정보: {current_price_count}개")
            return file_name
            
        except Exception as e:
            self.logger.error(f"금값 데이터 추출 및 저장 중 오류 발생: {str(e)}")
            raise