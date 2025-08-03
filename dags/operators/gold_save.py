import io
import logging
import datetime
import pandas as pd

import boto3
from botocore.config import Config

from airflow.models import BaseOperator

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

class GoldSaveOperator(BaseOperator):
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
    
    def create_table_if_not_exists(self, session):
        """
        gold_price 테이블이 존재하지 않으면 생성
        """
        create_query = """
        CREATE TABLE IF NOT EXISTS gold_price (
            reg_date TIMESTAMP NOT NULL DEFAULT NOW(),
            localDateTime VARCHAR(32) PRIMARY KEY NOT NULL,
            currentPrice BIGINT NOT NULL,
            accumulatedTradingVolume BIGINT NOT NULL
        );
        """
        try:
            session.execute(text(create_query))
            session.commit()
            self.logger.info("gold_price 테이블이 성공적으로 생성되었습니다.")
        except Exception as e:
            session.rollback()
            self.logger.error(f"테이블 생성 중 오류 발생: {str(e)}")
            raise
    
    def check_table_exists(self, session):
        """
        gold_price 테이블 존재 여부 확인
        """
        check_table_sql = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'gold_price'
        );
        """
        
        try:
            result = session.execute(text(check_table_sql))
            exists = result.scalar()
            if exists:
                self.logger.info("gold_price 테이블이 이미 존재합니다.")
            else:
                self.logger.info("gold_price 테이블이 존재하지 않습니다.")
            return exists
        except Exception as e:
            self.logger.error(f"테이블 존재 여부 확인 중 오류 발생: {str(e)}")
            raise
    
    def validate_data(self, row):
        """
        데이터 유효성 검증
        """
        try:
            # localDateTime 검증
            if not row.get('localDateTime') or not isinstance(row['localDateTime'], str):
                raise ValueError("localDateTime은 필수이며 문자열이어야 합니다.")
            
            # currentPrice 검증
            if not isinstance(row.get('currentPrice'), (int, float)) or row['currentPrice'] <= 0:
                raise ValueError("currentPrice는 양의 숫자여야 합니다.")
            
            # accumulatedTradingVolume 검증
            if not isinstance(row.get('accumulatedTradingVolume'), (int, float)) or row['accumulatedTradingVolume'] < 0:
                raise ValueError("accumulatedTradingVolume는 0 이상의 숫자여야 합니다.")
            
            return True
        except Exception as e:
            self.logger.error(f"데이터 검증 실패: {str(e)}, 데이터: {row}")
            return False
    
    def insert_data_safely(self, session, df):
        """
        데이터를 안전하게 삽입 (Session 기반 트랜잭션 관리)
        """
        inserted_count = 0
        error_count = 0
        
        # UPSERT 쿼리 (PostgreSQL의 ON CONFLICT 사용)
        upsert_sql = """
        INSERT INTO gold_price (localDateTime, currentPrice, accumulatedTradingVolume, reg_date)
        VALUES (:localDateTime, :currentPrice, :accumulatedTradingVolume, NOW())
        ON CONFLICT (localDateTime) 
        DO UPDATE SET
            currentPrice = EXCLUDED.currentPrice,
            accumulatedTradingVolume = EXCLUDED.accumulatedTradingVolume,
            reg_date = NOW();
        """
        
        try:
            for index, row in df.iterrows():
                try:
                    # 데이터 검증
                    if not self.validate_data(row):
                        error_count += 1
                        continue
                    
                    # 데이터 삽입/업데이트
                    session.execute(text(upsert_sql), {
                        'localDateTime': row['localDateTime'],
                        'currentPrice': row['currentPrice'],
                        'accumulatedTradingVolume': row['accumulatedTradingVolume']
                    })
                    
                    inserted_count += 1
                    
                    # 100개마다 로그 출력
                    if inserted_count % 100 == 0:
                        self.logger.info(f"중간 처리 완료: {inserted_count}개 처리됨")
                    
                except Exception as e:
                    self.logger.error(f"행 {index} 삽입 중 오류: {str(e)}, 데이터: {row}")
                    error_count += 1
                    # 개별 행 오류는 계속 진행하되 로그 기록
            
            # 모든 데이터 삽입 후 커밋
            session.commit()
            self.logger.info(f"트랜잭션 커밋 완료: {inserted_count}개 성공, {error_count}개 실패")
                
        except Exception as e:
            session.rollback()
            self.logger.error(f"트랜잭션 실행 중 오류 발생: {str(e)}")
            raise
        
        return inserted_count, error_count
    
    def get_table_count(self, session):
        """
        테이블의 총 레코드 수 조회
        """
        count_sql = "SELECT COUNT(*) FROM gold_price;"
        
        try:
            result = session.execute(text(count_sql))
            count = result.scalar()
            self.logger.info(f"테이블 총 레코드 수: {count}")
            return count
        except Exception as e:
            self.logger.error(f"레코드 수 조회 중 오류 발생: {str(e)}")
            raise
    
    def read_data_from_minio(self):
        """
        MinIO에서 금값 데이터를 읽어옴
        """
        try:
            # MinIO에서 최신 파일 목록 가져오기
            response = self.s3_client.list_objects_v2(
                Bucket=self.minio_bucket,
                Prefix='gold_data/'
            )
            
            if 'Contents' not in response:
                self.logger.warning("MinIO 버킷에 데이터 파일이 없습니다.")
                return pd.DataFrame()
            
            # 가장 최신 파일 찾기
            latest_file = max(response['Contents'], key=lambda x: x['LastModified'])
            file_key = latest_file['Key']
            
            self.logger.info(f"MinIO에서 파일 읽기: {file_key}")
            
            # 파일 다운로드
            response = self.s3_client.get_object(Bucket=self.minio_bucket, Key=file_key)
            file_content = response['Body'].read()
            
            # CSV 데이터를 DataFrame으로 변환
            df = pd.read_csv(io.StringIO(file_content.decode('utf-8')))
            
            # 컬럼명 검증
            required_columns = ['localDateTime', 'currentPrice', 'accumulatedTradingVolume']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                raise ValueError(f"필수 컬럼이 누락되었습니다: {missing_columns}")
            
            self.logger.info(f"MinIO에서 {len(df)}개의 데이터를 성공적으로 읽어왔습니다.")
            return df
            
        except Exception as e:
            self.logger.error(f"MinIO에서 데이터 읽기 실패: {str(e)}")
            raise

    def execute(self, context):
        self.logger.info("금값 데이터 저장 시작")
        
        # s3_client 초기화
        if self.s3_client is None:
            self.s3_client = self.s3_client_init()
        
        # PostgreSQL 연결 URL
        postgres_url = "postgresql://postgres:postgres@localhost:5432/price_db"
        
        # Engine과 Session 생성
        engine = None
        session = None
        
        try:
            # MinIO에서 데이터 읽기
            df = self.read_data_from_minio()
            
            if df.empty:
                self.logger.warning("삽입할 데이터가 없습니다.")
                return
            
            # 데이터베이스 연결 및 Session 생성
            engine = create_engine(postgres_url)
            Session = sessionmaker(bind=engine)
            session = Session()
            
            # 테이블 존재 여부 확인 및 생성
            if not self.check_table_exists(session):
                self.create_table_if_not_exists(session)
            
            self.logger.info(f"총 {len(df)}개의 데이터를 처리합니다.")
            
            # 데이터 삽입
            inserted_count, error_count = self.insert_data_safely(session, df)
            
            self.logger.info(f"데이터 저장 완료: 성공 {inserted_count}개, 실패 {error_count}개")
            
            # 결과 검증
            self.get_table_count(session)
            
        except Exception as e:
            self.logger.error(f"데이터 저장 중 오류 발생: {str(e)}")
            raise
        finally:
            # Session과 Engine 정리
            if session:
                session.close()
            if engine:
                engine.dispose()
            self.logger.info("데이터베이스 연결이 정리되었습니다.") 