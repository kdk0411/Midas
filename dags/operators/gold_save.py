import io
import logging
import datetime
import pandas as pd

import boto3
from botocore.config import Config

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from sqlalchemy import create_engine, Column, BigInteger, DateTime, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class GoldPrice(Base):
    __tablename__ = 'gold_today'
    
    seq = Column(BigInteger, primary_key=True, autoincrement=True)
    reg_date = Column(DateTime, nullable=False, default=datetime.datetime.now)
    gold_price = Column(String(16), nullable=False)
    run_time = Column(String(128), nullable=False)

class GoldSaveOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        postgres_conn_id='postgres_default', 
        minio_endpoint='localhost:9000',
        minio_access_key='minioadmin', 
        minio_secret_key='minioadmin',
        minio_bucket='gold-data',
        *args,
        **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(__name__)
        self.postgres_conn_id = postgres_conn_id
        self.minio_endpoint = minio_endpoint
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.minio_bucket = minio_bucket
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
        self.logger.info("금값 데이터 저장 시작")
        # PostgreSQL 연결 URL
        postgres_url = "postgresql://postgres:postgres@localhost:5432/gold_db"
        
        try:
            # XCom에서 변환된 CSV 파일 경로 가져오기
            csv_file_path = context['task_instance'].xcom_pull(task_ids='transform_gold_price')
            
            # MinIO에서 CSV 파일 읽기
            response = self.s3_client.get_object(self.minio_bucket, csv_file_path)
            df = pd.read_csv(io.BytesIO(response['Body'].read()))
            
            # 데이터베이스 연결
            engine = create_engine(postgres_url)
            Session = sessionmaker(bind=engine)
            session = Session()
            
            # 데이터 저장
            for _, row in df.iterrows():
                new_record = GoldPrice(
                    gold_price=row['gold_price'],
                    reg_date=pd.to_datetime(row['reg_date']),
                    run_time=row['run_time']
                )
                session.add(new_record)
            
            session.commit()
            self.logger.info("금값 데이터 저장 완료")
            
        except Exception as e:
            self.logger.error(f"데이터 저장 중 오류 발생: {str(e)}")
            if 'session' in locals():
                session.rollback()
            raise
        finally:
            if 'session' in locals():
                session.close()
            if 'engine' in locals():
                engine.dispose() 