import json
import datetime
import logging
import boto3
from botocore.client import Config
import sqlalchemy
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, DateTime, BigInteger
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import ProgrammingError

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LottoSaveOperator(BaseOperator):
    @apply_defaults
    def __init__(self, minio_config=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(__name__)
        
        # minio_config가 None이면 기본값 사용
        if minio_config is None:
            minio_config = {
                'minio_endpoint': 'localhost:9000',
                'minio_access_key': 'minio',
                'minio_secret_key': 'minio123',
                'minio_bucket': 'lotto-data'
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
    
    def check_table_exists(self, session):
        """
        lotto_results 테이블 존재 여부 확인
        """
        check_table_sql = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'lotto_results'
        );
        """
        
        try:
            result = session.execute(text(check_table_sql))
            exists = result.scalar()
            if exists:
                self.logger.info("lotto_results 테이블이 이미 존재합니다.")
            else:
                self.logger.info("lotto_results 테이블이 존재하지 않습니다.")
            return exists
        except Exception as e:
            self.logger.error(f"테이블 존재 여부 확인 중 오류 발생: {str(e)}")
            raise
    
    def create_table_if_not_exists(self, session):
        """
        lotto_results 테이블이 존재하지 않으면 생성
        """
        create_query = """
        CREATE TABLE IF NOT EXISTS lotto_results (
            draw_no INTEGER PRIMARY KEY,
            draw_date TIMESTAMP,
            num_1 SMALLINT,
            num_2 SMALLINT,
            num_3 SMALLINT,
            num_4 SMALLINT,
            num_5 SMALLINT,
            num_6 SMALLINT,
            bonus SMALLINT,
            extraction_timestamp TIMESTAMP,
            total_sell_amount BIGINT,
            first_win_amount BIGINT,
            first_winner_count INTEGER,
            first_accum_amount BIGINT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        try:
            session.execute(text(create_query))
            session.commit()
            self.logger.info("lotto_results 테이블이 성공적으로 생성되었습니다.")
        except Exception as e:
            session.rollback()
            self.logger.error(f"테이블 생성 중 오류 발생: {str(e)}")
            raise
    
    def validate_data(self, row):
        """
        데이터 유효성 검증
        """
        try:
            # draw_no 검증
            if not isinstance(row.get('draw_no'), (int, str)) or not row['draw_no']:
                raise ValueError("draw_no는 필수이며 숫자 또는 문자열이어야 합니다.")
            
            # 로또 번호 검증
            for i in range(1, 7):
                num_key = f'num_{i}'
                if not isinstance(row.get(num_key), (int, float)) or row[num_key] < 1 or row[num_key] > 45:
                    raise ValueError(f"{num_key}는 1-45 사이의 숫자여야 합니다.")
            
            # 보너스 번호 검증
            if not isinstance(row.get('bonus'), (int, float)) or row['bonus'] < 1 or row['bonus'] > 45:
                raise ValueError("bonus는 1-45 사이의 숫자여야 합니다.")
            
            return True
        except Exception as e:
            self.logger.error(f"데이터 검증 실패: {str(e)}, 데이터: {row}")
            return False
    
    def insert_data_safely(self, session, data):
        """
        데이터를 안전하게 삽입 (Session 기반 트랜잭션 관리)
        """
        inserted_count = 0
        error_count = 0
        
        # 단순 INSERT 쿼리 (UPSERT 불필요)
        insert_sql = """
        INSERT INTO lotto_results (
            draw_no, draw_date, num_1, num_2, num_3, num_4, num_5, num_6,
            bonus, extraction_timestamp, total_sell_amount, first_win_amount,
            first_winner_count, first_accum_amount
        ) VALUES (
            :draw_no, :draw_date, :num_1, :num_2, :num_3, :num_4, :num_5, :num_6,
            :bonus, :extraction_timestamp, :total_sell_amount, :first_win_amount,
            :first_winner_count, :first_accum_amount
        );
        """
        
        try:
            for row in data:
                try:
                    # 데이터 검증
                    if not self.validate_data(row):
                        error_count += 1
                        continue
                    
                    # 데이터 삽입
                    session.execute(text(insert_sql), {
                        "draw_no": row["draw_no"],
                        "draw_date": row["draw_date"],
                        "num_1": row["num_1"],
                        "num_2": row["num_2"],
                        "num_3": row["num_3"],
                        "num_4": row["num_4"],
                        "num_5": row["num_5"],
                        "num_6": row["num_6"],
                        "bonus": row["bonus"],
                        "extraction_timestamp": row["extraction_timestamp"],
                        "total_sell_amount": row["total_sell_amount"],
                        "first_win_amount": row["first_win_amount"],
                        "first_winner_count": row["first_winner_count"],
                        "first_accum_amount": row["first_accum_amount"]
                    })
                    
                    inserted_count += 1
                    
                except Exception as e:
                    self.logger.error(f"회차 {row.get('draw_no', 'unknown')} 삽입 중 오류: {str(e)}, 데이터: {row}")
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
        count_sql = "SELECT COUNT(*) FROM lotto_results;"
        
        try:
            result = session.execute(text(count_sql))
            count = result.scalar()
            self.logger.info(f"테이블 총 레코드 수: {count}")
            return count
        except Exception as e:
            self.logger.error(f"레코드 수 조회 중 오류 발생: {str(e)}")
            raise
    
    def read_data_from_minio(self, file_key):
        """
        MinIO에서 로또 데이터를 읽어옴
        """
        try:
            self.logger.info(f"MinIO에서 파일 읽기: {file_key}")
            
            # 파일 다운로드
            response = self.s3_client.get_object(Bucket=self.minio_bucket, Key=file_key)
            file_content = response['Body'].read()
            
            # JSON 데이터 파싱
            data = json.loads(file_content.decode('utf-8'))
            
            if not data:
                self.logger.warning("JSON 파일에 데이터가 없습니다.")
                return []
            
            self.logger.info(f"MinIO에서 {len(data)}개의 데이터를 성공적으로 읽어왔습니다.")
            return data
            
        except Exception as e:
            self.logger.error(f"MinIO에서 데이터 읽기 실패: {str(e)}")
            raise

    def execute(self, context):
        self.logger.info("로또 데이터 DB 저장 시작")
        
        # s3_client 초기화
        if self.s3_client is None:
            self.s3_client = self.s3_client_init()
        
        # 현재 날짜
        current_date = datetime.datetime.now().strftime('%Y-%m-%d')
        file_key = f"lotto_data_{current_date}.json"
        
        # PostgreSQL 연결 URL
        postgres_url = "postgresql://airflow:airflow@postgres:5432/airflow"
        
        # Engine과 Session 생성
        engine = None
        session = None
        
        try:
            # MinIO에서 데이터 읽기
            data = self.read_data_from_minio(file_key)
            
            if not data:
                self.logger.warning("삽입할 데이터가 없습니다.")
                return
            
            # 데이터베이스 연결 및 Session 생성
            engine = create_engine(postgres_url)
            Session = sessionmaker(bind=engine)
            session = Session()
            
            # 테이블 존재 여부 확인 및 생성
            if not self.check_table_exists(session):
                self.create_table_if_not_exists(session)
            
            self.logger.info(f"총 {len(data)}개의 데이터를 처리합니다.")
            
            # 데이터 삽입
            inserted_count, error_count = self.insert_data_safely(session, data)
            
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
        