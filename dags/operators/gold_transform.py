from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import logging
from datetime import datetime
from minio import Minio
import json
import io
import sys
import os

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def transform_gold_price(raw_file_path: str, minio_endpoint: str, minio_access_key: str, 
                        minio_secret_key: str, minio_bucket: str) -> str:
    """
    MinIO에서 JSON 파일을 읽어와 PySpark로 변환하고 CSV로 저장
    
    Args:
        raw_file_path: MinIO에 저장된 원본 JSON 파일 경로
        minio_endpoint: MinIO 서버 엔드포인트
        minio_access_key: MinIO 접근 키
        minio_secret_key: MinIO 시크릿 키
        minio_bucket: MinIO 버킷 이름
    
    Returns:
        str: 변환된 CSV 파일의 MinIO 경로
    """
    logger.info("금값 데이터 변환 시작")
    
    # MinIO 클라이언트 초기화
    minio_client = Minio(
        minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=False
    )
    
    # SparkSession 생성
    spark = SparkSession.builder \
        .appName("GoldPriceTransform") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{minio_endpoint}") \
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
        
    try:
        # MinIO에서 JSON 파일 읽기
        response = minio_client.get_object(minio_bucket, raw_file_path)
        raw_data = json.loads(response.read().decode())
        
        # 스키마 정의
        schema = StructType([
            StructField("gold_price", StringType(), False),
            StructField("reg_date", TimestampType(), False),
            StructField("run_time", StringType(), False)
        ])
        
        # 데이터프레임 생성
        data = [(
            raw_data['gold_price'],
            datetime.fromisoformat(raw_data['reg_date']),
            raw_data['run_time']
        )]
        df = spark.createDataFrame(data, schema)
        
        # 데이터 변환 및 정제
        df = df.withColumn("gold_price", df.gold_price.cast("string"))
        
        # 변환된 데이터를 CSV로 저장
        current_time = datetime.now()
        csv_file_name = f"transformed/gold_price_{current_time.strftime('%Y%m%d_%H%M%S')}.csv"
        
        # DataFrame을 CSV 문자열로 변환
        csv_data = df.toPandas().to_csv(index=False)
        
        # CSV 파일을 MinIO에 업로드
        minio_client.put_object(
            bucket_name=minio_bucket,
            object_name=csv_file_name,
            data=io.BytesIO(csv_data.encode()),
            length=len(csv_data.encode()),
            content_type='text/csv'
        )
        
        logger.info(f"변환된 데이터를 MinIO에 저장 완료: {csv_file_name}")
        return csv_file_name
        
    except Exception as e:
        logger.error(f"데이터 변환 중 오류 발생: {str(e)}")
        raise
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    # 커맨드 라인 인자로 필요한 파라미터들을 받음
    if len(sys.argv) != 6:
        print("Usage: python gold_transform.py <raw_file_path> <minio_endpoint> <minio_access_key> <minio_secret_key> <minio_bucket>")
        sys.exit(1)
        
    raw_file_path = sys.argv[1]
    minio_endpoint = sys.argv[2]
    minio_access_key = sys.argv[3]
    minio_secret_key = sys.argv[4]
    minio_bucket = sys.argv[5]
    
    result = transform_gold_price(
        raw_file_path=raw_file_path,
        minio_endpoint=minio_endpoint,
        minio_access_key=minio_access_key,
        minio_secret_key=minio_secret_key,
        minio_bucket=minio_bucket
    )
    print(result)  # DockerOperator에서 결과를 캡처하기 위해 출력 