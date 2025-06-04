import time
import json
import datetime
import logging
import requests
import pandas as pd
from bs4 import BeautifulSoup

import boto3
from botocore.client import Config

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LottoExtractOperator(BaseOperator):
    @apply_defaults
    def __init__(self, minio_endpoint='localhost:9000', minio_access_key='minioadmin',
                 minio_secret_key='minioadmin', minio_bucket='lotto-data', mode="full", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.minio_endpoint = minio_endpoint
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.minio_bucket = minio_bucket
        self.logger = logging.getLogger(__name__)
        """
        mode:
            - Full: 전체 데이터 추출
            - Backfill: 최근 데이터 추출
        """
        self.mode = mode

    def _s3_bucket_init(self):
        # boto3 클라이언트 초기화
        self.s3_client = boto3.client(
            's3',
            endpoint_url=f'http://{self.minio_endpoint}',
            aws_access_key_id=self.minio_access_key,
            aws_secret_access_key=self.minio_secret_key,
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'
        )
        try:
            self.s3_client.head_bucket(Bucket=self.minio_bucket)
        except:
            self.s3_client.create_bucket(Bucket=self.minio_bucket)
    
    def _get_recent_draw_no(self):
        url = "https://www.dhlottery.co.kr/common.do?method=main"
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        self.recent_draw_no = soup.find("strong", id="lottoDrwNo").text.strip()
    
    def _save_to_bucket(self, data, file_name):
        try:
            # DataFrame을 CSV 형식의 문자열로 변환
            csv_data = data.to_csv(index=False)
            self.s3_client.put_object(
                Bucket=self.minio_bucket,
                Key=file_name,
                Body=csv_data,
                ContentType='text/csv'
            )
            self.logger.info(f"추출된 로또 데이터를 MinIO에 저장 완료: {file_name}")
        except Exception as e:
            self.logger.error(f"MinIO 업로드 중 오류 발생: {str(e)}")
            raise
    
    def execute(self, context):
        self.logger.info("로또 데이터 추출 시작")
        
        self._s3_bucket_init()
        self._get_recent_draw_no()
        # 로또 API 호출
        api_url = "https://www.dhlottery.co.kr/gameResult.do?method=byWin&drwNo="
        data = []
        
        if self.mode == "full":
            for idx in range(1, int(self.recent_draw_no) + 1):
                url = api_url + str(idx)
                response = requests.get(url)
                soup = BeautifulSoup(response.text, 'html.parser')
                
                try:
                    # 추첨일
                    draw_date = soup.select_one("p.desc").text.strip()
                    draw_date = draw_date.replace("년 ", "-") \
                            .replace("월 ", "-") \
                            .replace("일 추첨", "") \
                            .replace("(", "") \
                            .replace(")", "")
                    
                    target_div = soup.select("div.win span.ball_645")
                    # 당첨번호
                    numbers = [int(ball.text.strip()) for ball in target_div]
                    # 보너스
                    bonus = int(soup.select_one("div.num.bonus span.ball_645").text.strip())
                    
                    row = {
                        "draw_no": idx,
                        "draw_date": draw_date,
                        "num_1": numbers[0],
                        "num_2": numbers[1],
                        "num_3": numbers[2],
                        "num_4": numbers[3],
                        "num_5": numbers[4],
                        "num_6": numbers[5],
                        "bonus": bonus
                    }
                    data.append(row)
                    self.logger.info(f"{idx}회차 데이터 추출 완료")
                    time.sleep(0.2)
                except Exception as e:
                    self.logger.error(f"{idx}회차 데이터 추출 중 오류 발생: {str(e)}")
                    continue
        elif self.mode == "backfill":
            try:
                url = f"https://www.dhlottery.co.kr/common.do?method=getLottoNumber&drwNo={self.recent_draw_no}"
                response = requests.get(url)
                soup = BeautifulSoup(response.text, 'html.parser')
                target_div = soup.select("div.win span.ball_645")
                numbers = [int(ball.text.strip()) for ball in target_div]
                bonus = int(soup.select_one("div.num.bonus span.ball_645").text.strip())
                row = {
                    "draw_no": self.recent_draw_no,
                    "draw_date": draw_date,
                    "num_1": numbers[0],
                    "num_2": numbers[1],
                    "num_3": numbers[2],
                    "num_4": numbers[3],
                    "num_5": numbers[4],
                    "num_6": numbers[5],
                    "bonus": bonus
                }
                data.append(row)
            except Exception as e:
                self.logger.error(f"최근 회차 데이터 추출 중 오류 발생: {str(e)}")
                return
        else:
            self.logger.error("유효하지 않은 모드입니다.")
            return
        df = pd.DataFrame(data)
        file_name = f"lotto_data_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        self._save_to_bucket(df, file_name)
        self.logger.info("로또 데이터 추출 완료")
