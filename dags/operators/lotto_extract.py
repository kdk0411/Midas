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
    def __init__(self, minio_config=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.minio_endpoint = minio_config["minio_endpoint"]
        self.minio_access_key = minio_config["minio_access_key"]
        self.minio_secret_key = minio_config["minio_secret_key"]
        self.minio_bucket = minio_config["minio_bucket"]
        self.logger = logging.getLogger(__name__)

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
        for i in range(1, 10):
            try:
                url = "https://www.dhlottery.co.kr/common.do?method=main"
                response = requests.get(url)
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # None 체크 추가
                draw_no_element = soup.find("strong", id="lottoDrwNo")
                if draw_no_element is None:
                    self.logger.info("최근 회차 번호를 찾을 수 없습니다. 웹사이트 구조가 변경되었을 수 있습니다.")
                    self.logger.info("10초 후 다시 시도")
                    time.sleep(10)
                    continue
                self.recent_draw_no = draw_no_element.text.strip()
                self.logger.info(f"최근 회차 번호: {self.recent_draw_no}")
                break
            except:
                self.logger.info("최근 회차 번호를 찾을 수 없습니다. 웹사이트 구조가 변경되었을 수 있습니다.")
                self.logger.info("10초 후 다시 시도")
                time.sleep(10)
                continue
    
    def _save_to_bucket(self, data, file_name):
        try:
            # 데이터를 JSON 형식으로 변환
            json_data = json.dumps(data, ensure_ascii=False, indent=2)
            self.s3_client.put_object(
                Bucket=self.minio_bucket,
                Key=file_name,
                Body=json_data.encode('utf-8'),
                ContentType='application/json'
            )
            self.logger.info(f"추출된 로또 데이터를 MinIO에 저장 완료: {file_name}")
        except Exception as e:
            self.logger.error(f"MinIO 업로드 중 오류 발생: {str(e)}")
            raise
    
    def execute(self, context):
        self.logger.info("로또 데이터 추출 시작")
        
        self._s3_bucket_init()
        self._get_recent_draw_no()
        
        # 현재 날짜를 가져와서 파일명에 사용
        current_date = datetime.datetime.now().strftime('%Y-%m-%d')
        
        # 로또 API 호출 (JSON API 사용)
        data = []
        try:
            url = f"https://www.dhlottery.co.kr/common.do?method=getLottoNumber&drwNo={self.recent_draw_no}"
            response = requests.get(url)
            
            # JSON 응답 파싱
            if response.status_code != 200:
                raise Exception(f"API 요청 실패: HTTP {response.status_code}")
            
            lotto_data = response.json()
            
            # returnValue 검증
            if lotto_data.get('returnValue') != 'success':
                raise Exception(f"API 응답 오류: {lotto_data.get('returnValue')}")
            
            # drwNo 검증
            api_draw_no = str(lotto_data.get('drwNo'))
            if api_draw_no != self.recent_draw_no:
                raise Exception(f"회차 번호 불일치: API={api_draw_no}, 최근회차={self.recent_draw_no}")
            
            # 로또 번호 추출
            numbers = [
                lotto_data["drwtNo1"],
                lotto_data["drwtNo2"],
                lotto_data["drwtNo3"],
                lotto_data["drwtNo4"],
                lotto_data["drwtNo5"],
                lotto_data["drwtNo6"]
            ]
            
            # None 값 체크
            if any(num is None for num in numbers):
                raise Exception("로또 번호 중 None 값이 있습니다.")
            
            bonus = lotto_data.get('bnusNo')
            if bonus is None:
                raise Exception("보너스 번호가 None입니다.")
            
            draw_date = lotto_data["drwNoDate"]
            
            row = {
                "draw_no": self.recent_draw_no,
                "draw_date": draw_date,
                "num_1": numbers[0],
                "num_2": numbers[1],
                "num_3": numbers[2],
                "num_4": numbers[3],
                "num_5": numbers[4],
                "num_6": numbers[5],
                "bonus": bonus,
                "extraction_timestamp": datetime.datetime.now().isoformat(),
                "total_sell_amount": lotto_data["totSellamnt"],
                "first_win_amount": lotto_data["firstWinamnt"],
                "first_winner_count": lotto_data["firstPrzwnerCo"],
                "first_accum_amount": lotto_data["firstAccumamnt"]
            }
            data.append(row)
            self.logger.info(f"로또 데이터 추출 성공: 회차 {self.recent_draw_no}, 번호: {numbers}, 보너스: {bonus}")
            
        except Exception as e:
            self.logger.error(f"최근 회차 데이터 추출 중 오류 발생: {str(e)}")
            # 오류 발생 시에도 빈 데이터로 파일 생성
            data = []
        
        # 날짜별로 JSON 파일 저장
        file_key = f"lotto_data_{current_date}.json"
        self._save_to_bucket(data, file_key)
        self.logger.info("로또 데이터 추출 완료")
