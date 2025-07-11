import logging
import requests
from datetime import datetime

from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

class GoldPriceSensor(BaseSensorOperator):
    """
    금값 데이터 가용성을 확인하는 Sensor
    - API 호출 가능성 확인
    - 시장 상태 및 데이터 품질 검증
    - 조건 만족 시 True 반환 (데이터 전달 없음)
    """
    
    @apply_defaults
    def __init__(
        self,
        mode='backfill',
        check_market_status=True,
        *args,
        **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(__name__)
        self.mode = mode
        self.check_market_status = check_market_status
    
    def poke(self, context):
        """
        금값 데이터 가용성을 확인하는 메인 로직
        """
        self.logger.info(f"금값 데이터 가용성 확인 시작 - 모드: {self.mode}")
        
        try:
            if self.mode == 'backfill':
                return self._check_backfill_availability()
            elif self.mode == 'fullrefresh':
                return self._check_fullrefresh_availability()
            else:
                self.logger.error(f"잘못된 모드: {self.mode}")
                return False
                
        except Exception as e:
            self.logger.error(f"데이터 가용성 확인 중 오류 발생: {str(e)}")
            return False
    
    def _check_backfill_availability(self):
        """
        backfill 모드에서 데이터 가용성 확인
        """
        api_url = "https://m.stock.naver.com/front-api/chart/pricesByPeriod?reutersCode=M04020000&category=metals&chartInfoType=gold&scriptChartType=day"
        
        # HTTP 요청
        try:
            response = requests.get(api_url, timeout=30)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"API 요청 실패: {str(e)}")
            return False
        
        json_data = response.json()
        
        # API 응답 성공 여부 확인
        if not json_data.get('isSuccess', False):
            self.logger.error(f"API 호출 실패: {json_data.get('message', 'Unknown error')}")
            return False
        
        result_data = json_data['result']
        
        # 데이터 품질 검증
        if not self._validate_data(result_data):
            return False
        
        # 시장 상태 확인 (설정에 따라)
        if self.check_market_status:
            market_status = result_data.get('marketStatus', 'UNKNOWN')
            if market_status != 'OPEN':
                self.logger.info(f"시장 상태가 OPEN이 아님: {market_status}")
                return False
        
        current_date = result_data.get('tradeBaseAt', 'Unknown')
        price_count = len(result_data.get('priceInfos', []))
        market_status = result_data.get('marketStatus', 'UNKNOWN')
        
        self.logger.info(f"데이터 가용성 확인 완료 - 거래일: {current_date}, 시장상태: {market_status}, 가격 정보: {price_count}개")
        return True
    
    def _check_fullrefresh_availability(self):
        """
        fullrefresh 모드에서 데이터 가용성 확인
        """
        api_url = "https://m.stock.naver.com/front-api/chart/pricesByPeriod?reutersCode=M04020000&category=metals&chartInfoType=gold&scriptChartType=areaYearFive"
        
        # HTTP 요청
        try:
            response = requests.get(api_url, timeout=30)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"API 요청 실패: {str(e)}")
            return False
        
        json_data = response.json()
        
        # API 응답 성공 여부 확인
        if not json_data.get('isSuccess', False):
            self.logger.error(f"API 호출 실패: {json_data.get('message', 'Unknown error')}")
            return False
        
        result_data = json_data['result']
        
        # 데이터 품질 검증
        if not self._validate_data(result_data):
            return False
        
        price_count = len(result_data.get('priceInfos', []))
        self.logger.info(f"전체 갱신 데이터 가용성 확인 완료 - 가격 정보: {price_count}개")
        return True
    
    def _validate_data(self, result_data):
        """
        데이터 품질 검증
        """
        if not result_data:
            self.logger.error("결과 데이터가 비어있습니다.")
            return False
        
        if self.mode == 'backfill':
            # backfill 모드에서는 tradeBaseAt 필수
            if not result_data.get('tradeBaseAt'):
                self.logger.error("tradeBaseAt 필드가 비어있습니다.")
                return False
        
        # 가격 정보 존재 여부 확인
        if not result_data.get('priceInfos'):
            self.logger.warning("가격 정보가 없습니다.")
            return False
        
        return True 