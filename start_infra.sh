#!/bin/bash

# 색상 정의
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}===== 데이터 파이프라인 인프라 시작 스크립트 =====${NC}"

# docker-compose 디렉토리가 없으면 생성
if [ ! -d "docker-compose" ]; then
    echo -e "${YELLOW}docker-compose 디렉토리 생성 중...${NC}"
    mkdir -p docker-compose
fi

# 네트워크 생성 (이미 있는 경우 무시)
echo -e "${YELLOW}Docker 네트워크 생성 중...${NC}"
docker network create ndsnet 2>/dev/null || echo "네트워크가 이미 존재합니다."

# 1. 스토리지 서비스 시작
echo -e "${YELLOW}스토리지 서비스 시작 중...${NC}"
docker-compose -f docker-compose/docker-compose-storage.yaml up -d
if [ $? -eq 0 ]; then
    echo -e "${GREEN}스토리지 서비스가 성공적으로 시작되었습니다.${NC}"
else
    echo -e "${RED}스토리지 서비스 시작에 실패했습니다. 종료합니다.${NC}"
    exit 1
fi

echo -e "${YELLOW}스토리지 서비스가 준비될 때까지 10초 대기 중...${NC}"
sleep 10

# 2. Airflow 서비스 시작
echo -e "${YELLOW}Airflow 서비스 시작 중...${NC}"
docker-compose -f docker-compose/docker-compose-airflow.yaml up -d
if [ $? -eq 0 ]; then
    echo -e "${GREEN}Airflow 서비스가 성공적으로 시작되었습니다.${NC}"
else
    echo -e "${RED}Airflow 서비스 시작에 실패했습니다.${NC}"
fi

# 3. 사용자 입력에 따라 처리 레이어와 모니터링 서비스 시작
echo -e "${YELLOW}데이터 처리 레이어(Spark)를 시작하시겠습니까? (y/n)${NC}"
read -r start_processing

if [[ $start_processing == "y" || $start_processing == "Y" ]]; then
    echo -e "${YELLOW}데이터 처리 레이어 시작 중...${NC}"
    docker-compose -f docker-compose/docker-compose-processing.yaml up -d
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}데이터 처리 레이어가 성공적으로 시작되었습니다.${NC}"
    else
        echo -e "${RED}데이터 처리 레이어 시작에 실패했습니다.${NC}"
    fi
fi

echo -e "${YELLOW}모니터링 서비스(ELK, Metabase)를 시작하시겠습니까? (y/n)${NC}"
read -r start_monitoring

if [[ $start_monitoring == "y" || $start_monitoring == "Y" ]]; then
    echo -e "${YELLOW}모니터링 서비스 시작 중...${NC}"
    docker-compose -f docker-compose/docker-compose-monitoring.yaml up -d
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}모니터링 서비스가 성공적으로 시작되었습니다.${NC}"
    else
        echo -e "${RED}모니터링 서비스 시작에 실패했습니다.${NC}"
    fi
fi

echo -e "${BLUE}===== 서비스 상태 확인 =====${NC}"
docker ps

echo -e "${GREEN}인프라 시작 완료!${NC}"
echo -e "${YELLOW}접속 정보:${NC}"
echo -e "Airflow: http://localhost:8080 (사용자/비밀번호: airflow/airflow)"
echo -e "MinIO: http://localhost:9001 (사용자/비밀번호: minio/minio123)"

if [[ $start_monitoring == "y" || $start_monitoring == "Y" ]]; then
    echo -e "Kibana: http://localhost:5601"
    echo -e "Elasticsearch: http://localhost:9200"
    echo -e "Metabase: http://localhost:3000"
fi

if [[ $start_processing == "y" || $start_processing == "Y" ]]; then
    echo -e "Spark Master UI: http://localhost:8082"
    echo -e "Spark Worker UI: http://localhost:8081"
fi