#!/bin/bash

# 색상 정의
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Airflow 서비스 시작 스크립트${NC}"

# 실행 중인 컨테이너 확인
if docker ps | grep -q "airflow\|postgres\|redis\|minio"; then
    echo -e "${YELLOW}실행 중인 Airflow 관련 컨테이너를 중지합니다...${NC}"
    docker compose -p airflow -f docker-compose-storage.yaml -f docker-compose-airflow.yaml down
    echo -e "${GREEN}모든 컨테이너가 중지되었습니다.${NC}"
fi

# Storage 서비스 시작
echo -e "${YELLOW}Storage 서비스를 시작합니다...${NC}"
docker compose -p airflow -f docker-compose-storage.yaml up -d
if [ $? -eq 0 ]; then
    echo -e "${GREEN}Storage 서비스가 시작되었습니다.${NC}"
else
    echo -e "${RED}Storage 서비스 시작 실패${NC}"
    exit 1
fi

# Airflow init 실행 (storage 서비스와 함께)
echo -e "${YELLOW}Airflow 초기화를 시작합니다...${NC}"
docker compose -p airflow -f docker-compose-storage.yaml -f docker-compose-airflow.yaml run airflow-init
if [ $? -eq 0 ]; then
    echo -e "${GREEN}Airflow 초기화가 완료되었습니다.${NC}"
else
    echo -e "${RED}Airflow 초기화 실패${NC}"
    exit 1
fi

# Airflow 서비스 시작
echo -e "${YELLOW}Airflow 서비스를 시작합니다...${NC}"
docker compose -p airflow -f docker-compose-airflow.yaml up -d
if [ $? -eq 0 ]; then
    echo -e "${GREEN}Airflow 서비스가 시작되었습니다.${NC}"
else
    echo -e "${RED}Airflow 서비스 시작 실패${NC}"
    exit 1
fi

# 서비스 상태 확인
echo -e "${YELLOW}서비스 상태를 확인합니다...${NC}"
docker ps | grep -E "airflow|postgres|redis|minio"

echo -e "${GREEN}모든 서비스가 정상적으로 시작되었습니다.${NC}"
echo -e "${YELLOW}Airflow UI: http://localhost:8080${NC}"
echo -e "${YELLOW}Flower UI: http://localhost:5555${NC}" 