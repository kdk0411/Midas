#!/bin/bash

# 색상 정의
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}===== 데이터 파이프라인 인프라 중지 스크립트 =====${NC}"

# 현재 실행 중인 서비스 목록 확인
echo -e "${YELLOW}현재 실행 중인 서비스:${NC}"
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

echo -e "\n${YELLOW}모든 서비스를 중지하시겠습니까? (y/n)${NC}"
read -r stop_all

if [[ $stop_all == "y" || $stop_all == "Y" ]]; then
    # 역순으로 중지
    echo -e "${YELLOW}모니터링 서비스 중지 중...${NC}"
    docker-compose -f docker-compose/docker-compose-monitoring.yaml down 2>/dev/null || echo -e "${RED}모니터링 서비스가 실행 중이 아닙니다.${NC}"

    echo -e "${YELLOW}데이터 처리 레이어 중지 중...${NC}"
    docker-compose -f docker-compose/docker-compose-processing.yaml down 2>/dev/null || echo -e "${RED}데이터 처리 레이어가 실행 중이 아닙니다.${NC}"

    echo -e "${YELLOW}Airflow 서비스 중지 중...${NC}"
    docker-compose -f docker-compose/docker-compose-airflow.yaml down 2>/dev/null || echo -e "${RED}Airflow 서비스가 실행 중이 아닙니다.${NC}"

    echo -e "${YELLOW}스토리지 서비스 중지 중...${NC}"
    docker-compose -f docker-compose/docker-compose-storage.yaml down 2>/dev/null || echo -e "${RED}스토리지 서비스가 실행 중이 아닙니다.${NC}"

    echo -e "${GREEN}모든 서비스가 중지되었습니다.${NC}"
else
    # 선택적 중지
    echo -e "${YELLOW}모니터링 서비스(ELK, Metabase)를 중지하시겠습니까? (y/n)${NC}"
    read -r stop_monitoring
    if [[ $stop_monitoring == "y" || $stop_monitoring == "Y" ]]; then
        echo -e "${YELLOW}모니터링 서비스 중지 중...${NC}"
        docker-compose -f docker-compose/docker-compose-monitoring.yaml down
        echo -e "${GREEN}모니터링 서비스가 중지되었습니다.${NC}"
    fi

    echo -e "${YELLOW}데이터 처리 레이어(Spark)를 중지하시겠습니까? (y/n)${NC}"
    read -r stop_processing
    if [[ $stop_processing == "y" || $stop_processing == "Y" ]]; then
        echo -e "${YELLOW}데이터 처리 레이어 중지 중...${NC}"
        docker-compose -f docker-compose/docker-compose-processing.yaml down
        echo -e "${GREEN}데이터 처리 레이어가 중지되었습니다.${NC}"
    fi

    echo -e "${YELLOW}Airflow 서비스를 중지하시겠습니까? (y/n)${NC}"
    read -r stop_airflow
    if [[ $stop_airflow == "y" || $stop_airflow == "Y" ]]; then
        echo -e "${YELLOW}Airflow 서비스 중지 중...${NC}"
        docker-compose -f docker-compose/docker-compose-airflow.yaml down
        echo -e "${GREEN}Airflow 서비스가 중지되었습니다.${NC}"
    fi

    echo -e "${YELLOW}스토리지 서비스(Redis, PostgreSQL, MinIO)를 중지하시겠습니까? (y/n)${NC}"
    read -r stop_storage
    if [[ $stop_storage == "y" || $stop_storage == "Y" ]]; then
        echo -e "${YELLOW}스토리지 서비스 중지 중...${NC}"
        docker-compose -f docker-compose/docker-compose-storage.yaml down
        echo -e "${GREEN}스토리지 서비스가 중지되었습니다.${NC}"
    fi
fi

# 남아있는 컨테이너 확인
echo -e "${BLUE}===== 남아있는 서비스 상태 확인 =====${NC}"
docker ps

echo -e "${YELLOW}Docker 네트워크를 삭제하시겠습니까? (y/n)${NC}"
read -r delete_network
if [[ $delete_network == "y" || $delete_network == "Y" ]]; then
    docker network rm ndsnet 2>/dev/null && echo -e "${GREEN}네트워크가 삭제되었습니다.${NC}" || echo -e "${RED}네트워크 삭제에 실패했습니다. 아직 연결된 컨테이너가 있을 수 있습니다.${NC}"
fi

echo -e "${GREEN}인프라 중지 작업이 완료되었습니다.${NC}"