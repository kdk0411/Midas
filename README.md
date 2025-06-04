# Midas

# 데이터 파이프라인 인프라 설정

이 프로젝트는 데이터 파이프라인 인프라를 Docker Compose를 사용하여 효율적으로 배포하는 방법을 제공합니다.

## 아키텍처 구성

인프라는 다음과 같이 4개의 주요 레이어로 분리되어 있습니다:
필수 
1. ndsnet 네트워크 생성이 필요함
- docker network create ndsnet
2. 파일 권한 -> 아래 2중 1택

sudo rm -rf dags logs plugins && mkdir -p dags logs plugins include config && sudo chown -R $(id -u):0 dags logs plugins include config

- echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0\nAIRFLOW__WEBSERVER__SECRET_KEY=$(openssl rand -hex 30)" > .env
- rm -rf dags && mkdir -p dags logs plugins include config
- sudo chown -R $(id -u):0 ./logs ./dags ./plugins ./include ./config requirements.txt
- sudo chown -R $(id -u):0 .

Airflow와 Storage는 의존성 문제로 아래 명령어로 실행해야 한다.
docker compose --env-file .env -p airflow -f docker-compose-storage.yaml -f docker-compose-airflow.yaml up -d
# --env-file .env : .env 파일을 envfile로 지정
# -p airflow : Project Name을 airflow로 지정
# -f : yaml 파일을 지정해서 사용
이미 실행 했다면 아래 명령어
docker compose -p airflow -f docker-compose-storage.yaml -f docker-compose-airflow.yaml down

각각 실행 - storage가 완벽하게 실행되면 airflow를 실행
docker compose -p airflow -f docker-compose-storage.yaml up -d
docker compose -p airflow -f docker-compose-airflow.yaml up -d

Migration Error
docker volume rm airflow_postgres-db-volume
이후 재시작

1. **스토리지 레이어** (`docker-compose-storage.yaml`)
   - PostgreSQL: Airflow 메타데이터 저장소
   - Redis: Airflow 작업 큐
   - MinIO: 객체 스토리지 (S3 호환)
   - docker compose -f docker-compose-storage.yaml up -d
   - docker exec -it postgres psql -U airflow -d airflow -> PostgreSQL 접근
   - psql -h localhost -p 5432 -U airflow -d airflow -> psql을 사용해서 접근 -> sudo apt-get install -y postgresql-client
     - \l : DB 목록 보기
     - \dt : Table 목록 보기
     - \d Table_name: 테이블 구조 보기
     - \q : psql 종료

2. **오케스트레이션 레이어** (`docker-compose-airflow.yaml`)
   - Airflow Webserver: UI 및 DAG 관리
   - Airflow Scheduler: 작업 스케줄링
   - Airflow Worker: 작업 실행
   - Airflow Triggerer: 이벤트 기반 작업 처리
   - echo "AIRFLOW_UID=$(id -u)" > .env -> 파일 권한 문제 방지 환경 변수 설정
   - docker compose -f docker-compose-airflow.yaml up -d
   - Flower 실행 방법 -> --profile flower 옵션 추가
      - docker compose -p airflow -f docker-compose-airflow.yaml up -d

3. **데이터 처리 레이어** (`docker-compose-processing.yaml`)
   - Spark Master: 분산 처리 관리
   - Spark Worker: 작업 실행

4. **모니터링 레이어** (`docker-compose-monitoring.yaml`)
   - Elasticsearch: 로그 데이터 저장
   - Kibana: 로그 시각화
   - Logstash: 로그 수집
   - Metabase: 데이터 분석 및 시각화

## 사용 방법

### 권한 설정

먼저 스크립트에 실행 권한을 부여합니다:

```bash
chmod +x start_infra.sh stop_infra.sh
```

### 인프라 시작

전체 인프라를 시작하려면 다음 명령을 실행하세요:

```bash
./start_infra.sh
```

이 스크립트는 다음 작업을 수행합니다:
1. Docker 네트워크 생성
2. 스토리지 서비스(PostgreSQL, Redis, MinIO) 시작
3. Airflow 서비스 시작
4. 선택적으로 데이터 처리 레이어(Spark) 시작
5. 선택적으로 모니터링 서비스(ELK, Metabase) 시작

### 인프라 중지

인프라를 중지하려면 다음 명령을 실행하세요:

```bash
./stop_infra.sh
```

이 스크립트는 모든 서비스를 중지하거나 선택적으로 특정 레이어를 중지하는 옵션을 제공합니다.

## 서비스 접속 정보

각 서비스에 다음 URL로 접속할 수 있습니다:

- **Airflow**: http://localhost:8080 (사용자/비밀번호: airflow/airflow)
- **MinIO**: http://localhost:9001 (사용자/비밀번호: minio/minio123)
- **Kibana**: http://localhost:5601
- **Elasticsearch**: http://localhost:9200
- **Metabase**: http://localhost:3000
- **Spark Master UI**: http://localhost:8082
- **Spark Worker UI**: http://localhost:8081

## 파일 구조

```
.
├── docker-compose-storage.yaml    # 스토리지 서비스 구성
├── docker-compose-airflow.yaml    # Airflow 관련 서비스 구성
├── docker-compose-processing.yaml # 데이터 처리 레이어 구성
├── docker-compose-monitoring.yaml # 모니터링 서비스 구성
├── start_infra.sh                 # 인프라 시작 스크립트
├── stop_infra.sh                  # 인프라 중지 스크립트
├── dags/                          # Airflow DAG 파일
│   ├── extract_dag.py
│   ├── transform_dag.py
│   └── load_dag.py
├── include/                       # 공유 파일
│   └── helpers/
├── spark/                         # Spark 구성
│   ├── master/
│   └── worker/
├── logstash/                      # Logstash 구성
│   └── pipeline/
└── kibana/                        # Kibana 구성
```

## 주의사항

- 로컬 환경에서 실행 시 충분한 리소스(CPU, 메모리)가 필요합니다.
- 스크립트는 로그를 남기고, 오류 처리를 포함합니다.
- 각 서비스는 필요에 따라 선택적으로 시작/중지할 수 있습니다.
