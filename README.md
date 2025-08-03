# Midas - 데이터 파이프라인 인프라

이 프로젝트는 **금 가격 데이터**와 **로또 데이터**를 수집, 처리, 저장하는 완전한 데이터 파이프라인 인프라를 Docker Compose를 사용하여 효율적으로 배포하는 방법을 제공합니다.

## 🏗️ 아키텍처 구성

인프라는 다음과 같이 3개의 주요 레이어로 분리되어 있습니다:

### 1. **오케스트레이션 레이어** (`docker-compose.yaml`)
- **Airflow Webserver**: UI 및 DAG 관리 (포트: 8080)
- **Airflow Scheduler**: 작업 스케줄링
- **Airflow Worker**: 작업 실행 (Celery Executor)
- **Airflow Triggerer**: 이벤트 기반 작업 처리
- **PostgreSQL**: Airflow 메타데이터 저장소 (포트: 5432)
- **Redis**: Airflow 작업 큐 (포트: 6379)
- **MinIO**: 객체 스토리지 (S3 호환) (포트: 9000/9001)

### 2. **데이터 처리 레이어** (`docker-compose-processing.yaml`)
- **Spark Master**: 분산 처리 관리 (포트: 8082)
- **Spark Worker**: 작업 실행 (포트: 8081)

### 3. **모니터링 레이어** (`docker-compose-monitoring.yaml`)
- **Elasticsearch**: 로그 데이터 저장 (포트: 9200)
- **Kibana**: 로그 시각화 (포트: 5601)
- **Logstash**: 로그 수집
- **Metabase**: 데이터 분석 및 시각화 (포트: 3000)

## 🚀 구현된 데이터 파이프라인

### 📊 금 가격 데이터 파이프라인 (`gold_price_dag.py`)
- **데이터 소스**: 한국수출입은행 API
- **수집 주기**: 매일 오전 9시
- **처리 과정**:
  1. **Extract**: API에서 금 가격 데이터 수집
  2. **Transform**: 데이터 정제 및 변환
  3. **Load**: PostgreSQL 및 MinIO에 저장

### 🎰 로또 데이터 파이프라인 (`lotto_dag.py`)
- **데이터 소스**: 동행복권 웹사이트
- **수집 주기**: 매주 토요일 오후 9시 (추첨 후)
- **처리 과정**:
  1. **Extract**: 웹 스크래핑으로 당첨 번호 수집
  2. **Transform**: 데이터 정제 및 통계 계산
  3. **Load**: PostgreSQL 및 MinIO에 저장

## 🛠️ 커스텀 컴포넌트

### 📦 커스텀 오퍼레이터 (`dags/operators/`)
- **`gold_extract.py`**: 금 가격 데이터 수집 오퍼레이터
- **`gold_transform.py`**: 금 가격 데이터 변환 오퍼레이터
- **`gold_save.py`**: 금 가격 데이터 저장 오퍼레이터
- **`lotto_extract.py`**: 로또 데이터 수집 오퍼레이터
- **`lotto_save.py`**: 로또 데이터 저장 오퍼레이터
- **`driver_option.py`**: Selenium 드라이버 설정 오퍼레이터

### 🔍 커스텀 센서 (`dags/sensors/`)
- **`gold_price_sensor.py`**: 금 가격 데이터 수집 조건 확인 센서

## ⚙️ 초기 설정

### 필수 사전 준비

1. **Docker 네트워크 생성**
```bash
docker network create ndsnet
```

2. **디렉토리 및 권한 설정** (아래 중 하나 선택)
```bash
# 방법 1: 완전 초기화
sudo rm -rf dags logs plugins && mkdir -p dags logs plugins include config && sudo chown -R $(id -u):0 dags logs plugins include config

# 방법 2: 기존 파일 유지
rm -rf dags && mkdir -p dags logs plugins include config
sudo chown -R $(id -u):0 ./logs ./dags ./plugins ./include ./config requirements.txt
sudo chown -R $(id -u):0 .
```

3. **환경 변수 설정**
```bash
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0\nHOSTNAME=localhost\nAIRFLOW__WEBSERVER__SECRET_KEY=$(openssl rand -hex 30)" > .env
```

### Migration 오류 해결
```bash
docker volume rm airflow_postgres-db-volume
# 이후 재시작
```

## 🚀 인프라 시작/중지

### 자동화 스크립트 사용

1. **스크립트 실행 권한 부여**
```bash
chmod +x start_infra.sh stop_infra.sh start_airflow.sh
```

2. **전체 인프라 시작**
```bash
./start_infra.sh
```

3. **Airflow만 시작**
```bash
./start_airflow.sh
```

4. **인프라 중지**
```bash
./stop_infra.sh
```

### 수동 실행

#### 기본 Airflow 서비스 시작
```bash
docker-compose -p airflow up -d
```

#### Flower 모니터링 포함 시작
```bash
docker-compose -p airflow -f docker-compose-airflow.yaml up -d --profile flower
```

#### 스토리지 서비스만 시작
```bash
docker-compose -f docker-compose-storage.yaml up -d
```

## 🔗 서비스 접속 정보

| 서비스 | URL | 접속 정보 |
|--------|-----|-----------|
| **Airflow** | http://localhost:8080 | 사용자: `airflow` / 비밀번호: `airflow` |
| **MinIO** | http://localhost:9001 | 사용자: `minio` / 비밀번호: `minio123` |
| **Kibana** | http://localhost:5601 | - |
| **Elasticsearch** | http://localhost:9200 | - |
| **Metabase** | http://localhost:3000 | - |
| **Spark Master UI** | http://localhost:8082 | - |
| **Spark Worker UI** | http://localhost:8081 | - |

## 🗄️ 데이터베이스 접속

### PostgreSQL 접속
```bash
# 컨테이너 내부에서 접속
docker exec -it postgres psql -U airflow -d airflow

# 로컬에서 접속 (PostgreSQL 클라이언트 필요)
sudo apt-get install -y postgresql-client
psql -h localhost -p 5432 -U airflow -d airflow
```

### PostgreSQL 유용한 명령어
```sql
\l          -- 데이터베이스 목록 보기
\dt         -- 테이블 목록 보기
\d table_name  -- 테이블 구조 보기
\q          -- psql 종료
```

## 📁 프로젝트 구조

```
.
├── docker-compose.yaml              # 메인 Airflow 서비스 구성
├── docker-compose-monitoring.yaml   # 모니터링 서비스 구성
├── docker-compose-processing.yaml   # 데이터 처리 레이어 구성
├── start_infra.sh                   # 전체 인프라 시작 스크립트
├── start_airflow.sh                 # Airflow 전용 시작 스크립트
├── stop_infra.sh                    # 인프라 중지 스크립트
├── requirements.txt                 # Python 의존성 패키지
├── .env                            # 환경 변수 설정
├── dags/                           # Airflow DAG 파일
│   ├── gold_price_dag.py           # 금 가격 데이터 파이프라인
│   ├── lotto_dag.py                # 로또 데이터 파이프라인
│   ├── operators/                  # 커스텀 오퍼레이터
│   │   ├── gold_extract.py         # 금 가격 수집
│   │   ├── gold_transform.py       # 금 가격 변환
│   │   ├── gold_save.py            # 금 가격 저장
│   │   ├── lotto_extract.py        # 로또 수집
│   │   ├── lotto_save.py           # 로또 저장
│   │   └── driver_option.py        # Selenium 드라이버 설정
│   └── sensors/                    # 커스텀 센서
│       └── gold_price_sensor.py    # 금 가격 센서
├── include/                        # 공유 파일 및 데이터
│   └── data/
│       └── minio/                  # MinIO 데이터 저장소
├── logs/                           # Airflow 로그
├── plugins/                        # Airflow 플러그인
├── config/                         # Airflow 설정
└── spark/                          # Spark 구성
```

## 📦 주요 의존성 패키지

- **pandas**: 데이터 처리 및 분석
- **requests**: HTTP 요청 처리
- **beautifulsoup4**: 웹 스크래핑
- **selenium**: 동적 웹 페이지 처리
- **minio**: MinIO 클라이언트
- **psycopg2-binary**: PostgreSQL 연결
- **apache-airflow-providers-***: Airflow 확장 기능
- **boto3/botocore**: AWS S3 호환 스토리지

## ⚠️ 주의사항

1. **시스템 요구사항**
   - 로컬 환경에서 실행 시 충분한 리소스(CPU, 메모리) 필요
   - 최소 8GB RAM 권장
   - Docker 및 Docker Compose 설치 필요

2. **권한 관리**
   - 파일 권한 설정이 중요하며, 잘못된 권한으로 인한 오류 발생 가능
   - `.env` 파일의 `AIRFLOW_UID` 설정 확인 필수

3. **네트워크 설정**
   - `ndsnet` 네트워크가 사전에 생성되어야 함
   - 포트 충돌 시 방화벽 설정 확인

4. **데이터 백업**
   - PostgreSQL 볼륨 데이터 백업 권장
   - MinIO 데이터 백업 설정 필요

## 🔧 문제 해결

### 일반적인 문제들

1. **권한 오류**
   ```bash
   sudo chown -R $(id -u):0 .
   ```

2. **Migration 오류**
   ```bash
   docker volume rm airflow_postgres-db-volume
   docker-compose down
   docker-compose up -d
   ```

3. **네트워크 오류**
   ```bash
   docker network create ndsnet
   ```

4. **포트 충돌**
   ```bash
   # 사용 중인 포트 확인
   sudo netstat -tulpn | grep :8080
   ```

## 📈 모니터링 및 로그

- **Airflow 로그**: `logs/` 디렉토리에서 확인
- **Docker 로그**: `docker logs <container_name>` 명령으로 확인
- **서비스 상태**: `docker ps` 명령으로 전체 서비스 상태 확인

## 🤝 기여 방법

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📄 라이선스

이 프로젝트는 MIT 라이선스 하에 배포됩니다.
